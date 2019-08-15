/**
 *  @file
 *  @copyright defined in eos/LICENSE.txt
 */
#include <eosio/cassandra_history_plugin/cassandra_history_plugin.hpp>
#include <eosio/cassandra_history_plugin/account_action_trace_shard_object.hpp>
#include <eosio/chain/config.hpp>
#include <eosio/chain/exceptions.hpp>
#include <eosio/chain/transaction.hpp>
#include <eosio/chain/types.hpp>

#include <fc/io/json.hpp>
#include <fc/utf8.hpp>
#include <fc/variant.hpp>

#include <boost/algorithm/string.hpp>
#include <boost/signals2/connection.hpp>

#include <algorithm>
#include <ctime>
#include <deque>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <set>
#include <thread>
#include <tuple>
#include <type_traits>
#include <vector>

#include "cassandra_client.h"
#include "ThreadPool/ThreadPool.h"


namespace eosio {
   static appbase::abstract_plugin& _cassandra_history_plugin = app().register_plugin<cassandra_history_plugin>();

struct filter_entry {
   name receiver;
   name action;
   name actor;

   friend bool operator<( const filter_entry& a, const filter_entry& b ) {
      return std::tie( a.receiver, a.action, a.actor ) < std::tie( b.receiver, b.action, b.actor );
   }

   //            receiver          action       actor
   bool match( const name& rr, const name& an, const name& ar ) const {
      return (receiver.value == 0 || receiver == rr) &&
             (action.value == 0 || action == an) &&
             (actor.value == 0 || actor == ar);
   }
};


class cassandra_history_plugin_impl {
   public:
   cassandra_history_plugin_impl();
   ~cassandra_history_plugin_impl();

   fc::optional<boost::signals2::scoped_connection> accepted_block_connection;
   fc::optional<boost::signals2::scoped_connection> irreversible_block_connection;
   fc::optional<boost::signals2::scoped_connection> accepted_transaction_connection;
   fc::optional<boost::signals2::scoped_connection> applied_transaction_connection;

   void check_task_queue_size();
   void consume_blocks();

   void on_accepted_block(const chain::block_state_ptr&);
   void on_applied_irreversible_block(const chain::block_state_ptr&);
   void on_accepted_transaction(const chain::transaction_metadata_ptr&);
   void on_applied_transaction(const chain::transaction_trace_ptr&);

   void process_accepted_block( chain::block_state_ptr );
   void process_irreversible_block( chain::block_state_ptr );
   void process_accepted_transaction(chain::transaction_metadata_ptr);
   void process_applied_transaction(chain::transaction_trace_ptr);

   void upsertAccount(
      const chain::action& act,
      const chain::block_timestamp_type& block_time);

   bool filter_include( const account_name& receiver, const action_name& act_name,
                        const vector<chain::permission_level>& authorization ) const;
   bool filter_include( const transaction& trx ) const;

   void init();

   bool log_head_block = false;
   std::chrono::time_point<std::chrono::system_clock> last_head_block_log_time;
   int head_log_interval = 4;
   uint32_t start_block_num = 0;
   std::atomic_bool start_block_reached{false};

   bool filter_on_star = true;
   std::set<filter_entry> filter_on;
   std::set<filter_entry> filter_out;

   template<typename Queue, typename Entry> void queue(Queue& queue, const Entry& e);
   std::mutex queue_mtx;
   std::condition_variable condition;
   std::thread consume_thread;

   size_t max_queue_size = 0;
   int queue_sleep_time = 0;
   std::deque<chain::transaction_metadata_ptr> transaction_metadata_queue;
   std::deque<chain::transaction_trace_ptr> transaction_trace_queue;
   std::deque<chain::block_state_ptr> block_state_queue;
   std::deque<chain::block_state_ptr> irreversible_block_state_queue;

   std::queue<std::function<void()>> upsert_account_task_queue;
   std::mutex upsert_account_task_mtx;

   chain_plugin* chain_plug = nullptr;
   std::atomic_bool done{false};
   std::atomic_bool startup{true};
   fc::optional<chain::chain_id_type> chain_id;
   fc::microseconds abi_serializer_max_time_ms;

   std::unique_ptr<chainbase::database> db;
   std::unique_ptr<CassandraClient> cas_client;

   size_t max_task_queue_size = 0;
   int task_queue_sleep_time = 0;
   std::unique_ptr<tp::ThreadPool> thread_pool;

   static const uint32_t account_actions_per_shard;
};


const uint32_t cassandra_history_plugin_impl::account_actions_per_shard = 10000;


bool cassandra_history_plugin_impl::filter_include( const account_name& receiver, const action_name& act_name,
                                           const vector<chain::permission_level>& authorization ) const
{
   bool include = false;
   if( filter_on_star ) {
      include = true;
   } else {
      auto itr = std::find_if( filter_on.cbegin(), filter_on.cend(), [&receiver, &act_name]( const auto& filter ) {
         return filter.match( receiver, act_name, 0 );
      } );
      if( itr != filter_on.cend() ) {
         include = true;
      } else {
         for( const auto& a : authorization ) {
            auto itr = std::find_if( filter_on.cbegin(), filter_on.cend(), [&receiver, &act_name, &a]( const auto& filter ) {
               return filter.match( receiver, act_name, a.actor );
            } );
            if( itr != filter_on.cend() ) {
               include = true;
               break;
            }
         }
      }
   }

   if( !include ) { return false; }
   if( filter_out.empty() ) { return true; }

   auto itr = std::find_if( filter_out.cbegin(), filter_out.cend(), [&receiver, &act_name]( const auto& filter ) {
      return filter.match( receiver, act_name, 0 );
   } );
   if( itr != filter_out.cend() ) { return false; }

   for( const auto& a : authorization ) {
      auto itr = std::find_if( filter_out.cbegin(), filter_out.cend(), [&receiver, &act_name, &a]( const auto& filter ) {
         return filter.match( receiver, act_name, a.actor );
      } );
      if( itr != filter_out.cend() ) { return false; }
   }

   return true;
}

bool cassandra_history_plugin_impl::filter_include( const transaction& trx ) const
{
   if( !filter_on_star || !filter_out.empty() ) {
      bool include = false;
      for( const auto& a : trx.actions ) {
         if( filter_include( a.account, a.name, a.authorization ) ) {
            include = true;
            break;
         }
      }
      if( !include ) {
         for( const auto& a : trx.context_free_actions ) {
            if( filter_include( a.account, a.name, a.authorization ) ) {
               include = true;
               break;
            }
         }
      }
      return include;
   }
   return true;
}


cassandra_history_plugin_impl::cassandra_history_plugin_impl()
{
}

cassandra_history_plugin_impl::~cassandra_history_plugin_impl()
{
   if (!startup) {
      try {
         ilog( "cassandra_history_plugin shutdown in process please be patient this can take a few minutes" );
         done = true;
         condition.notify_one();

         consume_thread.join();
      } catch( std::exception& e ) {
         elog( "Exception on cassandra_history_plugin shutdown of consume thread: ${e}", ("e", e.what()));
      }
   }
}

void cassandra_history_plugin_impl::init() {
   ilog("starting consume thread");
   consume_thread = std::thread([this] { consume_blocks(); });

   startup = false;
}


void cassandra_history_plugin_impl::check_task_queue_size() {
   auto task_queue_size = thread_pool->queue_size();
   if ( task_queue_size > max_task_queue_size ) {
      while (thread_pool->queue_size() > max_task_queue_size) {
         task_queue_sleep_time += 20;
         if (task_queue_sleep_time > 1000)
            wlog("thread pool task queue size: ${q}", ("q", task_queue_size));
         std::this_thread::sleep_for(std::chrono::milliseconds(task_queue_sleep_time));
      }
   } else {
      task_queue_sleep_time -= 20;
      if( task_queue_sleep_time < 0 ) task_queue_sleep_time = 0;
   }
}

void cassandra_history_plugin_impl::consume_blocks() {
   std::deque<chain::transaction_metadata_ptr> transaction_metadata_process_queue;
   std::deque<chain::transaction_trace_ptr> transaction_trace_process_queue;
   std::deque<chain::block_state_ptr> block_state_process_queue;
   std::deque<chain::block_state_ptr> irreversible_block_state_process_queue;

   try {
      while (true) {
         std::unique_lock<std::mutex> lock(queue_mtx);
         while ( transaction_metadata_queue.empty() &&
                 transaction_trace_queue.empty() &&
                 block_state_queue.empty() &&
                 irreversible_block_state_queue.empty() &&
                 !done ) {
            condition.wait(lock);
         }

         // capture for processing
         size_t transaction_metadata_size = transaction_metadata_queue.size();
         if (transaction_metadata_size > 0) {
            transaction_metadata_process_queue = move(transaction_metadata_queue);
            transaction_metadata_queue.clear();
         }
         size_t transaction_trace_size = transaction_trace_queue.size();
         if (transaction_trace_size > 0) {
            transaction_trace_process_queue = move(transaction_trace_queue);
            transaction_trace_queue.clear();
         }
         size_t block_state_size = block_state_queue.size();
         if (block_state_size > 0) {
            block_state_process_queue = move(block_state_queue);
            block_state_queue.clear();
         }
         size_t irreversible_block_size = irreversible_block_state_queue.size();
         if (irreversible_block_size > 0) {
            irreversible_block_state_process_queue = move(irreversible_block_state_queue);
            irreversible_block_state_queue.clear();
         }

         lock.unlock();

         if (done) {
            ilog("draining queue, size: ${q}", ("q", transaction_metadata_size + transaction_trace_size + block_state_size + irreversible_block_size));
         }

         // process transactions
         while (!transaction_trace_process_queue.empty()) {
            const auto& t = transaction_trace_process_queue.front();
            process_applied_transaction(t);
            transaction_trace_process_queue.pop_front();
         }

         while (!transaction_metadata_process_queue.empty()) {
            const auto& t = transaction_metadata_process_queue.front();
            process_accepted_transaction(t);
            transaction_metadata_process_queue.pop_front();
         }

         // process blocks
         while (!block_state_process_queue.empty()) {
            const auto& bs = block_state_process_queue.front();
            process_accepted_block( bs );
            block_state_process_queue.pop_front();
         }

         // process irreversible blocks
         while (!irreversible_block_state_process_queue.empty()) {
            const auto& bs = irreversible_block_state_process_queue.front();
            process_irreversible_block(bs);
            irreversible_block_state_process_queue.pop_front();
         }

         if( transaction_metadata_size == 0 &&
             transaction_trace_size == 0 &&
             block_state_size == 0 &&
             irreversible_block_size == 0 &&
             done ) {
            break;
         }
      }
      ilog("cassandra_history_plugin consume thread shutdown gracefully");
   } catch (fc::exception& e) {
      elog("FC Exception while consuming block ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while consuming block ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while consuming block");
   }
}


template<typename Queue, typename Entry>
void cassandra_history_plugin_impl::queue( Queue& queue, const Entry& e ) {
   std::unique_lock<std::mutex> lock( queue_mtx );
   auto queue_size = queue.size();
   if( queue_size > max_queue_size ) {
      while(queue.size() > max_queue_size) {
         lock.unlock();
         condition.notify_one();
         queue_sleep_time += 20;
         if (queue_sleep_time > 1000)
            wlog("queue size: ${q}, sleep time: ${t}", ("q", queue.size())("t", queue_sleep_time));
         std::this_thread::sleep_for(std::chrono::milliseconds(queue_sleep_time));
         lock.lock();
      }
   } else {
      queue_sleep_time -= 20;
      if( queue_sleep_time < 0 ) queue_sleep_time = 0;
   }
   queue.emplace_back( e );
   lock.unlock();
   condition.notify_one();
}


void cassandra_history_plugin_impl::on_accepted_block( const chain::block_state_ptr& bs ) {
   try {
      if( !start_block_reached ) {
         if( bs->block_num >= start_block_num ) {
            start_block_reached = true;
         }
      }
      if( start_block_reached ) {
         queue( block_state_queue, bs );
      }
   } catch (fc::exception& e) {
      elog("FC Exception while accepted_block ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while accepted_block ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while accepted_block");
   }
}

void cassandra_history_plugin_impl::on_applied_irreversible_block( const chain::block_state_ptr& bs ) {
   try {
      if( start_block_reached ) {
         queue( irreversible_block_state_queue, bs );
      }
   } catch (fc::exception& e) {
      elog("FC Exception while applied_irreversible_block ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while applied_irreversible_block ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while applied_irreversible_block");
   }
}

void cassandra_history_plugin_impl::on_accepted_transaction( const chain::transaction_metadata_ptr& t ) {
   try {
      if( start_block_reached ) {
         queue( transaction_metadata_queue, t );
      }
   } catch (fc::exception& e) {
      elog("FC Exception while accepted_transaction ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while accepted_transaction ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while accepted_transaction");
   }
}

void cassandra_history_plugin_impl::on_applied_transaction( const chain::transaction_trace_ptr& t ) {
   try {
      if( !t->producer_block_id.valid() ||
         !t->receipt || (t->receipt->status != chain::transaction_receipt_header::executed &&
            t->receipt->status != chain::transaction_receipt_header::soft_fail) )
         return;

      if( start_block_reached ) {
         queue( transaction_trace_queue, t );
      }
   } catch (fc::exception& e) {
      elog("FC Exception while applied_transaction ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while applied_transaction ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while applied_transaction");
   }
}


void cassandra_history_plugin_impl::process_accepted_block(chain::block_state_ptr bs) {
   check_task_queue_size();
   thread_pool->enqueue(
      [ bs{std::move(bs)}, this ]()
      {
         auto block_num = bs->block_num;
         if( block_num % 10000 == 0 )
         {
            ilog( "block_num: ${b}", ("b", block_num) );
         }

         const auto block_id = bs->id;
         const auto block_id_str = block_id.str();
         fc::variant bs_doc(bs);
         auto json_block = fc::prune_invalid_utf8(fc::json::to_string(bs_doc));
         try {
            cas_client->insertBlock(block_id_str, num_to_bytes(block_num), std::move(json_block), false);
            auto now = std::chrono::system_clock::now();
            if (std::chrono::duration_cast<std::chrono::seconds>(now - last_head_block_log_time).count() > head_log_interval) {
               ilog( "block_num: ${b}", ("b", block_num) );
               last_head_block_log_time = now;
            }
         } catch (const std::exception& e) {
            elog("STD Exception from insertBlock ${e}", ("e", e.what()));
            appbase::app().quit();
         } catch (...) {
            elog("Unknown exception from insertBlock");
            appbase::app().quit();
         }
      });
}

void cassandra_history_plugin_impl::process_irreversible_block(chain::block_state_ptr bs) {
   check_task_queue_size();
   thread_pool->enqueue(
      [ bs{std::move(bs)}, this ]()
      {
         auto block_num = bs->block_num;
         const auto block_id = bs->id;
         const auto block_id_str = block_id.str();
         fc::variant bs_doc(bs);
         auto json_block = fc::prune_invalid_utf8(fc::json::to_string(bs_doc));
         try {
            cas_client->insertBlock(block_id_str, num_to_bytes(block_num), std::move(json_block), true);
         } catch (const std::exception& e) {
            elog("STD Exception from insertBlock ${e}", ("e", e.what()));
            appbase::app().quit();
         } catch (...) {
            elog("Unknown exception from insertBlock");
            appbase::app().quit();
         }
      });
}

void cassandra_history_plugin_impl::process_accepted_transaction(chain::transaction_metadata_ptr t) {
   check_task_queue_size();
   thread_pool->enqueue(
      [ t{std::move(t)}, this ]()
      {
         const chain::signed_transaction& trx = t->packed_trx->get_signed_transaction();
         if( !filter_include( trx ) ) return;

         const auto& trx_id = t->id;
         const auto trx_id_str = trx_id.str();

         fc::variant trx_doc(trx);
         auto json_trx = fc::prune_invalid_utf8(fc::json::to_string(trx_doc));
         try {
            cas_client->insertTransaction(trx_id_str, std::move(json_trx));
         } catch (const std::exception& e) {
            elog("STD Exception from insertTransaction ${e}", ("e", e.what()));
            appbase::app().quit();
         } catch (...) {
            elog("Unknown exception from insertTransaction");
            appbase::app().quit();
         }
      });
}

void cassandra_history_plugin_impl::process_applied_transaction(chain::transaction_trace_ptr t) {

   bool executed = t->receipt->status == chain::transaction_receipt_header::executed;
   std::vector<std::reference_wrapper<eosio::chain::action_trace>> action_traces;
   std::vector<std::pair<eosio::chain::action, eosio::chain::block_timestamp_type>> upsertAccountActions;

   for( auto& atrace : t->action_traces ) {
      if(executed && atrace.receiver == chain::config::system_account_name) {
         upsertAccountActions.emplace_back(atrace.act, t->block_time);
      }

      if (filter_include(atrace.receiver, atrace.act.name, atrace.act.authorization)) {
         action_traces.emplace_back( atrace );
      }
   }

   if (!upsertAccountActions.empty()) {
      auto f = [upsertAccountActions{ std::move(upsertAccountActions) }, this]()
      {
         for (auto& p : upsertAccountActions)
         {
            upsertAccount(p.first, p.second);
         }
      };
      {
         std::unique_lock<std::mutex> guard(upsert_account_task_mtx);
         upsert_account_task_queue.emplace( std::move(f) );
      }
      check_task_queue_size();
      thread_pool->enqueue(
         [ this ]()
         {
            std::unique_lock<std::mutex> guard(upsert_account_task_mtx);
            std::function<void()> task = std::move( upsert_account_task_queue.front() );
            task();
            upsert_account_task_queue.pop();
         }
      );
   }
   
   if( action_traces.empty() ) return;

   auto block_time = t->block_time;
   auto block_time_ms = (int64_t)block_time.to_time_point().time_since_epoch().count() / 1000;

   auto& chain = chain_plug->chain();
   const auto& idx = db->get_index<account_action_trace_shard_multi_index, by_account>();

   std::vector<std::tuple<std::vector<cass_byte_t>, fc::time_point>> batchDateActionTrace;
   std::map<eosio::chain::account_name,
      std::vector<std::tuple<eosio::chain::account_name, int64_t, std::vector<cass_byte_t>, fc::time_point>>> batchAccountTrace;
   std::vector<std::function<void()>> actionTraceShardInserts;
   std::vector<std::function<void()>> traceInserts;

   for (auto& trace_ref : action_traces)
   {
      auto& atrace = trace_ref.get();
      if (!atrace.receipt.valid()) {
         continue;
      }
      auto global_seq_buffer = num_to_bytes(atrace.receipt->global_sequence);

      batchDateActionTrace.emplace_back(std::make_tuple(global_seq_buffer, block_time));
      traceInserts.emplace_back([=, &chain, &atrace]()
      {
         try {
            fc::variant doc = chain.to_variant_with_abi(atrace, abi_serializer_max_time_ms);
            auto json_atrace = fc::prune_invalid_utf8(fc::json::to_string(doc));
            cas_client->insertActionTrace(atrace.receipt->global_sequence, std::move(json_atrace),
               std::string(atrace.act.name), std::string(atrace.receiver), std::string(atrace.act.account));
         } catch (const std::exception& e) {
            elog("STD Exception from insertActionTrace ${e}", ("e", e.what()));
            appbase::app().quit();
         } catch (...) {
            elog("Unknown exception from insertActionTrace");
            appbase::app().quit();
         }
      });
      std::set<account_name> aset = { atrace.receiver };
      std::transform(std::begin(atrace.act.authorization), std::end(atrace.act.authorization),
         std::inserter(aset, std::begin(aset)), [](auto& auth) { return auth.actor; });
      for (auto a : aset)
      {
         bool need_insert_shard = false;
         int64_t lastShardId = std::numeric_limits<int64_t>::max(); //needed only to clear shards. Set it only after need_insert_shard is set to true
         int64_t shardId = 0;
         auto itr = idx.find(a);
         if (itr == idx.end())
         {
            need_insert_shard = true;
            lastShardId = 0;
            shardId = block_time_ms;
            db->create<account_action_trace_shard_object>([&]( auto& obj ) {
               obj.account = a;
               obj.timestamp = block_time_ms;
               obj.counter = 1;
            });
         }
         else if (itr->counter == account_actions_per_shard)
         {
            need_insert_shard = true;
            lastShardId = itr->timestamp;
            db->modify<account_action_trace_shard_object>(*itr, [&](auto& obj) {
               obj.timestamp = block_time_ms;
               obj.counter = 1;
            });
            shardId = itr->timestamp;
         }
         else
         {
            db->modify<account_action_trace_shard_object>(*itr, [&](auto& obj) {
               obj.counter = obj.counter + 1;
            });
            shardId = itr->timestamp;
         }
         if (need_insert_shard) {
            try {
               cas_client->clearFollowingShards(a, lastShardId);
            } catch (const std::exception& e) {
               elog("STD Exception from clearFollowingShards ${e}", ("e", e.what()));
               appbase::app().quit();
            } catch (...) {
               elog("Unknown exception from clearFollowingShards");
               appbase::app().quit();
            }
            actionTraceShardInserts.emplace_back([=]()
            {
               try {
                  cas_client->insertAccountActionTraceShard(a, shardId);
               } catch (const std::exception& e) {
                  elog("STD Exception from insertAccountActionTraceShard ${e}", ("e", e.what()));
                  appbase::app().quit();
               } catch (...) {
                  elog("Unknown exception from insertAccountActionTraceShard");
                  appbase::app().quit();
               }
            });
         }
         auto val = std::make_tuple(a, shardId, global_seq_buffer, block_time.to_time_point());
         auto res = batchAccountTrace.insert(std::make_pair(a, std::vector<decltype(val)>{}));
         res.first->second.emplace_back(val);
      }
   }

   check_task_queue_size();
   thread_pool->enqueue(
      [ this, t{std::move(t)}, block_time,
         batchDateActionTrace{std::move(batchDateActionTrace)},
         batchAccountTrace{std::move(batchAccountTrace)},
         traceInserts{std::move(traceInserts)},
         actionTraceShardInserts{std::move(actionTraceShardInserts)} ]()
      {
         const auto trx_id = t->id;
         const auto trx_id_str = trx_id.str();
         auto block_num = t->block_num;
         fc::variant trx_trace_doc(t);
         auto json_trx_trace = fc::prune_invalid_utf8(fc::json::to_string(trx_trace_doc));
         try {
            cas_client->insertTransactionTrace(trx_id_str, num_to_bytes(block_num), block_time, std::move(json_trx_trace));
         } catch (const std::exception& e) {
            elog("STD Exception from insertTransactionTrace ${e}", ("e", e.what()));
            appbase::app().quit();
         } catch (...) {
            elog("Unknown exception from insertTransactionTrace");
            appbase::app().quit();
         }

         double total_time = 0.0;
         double max_time = 0.0;
         for (auto& f : traceInserts)
         {
            auto start = std::chrono::high_resolution_clock::now();
            f();
            auto finish = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double> elapsed = finish - start;
            total_time += elapsed.count();
            if (elapsed.count() > max_time) {
               max_time = elapsed.count();
            }
            if (elapsed.count() > 1.0) {
               wlog("action trace insert took more than second");
            }
         }
         ilog("time info (${n} processed traces): total - ${total}s, avg - ${avg}s, max - ${max}s",
            ("n", traceInserts.size())("total", total_time)("avg", total_time / traceInserts.size())("max", max_time));
         try {
            cas_client->batchInsertDateActionTrace(batchDateActionTrace);
         } catch (const std::exception& e) {
            elog("STD Exception while executing batch with action traces ${e}", ("e", e.what()));
            appbase::app().quit();
         } catch (...) {
            elog("Unknown exception while executing batch with action traces");
            appbase::app().quit();
         }
         for (const auto& p : batchAccountTrace)
         {
            try {
               cas_client->batchInsertAccountActionTrace(p.second);
            } catch (const std::exception& e) {
               elog("STD Exception while executing batch with action traces ${e}", ("e", e.what()));
               appbase::app().quit();
            } catch (...) {
               elog("Unknown exception while executing batch with action traces");
               appbase::app().quit();
            }
         }
         for (auto& f : actionTraceShardInserts)
         {
            f();
         }
      });
}

void cassandra_history_plugin_impl::upsertAccount(
      const chain::action& act,
      const chain::block_timestamp_type& block_time)
{
   if (act.account != chain::config::system_account_name)
      return;
   
   try {
      if( act.name == chain::newaccount::get_name() ) {
         auto newacc = act.data_as<chain::newaccount>();
         cas_client->insertAccount(newacc, block_time.to_time_point());
      }
      else if( act.name == chain::updateauth::get_name() ) {
         const auto update = act.data_as<chain::updateauth>();
         cas_client->updateAccountAuth(update);
      }
      else if( act.name == chain::deleteauth::get_name() ) {
         const auto del = act.data_as<chain::deleteauth>();
         cas_client->deleteAccountAuth(del);
      }
      else if( act.name == chain::setabi::get_name() ) {
         auto setabi = act.data_as<chain::setabi>();
         cas_client->updateAccountAbi(setabi);
      }
   } catch( fc::exception& e ) {
      // if unable to unpack native type, skip account creation
   } catch (const std::exception& e) {
      elog("STD Exception from upsertAccount ${e}", ("e", e.what()));
      appbase::app().quit();
   } catch (...) {
      elog("Unknown exception from upsertAccount");
      appbase::app().quit();
   }
}


cassandra_history_plugin::cassandra_history_plugin():my(new cassandra_history_plugin_impl()){}
cassandra_history_plugin::~cassandra_history_plugin(){}

void cassandra_history_plugin::set_program_options(options_description&, options_description& cfg) {
   cfg.add_options()
         ("cassandra-url", bpo::value<std::string>(),
          "cassandra URL connection string If not specified then plugin is disabled.")
         ("cassandra-keyspace", bpo::value<std::string>(),
          "cassandra keyspace where data is located.")
         ("cassandra-replication-factor", bpo::value<size_t>()->default_value(1),
          "replication factor that will be used to create cassandra keyspace.")
         ("cassandra-retry-delay-ms", bpo::value<size_t>()->default_value(200),
          "Time in milliseconds that plugin will wait before retrying request to cassandra.")
         ("cassandra-max-retries", bpo::value<size_t>()->default_value(2),
          "Maximum number of retries for timed out requests.")
         ("cassandra-wipe", bpo::bool_switch()->default_value(false),
          "Only used with --replay-blockchain, --hard-replay-blockchain, or --delete-all-blocks to wipe cassandra db.")
         ("cassandra-block-start", bpo::value<uint32_t>()->default_value(0),
          "If specified then nothing is pushed to cassandra until specified block is reached.")
         ("cassandra-filter-on", bpo::value<vector<string>>()->composing(),
          "Track actions which match receiver:action:actor. Receiver, Action, & Actor may be blank to include all. i.e. eosio:: or :transfer:  Use * or leave unspecified to include all.")
         ("cassandra-filter-out", bpo::value<vector<string>>()->composing(),
          "Do not track actions which match receiver:action:actor. Receiver, Action, & Actor may be blank to exclude all.")
         ("cassandra-queue-size,q", bpo::value<uint32_t>()->default_value(1024),
          "The target queue size between nodeos and thread pool.")
         ("cassandra-thread-pool-size", bpo::value<size_t>()->default_value(4),
          "The size of the data processing thread pool.")
         ("cassandra-shard-db-size-mb", bpo::value<size_t>()->default_value(512),
          "Maximum size(megabytes) of the shard database.")
         ("cassandra-log-block-num", bpo::bool_switch()->default_value(false),
          "Log head block.")
         ;
}

void cassandra_history_plugin::plugin_initialize(const variables_map& options) {
   try {
      if( options.count( "cassandra-url" ) && options.count( "cassandra-keyspace" ) ) {
         ilog( "initializing cassandra_history_plugin" );

         if( options.count( "cassandra-log-block-num" )) {
            my->log_head_block = options.at( "cassandra-log-block-num" ).as<bool>();
            my->last_head_block_log_time = std::chrono::system_clock::now();
         }
         if( options.count( "cassandra-block-start" )) {
            my->start_block_num = options.at( "cassandra-block-start" ).as<uint32_t>();
         }
         if( my->start_block_num == 0 ) {
            my->start_block_reached = true;
         }

         if( options.count( "abi-serializer-max-time-ms" )) {
            uint32_t max_time = options.at( "abi-serializer-max-time-ms" ).as<uint32_t>();
            EOS_ASSERT(max_time > chain::config::default_abi_serializer_max_time_ms,
                       chain::plugin_config_exception, "--abi-serializer-max-time-ms required as default value not appropriate for parsing full blocks");
            fc::microseconds abi_serializer_max_time = app().get_plugin<chain_plugin>().get_abi_serializer_max_time();
            my->abi_serializer_max_time_ms = abi_serializer_max_time;
         }

         if( options.count( "cassandra-filter-on" )) {
            auto fo = options.at( "cassandra-filter-on" ).as<vector<string>>();
            my->filter_on_star = false;
            for( auto& s : fo ) {
               if( s == "*" ) {
                  my->filter_on_star = true;
                  break;
               }
               std::vector<std::string> v;
               boost::split( v, s, boost::is_any_of( ":" ));
               EOS_ASSERT( v.size() == 3, fc::invalid_arg_exception, "Invalid value ${s} for --cassandra-filter-on", ("s", s));
               filter_entry fe{v[0], v[1], v[2]};
               my->filter_on.insert( fe );
            }
         } else {
            my->filter_on_star = true;
         }
         if( options.count( "cassandra-filter-out" )) {
            auto fo = options.at( "cassandra-filter-out" ).as<vector<string>>();
            for( auto& s : fo ) {
               std::vector<std::string> v;
               boost::split( v, s, boost::is_any_of( ":" ));
               EOS_ASSERT( v.size() == 3, fc::invalid_arg_exception, "Invalid value ${s} for --cassandra-filter-out", ("s", s));
               filter_entry fe{v[0], v[1], v[2]};
               my->filter_out.insert( fe );
            }
         }

         bool dropKeyspace = false;
         if(options.at( "replay-blockchain" ).as<bool>() ||
            options.at( "hard-replay-blockchain" ).as<bool>() ||
            options.at( "delete-all-blocks" ).as<bool>())
         {
            ilog( "Wiping cassandra on startup" );
            dropKeyspace = true;
            fc::remove_all( app().data_dir() / "cass_shard" );
            fc::remove_all( app().data_dir() / "cass_failed" );
         }
         
         std::string url_str = options.at( "cassandra-url" ).as<std::string>();
         std::string keyspace_str = options.at( "cassandra-keyspace" ).as<std::string>();
         size_t replication_factor = options.at( "cassandra-replication-factor" ).as<size_t>();
         size_t retry_delay_ms = options.at( "cassandra-retry-delay-ms" ).as<size_t>();
         size_t max_retries = options.at( "cassandra-max-retries" ).as<size_t>();
         my->cas_client.reset( new CassandraClient(url_str, keyspace_str, replication_factor, dropKeyspace) );
         my->cas_client->setRetryDelay(retry_delay_ms);
         my->cas_client->setMaxRetries(max_retries);

         auto db_size = options.at("cassandra-shard-db-size-mb").as<size_t>();
         my->db.reset(new chainbase::database(app().data_dir() / "cass_shard", chain::database::read_write, db_size*1024*1024ll));
         my->db->add_index<account_action_trace_shard_multi_index>();

         if( options.count( "cassandra-queue-size" )) {
            my->max_queue_size = options.at( "cassandra-queue-size" ).as<uint32_t>();
         }
         my->max_task_queue_size = my->max_queue_size * 2;

         size_t thr_pool_size = options.at( "cassandra-thread-pool-size" ).as<size_t>();
         ilog("init thread pool, size: ${tps}", ("tps", thr_pool_size));
         my->thread_pool.reset( new tp::ThreadPool(thr_pool_size) );

         my->chain_plug = app().find_plugin<chain_plugin>();
         EOS_ASSERT(my->chain_plug, chain::missing_chain_plugin_exception, "");
         auto& chain = my->chain_plug->chain();
         my->chain_id.emplace( chain.get_chain_id());
         
         my->accepted_block_connection.emplace(
            chain.accepted_block.connect( [&]( const chain::block_state_ptr& bs ) {
               my->on_accepted_block( bs );
         } ));
         my->irreversible_block_connection.emplace(
            chain.irreversible_block.connect( [&]( const chain::block_state_ptr& bs ) {
               my->on_applied_irreversible_block( bs );
            } ));
         my->accepted_transaction_connection.emplace(
            chain.accepted_transaction.connect( [&]( const chain::transaction_metadata_ptr& t ) {
               my->on_accepted_transaction( t );
            } ));
         my->applied_transaction_connection.emplace(
            chain.applied_transaction.connect( [&]( std::tuple<const chain::transaction_trace_ptr&, const chain::signed_transaction&> t ) {
               auto [ttrace, strans] = t;
               my->on_applied_transaction( ttrace );
            } ));

         my->init();
      } else {
         wlog( "eosio::cassandra_history_plugin configured, but no --cassandra-url or --cassandra-keyspace specified." );
         wlog( "cassandra_history_plugin disabled." );
      }
   }
   FC_LOG_AND_RETHROW()
}

void cassandra_history_plugin::plugin_startup() {
   // Make the magic happen
}

void cassandra_history_plugin::plugin_shutdown() {
   my->accepted_block_connection.reset();
   my->irreversible_block_connection.reset();
   my->accepted_transaction_connection.reset();
   my->applied_transaction_connection.reset();

   my.reset();
}

}
