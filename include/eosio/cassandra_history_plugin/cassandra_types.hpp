#pragma once
#include <algorithm>
#include <functional>
#include <vector>

#include <cassandra.h>
#include <chainbase/chainbase.hpp>
#include <eosio/chain_plugin/chain_plugin.hpp> 
#include <fc/array.hpp>
#include <fc/optional.hpp>
#include <fc/variant.hpp>


namespace eosio
{
    using namespace boost::multi_index;

    enum cass_query_object_type
    {
        upsert_account,
        insert_account_action_trace,
        insert_account_action_trace_shard,
        insert_date_action_trace,
        insert_action_trace,
        insert_block,
        insert_transaction,
        insert_transaction_trace,
        OBJECT_TYPE_COUNT
    };


    class upsert_account_object : public chainbase::object<cass_query_object_type::upsert_account, upsert_account_object> {
        OBJECT_CTOR(upsert_account_object,(data))

        id_type id;
        chain::action_name name;
        fc::optional<fc::time_point> blockTime;
        chain::shared_blob data;
    };
    class insert_account_action_trace_object : public chainbase::object<cass_query_object_type::insert_account_action_trace, insert_account_action_trace_object> {
        OBJECT_CTOR(insert_account_action_trace_object,(globalSeq))

        id_type id;
        chain::account_name account;
        int64_t shardId;
        chain::shared_blob globalSeq;
        fc::time_point blockTime;

        void setGlobalSeq(const std::vector<cass_byte_t>& gs)
        {
            globalSeq.resize( gs.size() );
            for (int i = 0; i < gs.size(); i++)
            {
                globalSeq[i] = gs[i];
            }
        }
    };
    class insert_account_action_trace_shard_object : public chainbase::object<cass_query_object_type::insert_account_action_trace_shard, insert_account_action_trace_shard_object> {
        OBJECT_CTOR(insert_account_action_trace_shard_object)

        id_type id;
        chain::account_name account;
        int64_t shardId;
    };
    class insert_date_action_trace_object : public chainbase::object<cass_query_object_type::insert_date_action_trace, insert_date_action_trace_object> {
        OBJECT_CTOR(insert_date_action_trace_object,(globalSeq))

        id_type id;
        chain::shared_blob globalSeq;
        fc::time_point blockTime;

        void setGlobalSeq(const std::vector<cass_byte_t>& gs)
        {
            globalSeq.resize( gs.size() );
            for (int i = 0; i < gs.size(); i++)
            {
                globalSeq[i] = gs[i];
            }
        }
    };
    class insert_action_trace_object : public chainbase::object<cass_query_object_type::insert_action_trace, insert_action_trace_object> {
        OBJECT_CTOR(insert_action_trace_object,(actionTrace)(actionType)(receiver)(account))

        id_type id;
        uint64_t globalSeq;
        chain::shared_blob actionTrace;
        chain::shared_blob actionType;
        chain::shared_blob receiver;
        chain::shared_blob account;
    };
    class insert_block_object : public chainbase::object<cass_query_object_type::insert_block, insert_block_object> {
        OBJECT_CTOR(insert_block_object,(blockId)(blockNum)(block))

        id_type id;
        chain::shared_blob blockId;
        chain::shared_blob blockNum;
        chain::shared_blob block;
        bool irreversible;

        void setBlockNum(const std::vector<cass_byte_t>& num)
        {
            blockNum.resize( num.size() );
            for (int i = 0; i < num.size(); i++)
            {
                blockNum[i] = num[i];
            }
        }
    };
    class insert_transaction_object : public chainbase::object<cass_query_object_type::insert_transaction, insert_transaction_object> {
        OBJECT_CTOR(insert_transaction_object,(transactionId)(transaction))

        id_type id;
        chain::shared_blob transactionId;
        chain::shared_blob transaction;
    };
    class insert_transaction_trace_object : public chainbase::object<cass_query_object_type::insert_transaction_trace, insert_transaction_trace_object> {
        OBJECT_CTOR(insert_transaction_trace_object,(transactionId)(blockNum)(transactionTrace))

        id_type id;
        chain::shared_blob transactionId;
        chain::shared_blob blockNum;
        fc::time_point blockTime;
        chain::shared_blob transactionTrace;

        void setBlockNum(const std::vector<cass_byte_t>& num)
        {
            blockNum.resize( num.size() );
            for (int i = 0; i < num.size(); i++)
            {
                blockNum[i] = num[i];
            }
        }
    };


    struct by_id;
    struct by_account_shard_id;

    using upsert_account_multi_index = chainbase::shared_multi_index_container<
        upsert_account_object,
        indexed_by<
            ordered_unique<tag<by_id>, BOOST_MULTI_INDEX_MEMBER(upsert_account_object, upsert_account_object::id_type, id)>
        >
    >;
    typedef chainbase::generic_index<upsert_account_multi_index> upsert_account_index;

    using insert_account_action_trace_multi_index = chainbase::shared_multi_index_container<
        insert_account_action_trace_object,
        indexed_by<
            ordered_unique<tag<by_id>, BOOST_MULTI_INDEX_MEMBER(insert_account_action_trace_object, insert_account_action_trace_object::id_type, id)>,
            ordered_non_unique<
                tag<by_account_shard_id>,
                composite_key<
                    insert_account_action_trace_object,
                    member<insert_account_action_trace_object,chain::account_name,&insert_account_action_trace_object::account>,
                    member<insert_account_action_trace_object,int64_t,&insert_account_action_trace_object::shardId>
                >//,
                //composite_key_compare< /*std::less<std::string>, */std::less<int64_t> > //TODO: check order
            >
        >
    >;
    typedef chainbase::generic_index<insert_account_action_trace_multi_index> insert_account_action_trace_index;

    using insert_account_action_trace_shard_multi_index = chainbase::shared_multi_index_container<
        insert_account_action_trace_shard_object,
        indexed_by<
            ordered_unique<tag<by_id>, BOOST_MULTI_INDEX_MEMBER(insert_account_action_trace_shard_object, insert_account_action_trace_shard_object::id_type, id)>,
            ordered_non_unique<
                tag<by_account_shard_id>,
                composite_key<
                    insert_account_action_trace_shard_object,
                    member<insert_account_action_trace_shard_object,chain::account_name,&insert_account_action_trace_shard_object::account>,
                    member<insert_account_action_trace_shard_object,int64_t,&insert_account_action_trace_shard_object::shardId>
                >//,
                //composite_key_compare< /*std::less<std::string>, */std::less<int64_t> > //TODO: check order
            >
        >
    >;
    typedef chainbase::generic_index<insert_account_action_trace_shard_multi_index> insert_account_action_trace_shard_index;

    using insert_date_action_trace_multi_index = chainbase::shared_multi_index_container<
        insert_date_action_trace_object,
        indexed_by<
            ordered_unique<tag<by_id>, BOOST_MULTI_INDEX_MEMBER(insert_date_action_trace_object, insert_date_action_trace_object::id_type, id)>
        >
    >;
    typedef chainbase::generic_index<insert_date_action_trace_multi_index> insert_date_action_trace_index;
    
    using insert_action_trace_multi_index = chainbase::shared_multi_index_container<
        insert_action_trace_object,
        indexed_by<
            ordered_unique<tag<by_id>, BOOST_MULTI_INDEX_MEMBER(insert_action_trace_object, insert_action_trace_object::id_type, id)>
        >
    >;
    typedef chainbase::generic_index<insert_action_trace_multi_index> insert_action_trace_index;

    using insert_block_multi_index = chainbase::shared_multi_index_container<
        insert_block_object,
        indexed_by<
            ordered_unique<tag<by_id>, BOOST_MULTI_INDEX_MEMBER(insert_block_object, insert_block_object::id_type, id)>
        >
    >;
    typedef chainbase::generic_index<insert_block_multi_index> insert_block_index;

    using insert_transaction_multi_index = chainbase::shared_multi_index_container<
        insert_transaction_object,
        indexed_by<
            ordered_unique<tag<by_id>, BOOST_MULTI_INDEX_MEMBER(insert_transaction_object, insert_transaction_object::id_type, id)>
        >
    >;
    typedef chainbase::generic_index<insert_transaction_multi_index> insert_transaction_index;

    using insert_transaction_trace_multi_index = chainbase::shared_multi_index_container<
        insert_transaction_trace_object,
        indexed_by<
            ordered_unique<tag<by_id>, BOOST_MULTI_INDEX_MEMBER(insert_transaction_trace_object, insert_transaction_trace_object::id_type, id)>
        >
    >;
    typedef chainbase::generic_index<insert_transaction_trace_multi_index> insert_transaction_trace_index;
}


CHAINBASE_SET_INDEX_TYPE( eosio::upsert_account_object,                    eosio::upsert_account_multi_index )
CHAINBASE_SET_INDEX_TYPE( eosio::insert_account_action_trace_object,       eosio::insert_account_action_trace_multi_index )
CHAINBASE_SET_INDEX_TYPE( eosio::insert_account_action_trace_shard_object, eosio::insert_account_action_trace_shard_multi_index )
CHAINBASE_SET_INDEX_TYPE( eosio::insert_date_action_trace_object,          eosio::insert_date_action_trace_multi_index )
CHAINBASE_SET_INDEX_TYPE( eosio::insert_action_trace_object,               eosio::insert_action_trace_multi_index )
CHAINBASE_SET_INDEX_TYPE( eosio::insert_block_object,                      eosio::insert_block_multi_index )
CHAINBASE_SET_INDEX_TYPE( eosio::insert_transaction_object,                eosio::insert_transaction_multi_index )
CHAINBASE_SET_INDEX_TYPE( eosio::insert_transaction_trace_object,          eosio::insert_transaction_trace_multi_index )

FC_REFLECT( eosio::upsert_account_object, (name)(blockTime)(data) )
FC_REFLECT( eosio::insert_account_action_trace_object, (account)(shardId)(globalSeq)(blockTime) )
FC_REFLECT( eosio::insert_account_action_trace_shard_object, (account)(shardId) )
FC_REFLECT( eosio::insert_date_action_trace_object, (globalSeq)(blockTime) )
FC_REFLECT( eosio::insert_action_trace_object, (actionTrace)(actionType)(receiver)(account) )
FC_REFLECT( eosio::insert_block_object, (blockId)(blockNum)(block)(irreversible) )
FC_REFLECT( eosio::insert_transaction_object, (transactionId)(transaction) )
FC_REFLECT( eosio::insert_transaction_trace_object, (transactionId)(blockNum)(blockTime)(transactionTrace) )