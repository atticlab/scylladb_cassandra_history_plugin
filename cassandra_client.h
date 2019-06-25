#pragma once
#include <atomic>
#include <functional>
#include <iostream>
#include <mutex>
#include <tuple>
#include <vector>

#include <cassandra.h>
#include <eosio/cassandra_history_plugin/cassandra_types.hpp>
#include <eosio/chain/action.hpp>
#include <eosio/chain/block_timestamp.hpp>
#include <eosio/chain/contract_types.hpp>
#include <chainbase/chainbase.hpp>
#include <fc/time.hpp>

#include "cassandra_guard.h"


class CassandraClient
{
public:
    CassandraClient(const std::string& hostUrl, const std::string& keyspace, size_t replicationFactor, bool dropKeyspace);
    ~CassandraClient();

    void init();
    void insertFailed();
    void prepareStatements();

    void setRetryDelay(size_t retry_delay_ms) { retry_delay_ms_ = retry_delay_ms; }
    void setMaxRetries(size_t max_retries) { max_retries_ = max_retries; }

    void batchInsertDateActionTrace(
        const std::vector<std::tuple<std::vector<cass_byte_t>, fc::time_point, std::vector<cass_byte_t>>>& data);
    void batchInsertAccountActionTrace(
        const std::vector<std::tuple<eosio::chain::account_name, int64_t, std::vector<cass_byte_t>, fc::time_point, std::vector<cass_byte_t>>>& data);

    void insertAccount(
        const eosio::chain::newaccount& newacc,
        fc::time_point blockTime);
    void deleteAccountAuth(
        const eosio::chain::deleteauth& del);
    void updateAccountAuth(
        const eosio::chain::updateauth& update);
    void updateAccountAbi(
        const eosio::chain::setabi& setabi);
    
    void insertAccountActionTrace(
        const eosio::chain::account_name& account,
        int64_t shardId,
        std::vector<cass_byte_t> globalSeq,
        fc::time_point blockTime);
    void insertAccountActionTraceWithParent(
        const eosio::chain::account_name& account,
        int64_t shardId,
        std::vector<cass_byte_t> globalSeq,
        fc::time_point blockTime,
        std::vector<cass_byte_t> parent);
    void insertAccountActionTraceShard(
        const eosio::chain::account_name& account,
        int64_t shardId);
    void insertDateActionTrace(
        std::vector<cass_byte_t> globalSeq,
        fc::time_point blockTime,
        std::vector<cass_byte_t> parent);
    void insertActionTrace(
        std::vector<cass_byte_t> globalSeq,
        std::string&& actionTrace,
        const std::string& actionType,
        const std::string& receiver,
        const std::string& account);
    void insertActionTraceWithParent(
        std::vector<cass_byte_t> globalSeq,
        std::vector<cass_byte_t> parent,
        const std::string& actionType,
        const std::string& receiver,
        const std::string& account);
    void insertBlock(
        const std::string& id,
        std::vector<cass_byte_t> blockNumBuffer,
        std::string&& block,
        bool irreversible);
    void insertTransaction(
        const std::string& id,
        std::string&& transaction);
    void insertTransactionTrace(
        const std::string& id,
        std::vector<cass_byte_t> blockNumBuffer,
        fc::time_point blockTime,
        std::string&& transactionTrace);

    void clearFollowingShards(
        const eosio::chain::account_name& account,
        int64_t prevShardId);

    void resetKeyspace();


    static const std::string account_table;
    static const std::string account_public_key_table;
    static const std::string account_controlling_account_table;
    static const std::string account_action_trace_table;
    static const std::string account_action_trace_shard_table;
    static const std::string date_action_trace_table;
    static const std::string action_trace_table;
    static const std::string block_table;
    static const std::string lib_table;
    static const std::string transaction_table;
    static const std::string transaction_trace_table;

private:
    CassandraClient(const CassandraClient& other) = delete;
    CassandraClient& operator=(const CassandraClient& other) = delete;

    bool checkTimeout(future_guard& future) const;
    future_guard execute(const std::string& query);
    future_guard execute(batch_guard& b);
    future_guard execute(batch_guard&& b);
    future_guard execute(statement_guard& gStatement);
    future_guard execute(statement_guard&& gStatement);
    //executes batch, if returned error is timeout then sleeps and retries again
    //if did not succeed -> stop node
    void executeWait(batch_guard&& b, const std::function<void()>& onError);
    //executes statement, if returned error is timeout then sleeps and retries again
    //if did not succeed -> stop node
    void executeWait(statement_guard&& gStatement, const std::function<void()>& onError);
    //wait for future to return something
    //if error returned -> stop node
    void waitFuture(future_guard&& gFuture);
    //wait for future to return something
    //if error returned -> execute callback and stop node
    void waitFuture(future_guard&& gFuture, const std::function<void()>& onError);

    

    chainbase::database failed;
    std::mutex db_mtx;

    std::string keyspace_;
    size_t replicationFactor_;
    size_t retry_delay_ms_ = 200;
    size_t max_retries_ = 2;

    cluster_guard gCluster_;
    session_guard gSession_;
    prepared_guard gPreparedDeleteAccountPublicKeys_;
    prepared_guard gPreparedDeleteAccountControls_;
    prepared_guard gPreparedInsertAccount_;
    prepared_guard gPreparedInsertAccountAbi_;
    prepared_guard gPreparedInsertAccountPublicKeys_;
    prepared_guard gPreparedInsertAccountControls_;
    prepared_guard gPreparedInsertAccountActionTrace_;
    prepared_guard gPreparedInsertAccountActionTraceWithParent_;
    prepared_guard gPreparedInsertAccountActionTraceShard_;
    prepared_guard gPreparedInsertDateActionTrace_;
    prepared_guard gPreparedInsertDateActionTraceWithParent_;
    prepared_guard gPreparedInsertBlock_;
    prepared_guard gPreparedInsertIrreversibleBlock_;
    prepared_guard gPreparedInsertTransaction_;
    prepared_guard gPreparedInsertTransactionTrace_;
    prepared_guard gPreparedUpdateIrreversible_;

    std::string insertActionTraceQuery;
    std::string insertActionTraceWithParentQuery;
};