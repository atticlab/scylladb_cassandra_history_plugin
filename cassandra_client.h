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
        std::string&& actionTrace);
    void insertActionTraceWithParent(
        std::vector<cass_byte_t> globalSeq,
        std::vector<cass_byte_t> parent);
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

    void resetKeyspace();

    void execute(const std::string& query);


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

    future_guard executeStatement(statement_guard&& gStatement);
    future_guard executeBatch(batch_guard&& b);
    void waitFuture(future_guard&& gFuture);
    void waitFuture(future_guard&& gFuture, const std::function<void()>& onError);

    chainbase::database failed;
    std::mutex db_mtx;

    std::string keyspace_;
    size_t replicationFactor_;
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
    prepared_guard gPreparedInsertActionTrace_;
    prepared_guard gPreparedInsertActionTraceWithParent_;
    prepared_guard gPreparedInsertBlock_;
    prepared_guard gPreparedInsertIrreversibleBlock_;
    prepared_guard gPreparedInsertTransaction_;
    prepared_guard gPreparedInsertTransactionTrace_;
    prepared_guard gPreparedUpdateIrreversible_;
};