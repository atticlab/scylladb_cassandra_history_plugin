#include "cassandra_client.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <ctime>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>

#include <appbase/application.hpp>
#include <fc/io/json.hpp>
#include <fc/variant.hpp>
#include <fc/variant_object.hpp>


const std::string CassandraClient::account_table                     = "account";
const std::string CassandraClient::account_public_key_table          = "account_public_key";
const std::string CassandraClient::account_controlling_account_table = "account_controlling_account";
const std::string CassandraClient::account_action_trace_table        = "account_action_trace";
const std::string CassandraClient::account_action_trace_shard_table  = "account_action_trace_shard";
const std::string CassandraClient::date_action_trace_table           = "date_action_trace";
const std::string CassandraClient::action_trace_table                = "action_trace";
const std::string CassandraClient::block_table                       = "block";
const std::string CassandraClient::lib_table                         = "lib";
const std::string CassandraClient::transaction_table                 = "transaction";
const std::string CassandraClient::transaction_trace_table           = "transaction_trace";


CassandraClient::CassandraClient(const std::string& hostUrl, const std::string& keyspace, size_t replicationFactor, bool dropKeyspace)
    : failed(appbase::app().data_dir() / "cass_failed", eosio::chain::database::read_write, 4096*1024*1024ll),
    keyspace_(keyspace),
    replicationFactor_(replicationFactor),
    gCluster_(nullptr, cass_cluster_free),
    gSession_(nullptr, cass_session_free),
    gPreparedDeleteAccountPublicKeys_(nullptr, cass_prepared_free),
    gPreparedDeleteAccountControls_(nullptr, cass_prepared_free),
    gPreparedInsertAccount_(nullptr, cass_prepared_free),
    gPreparedInsertAccountAbi_(nullptr, cass_prepared_free),
    gPreparedInsertAccountPublicKeys_(nullptr, cass_prepared_free),
    gPreparedInsertAccountControls_(nullptr, cass_prepared_free),
    gPreparedInsertAccountActionTrace_(nullptr, cass_prepared_free),
    gPreparedInsertAccountActionTraceWithParent_(nullptr, cass_prepared_free),
    gPreparedInsertAccountActionTraceShard_(nullptr, cass_prepared_free),
    gPreparedInsertDateActionTrace_(nullptr, cass_prepared_free),
    gPreparedInsertDateActionTraceWithParent_(nullptr, cass_prepared_free),
    gPreparedInsertActionTrace_(nullptr, cass_prepared_free),
    gPreparedInsertActionTraceWithParent_(nullptr, cass_prepared_free),
    gPreparedInsertBlock_(nullptr, cass_prepared_free),
    gPreparedInsertIrreversibleBlock_(nullptr, cass_prepared_free),
    gPreparedInsertTransaction_(nullptr, cass_prepared_free),
    gPreparedInsertTransactionTrace_(nullptr, cass_prepared_free),
    gPreparedUpdateIrreversible_(nullptr, cass_prepared_free)
{
    failed.add_index<eosio::upsert_account_multi_index>();
    failed.add_index<eosio::insert_account_action_trace_multi_index>();
    failed.add_index<eosio::insert_account_action_trace_shard_multi_index>();
    failed.add_index<eosio::insert_date_action_trace_multi_index>();
    failed.add_index<eosio::insert_action_trace_multi_index>();
    failed.add_index<eosio::insert_block_multi_index>();
    failed.add_index<eosio::insert_transaction_multi_index>();
    failed.add_index<eosio::insert_transaction_trace_multi_index>();
    
    CassCluster* cluster;
    CassSession* session;
    CassFuture* connectFuture;
    CassError err;

    cluster = cass_cluster_new();
    gCluster_.reset(cluster);
    cass_cluster_set_contact_points(cluster, hostUrl.c_str());

    session = cass_session_new();
    gSession_.reset(session);
    connectFuture = cass_session_connect(session, cluster);
    auto gFuture = future_guard(connectFuture, cass_future_free);
    err = cass_future_error_code(connectFuture);

    if (err == CASS_OK)
    {
        ilog("Connected to Apache Cassandra");
        if (dropKeyspace) {
            resetKeyspace();
        }
        else {
            init();
        }
        prepareStatements();
        insertFailed();
    }
    else
    {
        const char* message;
        size_t message_length;
        cass_future_error_message(connectFuture, &message, &message_length);
        std::string error_str(message, message_length);
        elog("Unable to connect to Apache Cassandra: ${desc}", ("desc", error_str));
        throw std::runtime_error(error_str);
    }
}

CassandraClient::~CassandraClient()
{
}


void CassandraClient::init()
{
    std::vector<std::string> queries = {
        "CREATE KEYSPACE " + keyspace_ + " WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': "
            + std::to_string(replicationFactor_) + " };",
        "USE " + keyspace_ + ";",
        "CREATE TABLE " + date_action_trace_table + " ( global_seq varint, block_date date, block_time timestamp, parent varint, "
            "PRIMARY KEY(block_date, block_time, global_seq));",
        "CREATE TABLE " + action_trace_table +
            " (part_key int, global_seq varint, parent varint, doc text, action_type text, receiver text, account text, PRIMARY KEY (part_key, global_seq));",
        "CREATE MATERIALIZED VIEW action_trace_by_action_type AS SELECT * FROM action_trace "
            "WHERE part_key IS NOT NULL AND global_seq IS NOT NULL AND action_type IS NOT NULL PRIMARY KEY ((part_key,action_type), global_seq);",
        "CREATE TABLE " + account_action_trace_shard_table + " ( account_name text, shard_id timestamp, PRIMARY KEY(account_name, shard_id));",
        "CREATE TABLE " + account_action_trace_table + " (shard_id timestamp, account_name text, global_seq varint, block_time timestamp, parent varint, "
            "PRIMARY KEY((account_name, shard_id), block_time, global_seq));",
        "CREATE TABLE " + transaction_table + " (id text, doc text, PRIMARY KEY(id));",
        "CREATE TABLE " + transaction_trace_table + " (id text, block_num varint, block_date date, doc text, PRIMARY KEY(id));",
        "CREATE TABLE " + block_table + " (id text, block_num varint, irreversible boolean, doc text, PRIMARY KEY(id));",
        "CREATE INDEX ON " + keyspace_ + "." + block_table + " (block_num);",
        "CREATE TABLE " + lib_table + " (part_key int, block_num varint, PRIMARY KEY(part_key));",
        "CREATE TABLE " + account_table + " (name text, creator text, account_create_time timestamp, abi text, PRIMARY KEY(name));",
        "CREATE TABLE " + account_public_key_table + " (name text, permission text, key text, PRIMARY KEY(name, permission, key));",
        "CREATE INDEX ON " + keyspace_ + "." + account_public_key_table + " (key);",
        "CREATE TABLE " + account_controlling_account_table + " (name text, controlling_name text, permission text, PRIMARY KEY(name, permission, controlling_name));",
        "CREATE INDEX ON " + keyspace_ + "." + account_controlling_account_table + " (controlling_name);",
        "INSERT INTO " + lib_table + " (part_key, block_num) VALUES(0, 0);"
    };
    for (const auto& query : queries)
    {
        auto gFuture = execute(query);
        auto cassFuture = gFuture.get();
        auto ec = cass_future_error_code(cassFuture);
        if (ec != CASS_OK && ec != CASS_ERROR_SERVER_ALREADY_EXISTS) {
            const char* message;
            size_t message_length;
            cass_future_error_message(cassFuture, &message, &message_length);
            if (ec == CASS_ERROR_SERVER_INVALID_QUERY) {
                wlog("Cassandra returned Invalid query error: ${desc}.\nMost likely this happened because cassandra tables was already set up."
                    " Other way any unsuccessful query will cause nodeos to stop.", ("desc", std::string(message, message_length)));
            }
            else {
                elog("Unable to run query: ${desc}", ("desc", std::string(message, message_length)));
                appbase::app().quit();
            }
        }
    }
}

void CassandraClient::insertFailed()
{
    const auto& accountUpsertsIdx = failed.get_index<eosio::upsert_account_multi_index, eosio::by_id>();
    const auto& insertAccountActionTraceIdx = failed.get_index<eosio::insert_account_action_trace_multi_index, eosio::by_account_shard_id>();
    const auto& insertAccountActionTraceShardIdx = failed.get_index<eosio::insert_account_action_trace_shard_multi_index, eosio::by_account_shard_id>();
    const auto& insertDateActionTraceIdx = failed.get_index<eosio::insert_date_action_trace_multi_index, eosio::by_id>();
    const auto& insertActionTraceIdx = failed.get_index<eosio::insert_action_trace_multi_index, eosio::by_id>();
    const auto& insertBlockIdx = failed.get_index<eosio::insert_block_multi_index, eosio::by_id>();
    const auto& insertTransactionIdx = failed.get_index<eosio::insert_transaction_multi_index, eosio::by_id>();
    const auto& insertTransactionTraceIdx = failed.get_index<eosio::insert_transaction_trace_multi_index, eosio::by_id>();
    std::vector<eosio::upsert_account_object> accUpserts;
    std::vector<eosio::insert_account_action_trace_object> accActionTraces;
    std::vector<eosio::insert_account_action_trace_shard_object> accActionTraceShards;
    std::vector<eosio::insert_date_action_trace_object> dateActionTraces;
    std::vector<eosio::insert_action_trace_object> actionTraces;
    std::vector<eosio::insert_block_object> blocks;
    std::vector<eosio::insert_transaction_object> transactions;
    std::vector<eosio::insert_transaction_trace_object> transactionTraces;

    while(!accountUpsertsIdx.empty()) {
        auto it = accountUpsertsIdx.begin();
        accUpserts.push_back(*it);
        failed.remove(*it);
    }
    while(!insertAccountActionTraceIdx.empty()) {
        auto it = insertAccountActionTraceIdx.begin();
        accActionTraces.push_back(*it);
        failed.remove(*it);
    }
    while(!insertAccountActionTraceShardIdx.empty()) {
        auto it = insertAccountActionTraceShardIdx.begin();
        accActionTraceShards.push_back(*it);
        failed.remove(*it);
    }
    while(!insertDateActionTraceIdx.empty()) {
        auto it = insertDateActionTraceIdx.begin();
        dateActionTraces.push_back(*it);
        failed.remove(*it);
    }
    while(!insertActionTraceIdx.empty()) {
        auto it = insertActionTraceIdx.begin();
        actionTraces.push_back(*it);
        failed.remove(*it);
    }
    while(!insertBlockIdx.empty()) {
        auto it = insertBlockIdx.begin();
        blocks.push_back(*it);
        failed.remove(*it);
    }
    while(!insertTransactionIdx.empty()) {
        auto it = insertTransactionIdx.begin();
        transactions.push_back(*it);
        failed.remove(*it);
    }
    while(!insertTransactionTraceIdx.empty()) {
        auto it = insertTransactionTraceIdx.begin();
        transactionTraces.push_back(*it);
        failed.remove(*it);
    }

    for (const auto& obj : accUpserts)
    {
        if( obj.name == eosio::chain::newaccount::get_name() ) {
            eosio::chain::newaccount newacc;
            fc::datastream<const char*> ds( obj.data.data(), obj.data.size() );
            fc::raw::unpack( ds, newacc );
            if (obj.blockTime.valid()) {
                insertAccount(newacc, *obj.blockTime);
            }
            else {

            }
        }
        else if( obj.name == eosio::chain::updateauth::get_name() ) {
            eosio::chain::updateauth update;
            fc::datastream<const char*> ds( obj.data.data(), obj.data.size() );
            fc::raw::unpack( ds, update );
            updateAccountAuth(update);
        }
        else if( obj.name == eosio::chain::deleteauth::get_name() ) {
            eosio::chain::deleteauth del;
            fc::datastream<const char*> ds( obj.data.data(), obj.data.size() );
            fc::raw::unpack( ds, del );
            deleteAccountAuth(del);
        }
        else if( obj.name == eosio::chain::setabi::get_name() ) {
            eosio::chain::setabi setabi;
            fc::datastream<const char*> ds( obj.data.data(), obj.data.size() );
            fc::raw::unpack( ds, setabi );
            updateAccountAbi(setabi);
        }
    }
    for (const auto& obj : accActionTraces)
    {
        eosio::chain::account_name account = obj.account;
        int64_t shardId = obj.shardId;
        std::vector<cass_byte_t> globalSeq;
        fc::time_point blockTime = obj.blockTime;
        std::vector<cass_byte_t> parent;
        globalSeq.resize( obj.globalSeq.size() );
        for (int i = 0; i < obj.globalSeq.size(); i++) {
            globalSeq[i] = obj.globalSeq[i];
        }
        parent.resize( obj.parent.size() );
        for (int i = 0; i < obj.parent.size(); i++) {
            parent[i] = obj.parent[i];
        }
        bool withParent = parent.size() != 0;
        if (withParent) {
            insertAccountActionTraceWithParent(account, shardId, globalSeq, blockTime, parent);
        }
        else {
            insertAccountActionTrace(account, shardId, globalSeq, blockTime);
        }
    }
    for (const auto& obj : accActionTraceShards)
    {
        insertAccountActionTraceShard(obj.account, obj.shardId);
    }
    for (const auto& obj : dateActionTraces)
    {
        std::vector<cass_byte_t> globalSeq;
        fc::time_point blockTime = obj.blockTime;
        std::vector<cass_byte_t> parent;
        globalSeq.resize( obj.globalSeq.size() );
        for (int i = 0; i < obj.globalSeq.size(); i++)
        {
            globalSeq[i] = obj.globalSeq[i];
        }
        parent.resize( obj.parent.size() );
        for (int i = 0; i < obj.parent.size(); i++)
        {
            parent[i] = obj.parent[i];
        }
        insertDateActionTrace(globalSeq, blockTime, parent);
    }
    for (const auto& obj : actionTraces)
    {
        std::vector<cass_byte_t> globalSeq;
        std::vector<cass_byte_t> parent;
        globalSeq.resize( obj.globalSeq.size() );
        for (int i = 0; i < obj.globalSeq.size(); i++)
        {
            globalSeq[i] = obj.globalSeq[i];
        }
        parent.resize( obj.parent.size() );
        for (int i = 0; i < obj.parent.size(); i++)
        {
            parent[i] = obj.parent[i];
        }
        std::string actionType = obj.actionType.data();
        std::string receiver = obj.receiver.data();
        std::string account = obj.account.data();
        if (!obj.actionTrace.empty())
        {
            std::string s;
            s = obj.actionTrace.data();
            insertActionTrace(globalSeq, std::move(s), actionType, receiver, account);
        }
        else
        {
            insertActionTraceWithParent(globalSeq, parent, actionType, receiver, account);
        }
    }
    for (const auto& obj : blocks)
    {
        std::string blockId = obj.blockId.data();
        std::vector<cass_byte_t> blockNum;
        blockNum.resize( obj.blockNum.size() );
        for (int i = 0; i < obj.blockNum.size(); i++)
        {
            blockNum[i] = obj.blockNum[i];
        }
        std::string block = obj.block.data();
        bool irreversible = obj.irreversible;
        insertBlock(blockId, blockNum, std::move(block), irreversible);
    }
    for (const auto& obj : transactions)
    {
        std::string transactionId = obj.transactionId.data();
        std::string transaction = obj.transaction.data();
        insertTransaction(transactionId, std::move(transaction));
    }
    for (const auto& obj : transactionTraces)
    {
        std::string transactionId = obj.transactionId.data();
        std::vector<cass_byte_t> blockNum;
        blockNum.resize( obj.blockNum.size() );
        for (int i = 0; i < obj.blockNum.size(); i++)
        {
            blockNum[i] = obj.blockNum[i];
        }
        fc::time_point blockTime = obj.blockTime;
        std::string transactionTrace = obj.transactionTrace.data();
        insertTransactionTrace(transactionId, blockNum, blockTime, std::move(transactionTrace));
    }
}

void CassandraClient::prepareStatements()
{
    std::string deleteAccountPublicKeysQuery = "DELETE FROM " + account_public_key_table +
        " WHERE name=? and permission=?";
    std::string deleteAccountControlsQuery = "DELETE FROM " + account_controlling_account_table +
        " WHERE name=? and permission=?";
    std::string insertAccountQuery = "INSERT INTO " + account_table +
        " (name, creator, account_create_time) VALUES(?, ?, ?)";
    std::string insertAccountAbiQuery = "INSERT INTO " + account_table +
        " (name, abi) VALUES (?, ?);";
    std::string insertAccountPublicKeysQuery = "INSERT INTO " + account_public_key_table +
        " (name, permission, key) VALUES(?, ?, ?)";
    std::string insertAccountControlsQuery = "INSERT INTO " + account_controlling_account_table +
        " (name, controlling_name, permission) VALUES(?, ?, ?)";
    std::string insertAccountActionTraceQuery = "INSERT INTO " + account_action_trace_table +
        " (account_name, shard_id, global_seq, block_time) VALUES(?, ?, ?, ?)";
    std::string insertAccountActionTraceWithParentQuery = "INSERT INTO " + account_action_trace_table +
        " (account_name, shard_id, global_seq, block_time, parent) VALUES(?, ?, ?, ?, ?)";
    std::string insertAccountActionTraceShardQuery = "INSERT INTO " + account_action_trace_shard_table +
        " (account_name, shard_id) VALUES(?, ?)";
    std::string insertDateActionTraceQuery = "INSERT INTO " + date_action_trace_table +
        " (global_seq, block_date, block_time) VALUES(?, ?, ?)";
    std::string insertDateActionTraceWithParentQuery = "INSERT INTO " + date_action_trace_table +
        " (global_seq, block_date, block_time, parent) VALUES(?, ?, ?, ?)";
    std::string insertActionTraceQuery = "INSERT INTO " + action_trace_table +
        " (part_key, global_seq, doc, action_type, receiver, account) VALUES(partition_by_sequence(?), ?, ?, ?, ?, ?)";
    std::string insertActionTraceWithParentQuery = "INSERT INTO " + action_trace_table +
        " (part_key, global_seq, parent, action_type, receiver, account) VALUES(partition_by_sequence(?), ?, ?, ?, ?, ?)";
    std::string insertBlockQuery = "INSERT INTO " + block_table +
        " (id, block_num, doc) VALUES(?, ?, ?)";
    std::string insertIrreversibleBlockQuery = "INSERT INTO " + block_table +
        " (id, block_num, irreversible, doc) VALUES(?, ?, true, ?)";
    std::string insertTransactionQuery = "INSERT INTO " + transaction_table +
        " (id, doc) VALUES(?, ?)";
    std::string insertTransactionTraceQuery = "INSERT INTO " + transaction_trace_table +
        " (id, block_num, block_date, doc) VALUES(?, ?, ?, ?)";
    std::string updateIrreversibleQuery = "UPDATE " + lib_table +
        " SET block_num=? where part_key=0";

    auto prepare = [this](const std::string& query, prepared_guard* prepared) -> bool
    {
        auto prepareFuture = cass_session_prepare(gSession_.get(), query.c_str());
        future_guard gFuture(prepareFuture, cass_future_free);
        auto rc = cass_future_error_code(prepareFuture);
        if (rc != CASS_OK) {
            elog("Prepare query error: ${desc}, query: ${query}",
                ("desc", cass_error_desc(rc))("query", query));
        }
        prepared->reset(cass_future_get_prepared(prepareFuture));
        return (rc == CASS_OK);
    };

    bool ok = true;
    ok &= prepare(deleteAccountPublicKeysQuery,            &gPreparedDeleteAccountPublicKeys_);
    ok &= prepare(deleteAccountControlsQuery,              &gPreparedDeleteAccountControls_);
    ok &= prepare(insertAccountQuery,                      &gPreparedInsertAccount_);
    ok &= prepare(insertAccountAbiQuery,                   &gPreparedInsertAccountAbi_);
    ok &= prepare(insertAccountPublicKeysQuery,            &gPreparedInsertAccountPublicKeys_);
    ok &= prepare(insertAccountControlsQuery,              &gPreparedInsertAccountControls_);
    ok &= prepare(insertAccountActionTraceQuery,           &gPreparedInsertAccountActionTrace_);
    ok &= prepare(insertAccountActionTraceWithParentQuery, &gPreparedInsertAccountActionTraceWithParent_);
    ok &= prepare(insertAccountActionTraceShardQuery,      &gPreparedInsertAccountActionTraceShard_);
    ok &= prepare(insertDateActionTraceQuery,              &gPreparedInsertDateActionTrace_);
    ok &= prepare(insertDateActionTraceWithParentQuery,    &gPreparedInsertDateActionTraceWithParent_);
    ok &= prepare(insertActionTraceQuery,                  &gPreparedInsertActionTrace_);
    ok &= prepare(insertActionTraceWithParentQuery,        &gPreparedInsertActionTraceWithParent_);
    ok &= prepare(insertBlockQuery,                        &gPreparedInsertBlock_);
    ok &= prepare(insertIrreversibleBlockQuery,            &gPreparedInsertIrreversibleBlock_);
    ok &= prepare(insertTransactionQuery,                  &gPreparedInsertTransaction_);
    ok &= prepare(insertTransactionTraceQuery,             &gPreparedInsertTransactionTrace_);
    ok &= prepare(updateIrreversibleQuery,                 &gPreparedUpdateIrreversible_);

    if (!ok)
    {
        appbase::app().quit();
    }
}

void CassandraClient::batchInsertDateActionTrace(
    const std::vector<std::tuple<std::vector<cass_byte_t>, fc::time_point, std::vector<cass_byte_t>>>& data)
{
    bool errorHandled = false;
    auto f = [&, this]()
    {
        if (errorHandled) return;

        std::lock_guard<std::mutex> lock(db_mtx);
        for (const auto& val : data)
        {
            std::vector<cass_byte_t> globalSeq;
            fc::time_point blockTime;
            std::vector<cass_byte_t> parent;
            std::tie(globalSeq, blockTime, parent) = val;

            failed.create<eosio::insert_date_action_trace_object>([&]( auto& obj ) {
                obj.setGlobalSeq(globalSeq);
                obj.blockTime = blockTime;
                obj.setParent(parent);
            });
        }
        errorHandled = true; //needs to be set so only one object will be written to db even if multiple waitFuture fail
    };
    exit_scope guard(f); //in case of exception

    CassStatement* statement = nullptr;
    auto batch = cass_batch_new(CASS_BATCH_TYPE_LOGGED);
    batch_guard gBatch(batch, cass_batch_free);
    size_t batchCount = 0;

    for (const auto& val : data)
    {
        std::vector<cass_byte_t> globalSeq;
        fc::time_point blockTime;
        std::vector<cass_byte_t> parent;
        std::tie(globalSeq, blockTime, parent) = val;
        bool withParent = !parent.empty();
        statement = withParent ?
            cass_prepared_bind(gPreparedInsertDateActionTraceWithParent_.get()) :
            cass_prepared_bind(gPreparedInsertDateActionTrace_.get());
        auto gStatement = statement_guard(statement, cass_statement_free);
        cass_uint32_t blockDate = cass_date_from_epoch(blockTime.sec_since_epoch());
        int64_t msFromEpoch = (int64_t)blockTime.time_since_epoch().count() / 1000;
        cass_statement_bind_bytes_by_name(statement, "global_seq", globalSeq.data(), globalSeq.size());
        cass_statement_bind_uint32_by_name(statement, "block_date", blockDate);
        cass_statement_bind_int64_by_name(statement, "block_time", msFromEpoch);
        if (withParent) {
            cass_statement_bind_bytes_by_name(statement, "parent", parent.data(), parent.size());
        }
        cass_batch_add_statement(batch, statement);

        batchCount++;
        if (batchCount == 10) {
            executeWait(std::move(gBatch), f);

            batch = cass_batch_new(CASS_BATCH_TYPE_LOGGED);
            gBatch.reset(batch);
            batchCount = 0;
        }
    }
    if (batchCount > 0) {
        executeWait(std::move(gBatch), f);
    }
    guard.reset(); //no exception occured
}

void CassandraClient::batchInsertAccountActionTrace(
    const std::vector<std::tuple<eosio::chain::account_name, int64_t, std::vector<cass_byte_t>, fc::time_point, std::vector<cass_byte_t>>>& data)
{
    bool errorHandled = false;
    auto f = [&, this]()
    {
        if (errorHandled) return;

        std::lock_guard<std::mutex> lock(db_mtx);
        for (const auto& val : data)
        {
            eosio::chain::account_name account;
            int64_t shardId;
            std::vector<cass_byte_t> globalSeq;
            fc::time_point blockTime;
            std::vector<cass_byte_t> parent;
            std::tie(account, shardId, globalSeq, blockTime, parent) = val;
            bool withParent = parent.size() != 0;

            failed.create<eosio::insert_account_action_trace_object>([&]( auto& obj ) {
                obj.account = account;
                obj.shardId = shardId;
                obj.setGlobalSeq(globalSeq);
                obj.blockTime = blockTime;
                if (withParent) {
                    obj.setParent(parent);
                }
            });
        }
        errorHandled = true; //needs to be set so only one object will be written to db even if multiple waitFuture fail
    };
    exit_scope guard(f);

    CassStatement* statement = nullptr;
    auto batch = cass_batch_new(CASS_BATCH_TYPE_LOGGED);
    batch_guard gBatch(batch, cass_batch_free);
    size_t batchCount = 0;

    for (const auto& val : data)
    {
        eosio::chain::account_name account;
        int64_t shardId;
        std::vector<cass_byte_t> globalSeq;
        fc::time_point blockTime;
        std::vector<cass_byte_t> parent;
        std::tie(account, shardId, globalSeq, blockTime, parent) = val;
        bool withParent = parent.size() != 0;
        statement = withParent ?
            cass_prepared_bind(gPreparedInsertAccountActionTraceWithParent_.get()) :
            cass_prepared_bind(gPreparedInsertAccountActionTrace_.get());
        auto gStatement = statement_guard(statement, cass_statement_free);

        int64_t msFromEpoch = (int64_t)blockTime.time_since_epoch().count() / 1000;
        cass_statement_bind_string_by_name(statement, "account_name", std::string(account).c_str());
        cass_statement_bind_int64_by_name(statement, "shard_id", shardId);
        cass_statement_bind_bytes_by_name(statement, "global_seq", globalSeq.data(), globalSeq.size());
        cass_statement_bind_int64_by_name(statement, "block_time", msFromEpoch);
        if (withParent) {
            cass_statement_bind_bytes_by_name(statement, "parent", parent.data(), parent.size());
        }
        cass_batch_add_statement(batch, statement);

        batchCount++;
        if (batchCount == 10) {
            executeWait(std::move(gBatch), f);

            batch = cass_batch_new(CASS_BATCH_TYPE_LOGGED);
            gBatch.reset(batch);
            batchCount = 0;
        }
    }
    if (batchCount > 0) {
        executeWait(std::move(gBatch), f);
    }
    guard.reset();
}

void CassandraClient::insertAccount(
    const eosio::chain::newaccount& newacc,
    fc::time_point blockTime)
{
    bool errorHandled = false;
    auto f = [this, &newacc, &blockTime, &errorHandled]()
    {
        if (errorHandled) return;
        std::lock_guard<std::mutex> lock(db_mtx);

        failed.create<eosio::upsert_account_object>([&]( auto& obj ) {
            obj.name = eosio::chain::newaccount::get_name();
            obj.blockTime = blockTime;
            obj.data.resize( fc::raw::pack_size( newacc ) );
            fc::datastream<char*> ds( obj.data.data(), obj.data.size() );
            fc::raw::pack( ds, newacc );
        });
        errorHandled = true; //needs to be set so only one object will be written to db even if multiple waitFuture fail
    };
    exit_scope guard(f);

    CassStatement* statement = nullptr;
    CassBatch*         batch = nullptr;
    CassFuture*       future = nullptr;
    statement_guard gStatement(nullptr, cass_statement_free);
    batch_guard gBatch(nullptr, cass_batch_free);

    statement = cass_prepared_bind(gPreparedInsertAccount_.get());
    gStatement.reset(statement);
    int64_t msFromEpoch = (int64_t)blockTime.time_since_epoch().count() / 1000;
    cass_statement_bind_string_by_name(statement, "name", newacc.name.to_string().c_str());
    cass_statement_bind_string_by_name(statement, "creator", newacc.creator.to_string().c_str());
    cass_statement_bind_int64_by_name(statement, "account_create_time", msFromEpoch);
    executeWait(std::move(gStatement), f);

    batch = cass_batch_new(CASS_BATCH_TYPE_UNLOGGED);
    gBatch.reset(batch);
    for( const auto& account : newacc.owner.accounts ) {
        statement = cass_prepared_bind(gPreparedInsertAccountControls_.get());
        gStatement.reset(statement);
        cass_statement_bind_string_by_name(statement, "name", newacc.name.to_string().c_str());
        cass_statement_bind_string_by_name(statement, "controlling_name", account.permission.actor.to_string().c_str());
        cass_statement_bind_string_by_name(statement, "permission", "owner");
        cass_batch_add_statement(batch, statement);
    }
    for( const auto& account : newacc.active.accounts ) {
        statement = cass_prepared_bind(gPreparedInsertAccountControls_.get());
        gStatement.reset(statement);
        cass_statement_bind_string_by_name(statement, "name", newacc.name.to_string().c_str());
        cass_statement_bind_string_by_name(statement, "controlling_name", account.permission.actor.to_string().c_str());
        cass_statement_bind_string_by_name(statement, "permission", "active");
        cass_batch_add_statement(batch, statement);
    }
    executeWait(std::move(gBatch), f);

    batch = cass_batch_new(CASS_BATCH_TYPE_UNLOGGED);
    gBatch.reset(batch);
    for( const auto& pub_key_weight : newacc.owner.keys ) {
        statement = cass_prepared_bind(gPreparedInsertAccountPublicKeys_.get());
        gStatement.reset(statement);
        cass_statement_bind_string_by_name(statement, "name", newacc.name.to_string().c_str());
        cass_statement_bind_string_by_name(statement, "permission", "owner");
        cass_statement_bind_string_by_name(statement, "key", pub_key_weight.key.operator std::string().c_str());
        cass_batch_add_statement(batch, statement);
    }
    for( const auto& pub_key_weight : newacc.active.keys ) {
        statement = cass_prepared_bind(gPreparedInsertAccountPublicKeys_.get());
        gStatement.reset(statement);
        cass_statement_bind_string_by_name(statement, "name", newacc.name.to_string().c_str());
        cass_statement_bind_string_by_name(statement, "permission", "active");
        cass_statement_bind_string_by_name(statement, "key", pub_key_weight.key.operator std::string().c_str());
        cass_batch_add_statement(batch, statement);
    }
    executeWait(std::move(gBatch), f);

    guard.reset();
}

void CassandraClient::deleteAccountAuth(
    const eosio::chain::deleteauth& del)
{
    bool errorHandled = false;
    auto f = [this, &del, &errorHandled]()
    {
        if (errorHandled) return;
        std::lock_guard<std::mutex> lock(db_mtx);

        failed.create<eosio::upsert_account_object>([&]( auto& obj ) {
            obj.name = eosio::chain::deleteauth::get_name();
            obj.data.resize( fc::raw::pack_size( del ) );
            fc::datastream<char*> ds( obj.data.data(), obj.data.size() );
            fc::raw::pack( ds, del );
        });
        errorHandled = true; //needs to be set so only one object will be written to db even if multiple waitFuture fail
    };
    exit_scope guard(f);

    CassStatement* statement = nullptr;
    statement_guard gStatement(nullptr, cass_statement_free);

    statement = cass_prepared_bind(gPreparedDeleteAccountPublicKeys_.get());
    gStatement.reset(statement);
    cass_statement_bind_string_by_name(statement, "name", del.account.to_string().c_str());
    cass_statement_bind_string_by_name(statement, "permission", del.permission.to_string().c_str());
    executeWait(std::move(gStatement), f);

    statement = cass_prepared_bind(gPreparedDeleteAccountControls_.get());
    gStatement.reset(statement);
    cass_statement_bind_string_by_name(statement, "name", del.account.to_string().c_str());
    cass_statement_bind_string_by_name(statement, "permission", del.permission.to_string().c_str());
    executeWait(std::move(gStatement), f);

    guard.reset();
}

void CassandraClient::updateAccountAuth(
    const eosio::chain::updateauth& update)
{
    bool errorHandled = false;
    auto f = [this, &update, &errorHandled]()
    {
        if (errorHandled) return;
        std::lock_guard<std::mutex> lock(db_mtx);

        failed.create<eosio::upsert_account_object>([&]( auto& obj ) {
            obj.name = eosio::chain::updateauth::get_name();
            obj.data.resize( fc::raw::pack_size( update ) );
            fc::datastream<char*> ds( obj.data.data(), obj.data.size() );
            fc::raw::pack( ds, update );
        });
        errorHandled = true; //needs to be set so only one object will be written to db even if multiple waitFuture fail
    };
    exit_scope guard(f);

    CassStatement* statement = nullptr;
    CassBatch*         batch = nullptr;
    CassFuture*       future = nullptr;
    statement_guard gStatement(nullptr, cass_statement_free);
    batch_guard gBatch(nullptr, cass_batch_free);

    statement = cass_prepared_bind(gPreparedDeleteAccountPublicKeys_.get());
    gStatement.reset(statement);
    cass_statement_bind_string_by_name(statement, "name", update.account.to_string().c_str());
    cass_statement_bind_string_by_name(statement, "permission", update.permission.to_string().c_str());
    executeWait(std::move(gStatement), f);

    statement = cass_prepared_bind(gPreparedDeleteAccountControls_.get());
    gStatement.reset(statement);
    cass_statement_bind_string_by_name(statement, "name", update.account.to_string().c_str());
    cass_statement_bind_string_by_name(statement, "permission", update.permission.to_string().c_str());
    executeWait(std::move(gStatement), f);

    batch = cass_batch_new(CASS_BATCH_TYPE_UNLOGGED);
    gBatch.reset(batch);
    for( const auto& pub_key_weight : update.auth.keys ) {
        statement = cass_prepared_bind(gPreparedInsertAccountPublicKeys_.get());
        gStatement.reset(statement);
        cass_statement_bind_string_by_name(statement, "name", update.account.to_string().c_str());
        cass_statement_bind_string_by_name(statement, "permission", update.permission.to_string().c_str());
        cass_statement_bind_string_by_name(statement, "key", pub_key_weight.key.operator std::string().c_str());
        cass_batch_add_statement(batch, statement);
    }
    executeWait(std::move(gBatch), f);

    batch = cass_batch_new(CASS_BATCH_TYPE_UNLOGGED);
    gBatch.reset(batch);
    for( const auto& account : update.auth.accounts ) {
        statement = cass_prepared_bind(gPreparedInsertAccountControls_.get());
        gStatement.reset(statement);
        cass_statement_bind_string_by_name(statement, "name", update.account.to_string().c_str());
        cass_statement_bind_string_by_name(statement, "controlling_name", account.permission.actor.to_string().c_str());
        cass_statement_bind_string_by_name(statement, "permission", update.permission.to_string().c_str());
        cass_batch_add_statement(batch, statement);
    }
    executeWait(std::move(gBatch), f);

    guard.reset();
}

void CassandraClient::updateAccountAbi(
    const eosio::chain::setabi& setabi)
{
    bool errorHandled = false;
    auto f = [this, &setabi, &errorHandled]()
    {
        if (errorHandled) return;
        std::lock_guard<std::mutex> lock(db_mtx);

        failed.create<eosio::upsert_account_object>([&]( auto& obj ) {
            obj.name = eosio::chain::setabi::get_name();
            obj.data.resize( fc::raw::pack_size( setabi ) );
            fc::datastream<char*> ds( obj.data.data(), obj.data.size() );
            fc::raw::pack( ds, setabi );
        });
        errorHandled = true; //needs to be set so only one object will be written to db even if multiple waitFuture fail
    };
    exit_scope guard(f);

    auto statement = cass_prepared_bind(gPreparedInsertAccountAbi_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_statement_bind_string_by_name(statement, "name", setabi.account.to_string().c_str());
    cass_statement_bind_string_by_name(statement, "abi", fc::json::to_string(fc::variant(setabi.abi)).c_str());
    executeWait(std::move(gStatement), f);

    guard.reset();
}

void CassandraClient::insertAccountActionTrace(
    const eosio::chain::account_name& account,
    int64_t shardId,
    std::vector<cass_byte_t> globalSeq,
    fc::time_point blockTime)
{
    bool errorHandled = false;
    auto f = [&, this]()
    {
        if (errorHandled) return;
        std::lock_guard<std::mutex> lock(db_mtx);

        failed.create<eosio::insert_account_action_trace_object>([&]( auto& obj ) {
            obj.account = account;
            obj.shardId = shardId;
            obj.setGlobalSeq(globalSeq);
            obj.blockTime = blockTime;
        });
        errorHandled = true; //needs to be set so only one object will be written to db even if multiple waitFuture fail
    };
    exit_scope guard(f);

    int64_t msFromEpoch = (int64_t)blockTime.time_since_epoch().count() / 1000;
    auto statement = cass_prepared_bind(gPreparedInsertAccountActionTrace_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_statement_bind_string_by_name(statement, "account_name", std::string(account).c_str());
    cass_statement_bind_int64_by_name(statement, "shard_id", shardId);
    cass_statement_bind_bytes_by_name(statement, "global_seq", globalSeq.data(), globalSeq.size());
    cass_statement_bind_int64_by_name(statement, "block_time", msFromEpoch);
    executeWait(std::move(gStatement), f);

    guard.reset();
}

void CassandraClient::insertAccountActionTraceWithParent(
    const eosio::chain::account_name& account,
    int64_t shardId,
    std::vector<cass_byte_t> globalSeq,
    fc::time_point blockTime,
    std::vector<cass_byte_t> parent)
{
    bool errorHandled = false;
    auto f = [&, this]()
    {
        if (errorHandled) return;
        std::lock_guard<std::mutex> lock(db_mtx);

        failed.create<eosio::insert_account_action_trace_object>([&]( auto& obj ) {
            obj.account = account;
            obj.shardId = shardId;
            obj.setGlobalSeq(globalSeq);
            obj.blockTime = blockTime;
            obj.setParent(parent);
        });
        errorHandled = true; //needs to be set so only one object will be written to db even if multiple waitFuture fail
    };
    exit_scope guard(f);

    int64_t msFromEpoch = (int64_t)blockTime.time_since_epoch().count() / 1000;
    auto statement = cass_prepared_bind(gPreparedInsertAccountActionTraceWithParent_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_statement_bind_string_by_name(statement, "account_name", std::string(account).c_str());
    cass_statement_bind_int64_by_name(statement, "shard_id", shardId);
    cass_statement_bind_bytes_by_name(statement, "global_seq", globalSeq.data(), globalSeq.size());
    cass_statement_bind_int64_by_name(statement, "block_time", msFromEpoch);
    cass_statement_bind_bytes_by_name(statement, "parent", parent.data(), parent.size());
    executeWait(std::move(gStatement), f);

    guard.reset();
}

void CassandraClient::insertAccountActionTraceShard(
    const eosio::chain::account_name& account,
    int64_t shardId)
{
    bool errorHandled = false;
    auto f = [&, this]()
    {
        if (errorHandled) return;
        std::lock_guard<std::mutex> lock(db_mtx);

        failed.create<eosio::insert_account_action_trace_shard_object>([&]( auto& obj ) {
            obj.account = account;
            obj.shardId = shardId;
        });
        errorHandled = true; //needs to be set so only one object will be written to db even if multiple waitFuture fail
    };
    exit_scope guard(f);

    auto statement = cass_prepared_bind(gPreparedInsertAccountActionTraceShard_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_statement_bind_string_by_name(statement, "account_name", std::string(account).c_str());
    cass_statement_bind_int64_by_name(statement, "shard_id", shardId);
    executeWait(std::move(gStatement), f);

    guard.reset();
}

void CassandraClient::insertDateActionTrace(
    std::vector<cass_byte_t> globalSeq,
    fc::time_point blockTime,
    std::vector<cass_byte_t> parent)
{
    bool errorHandled = false;
    auto f = [&, this]()
    {
        if (errorHandled) return;
        std::lock_guard<std::mutex> lock(db_mtx);

        failed.create<eosio::insert_date_action_trace_object>([&]( auto& obj ) {
            obj.setGlobalSeq(globalSeq);
            obj.blockTime = blockTime;
            obj.setParent(parent);
        });
        errorHandled = true; //needs to be set so only one object will be written to db even if multiple waitFuture fail
    };
    exit_scope guard(f);

    bool withParent = !parent.empty();
    auto statement = withParent ?
        cass_prepared_bind(gPreparedInsertDateActionTraceWithParent_.get()) :
        cass_prepared_bind(gPreparedInsertDateActionTrace_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_uint32_t blockDate = cass_date_from_epoch(blockTime.sec_since_epoch());
    int64_t msFromEpoch = (int64_t)blockTime.time_since_epoch().count() / 1000;
    cass_statement_bind_bytes_by_name(statement, "global_seq", globalSeq.data(), globalSeq.size());
    cass_statement_bind_uint32_by_name(statement, "block_date", blockDate);
    cass_statement_bind_int64_by_name(statement, "block_time", msFromEpoch);
    if (withParent) {
        cass_statement_bind_bytes_by_name(statement, "parent", parent.data(), parent.size());
    }
    executeWait(std::move(gStatement), f);

    guard.reset();
}

void CassandraClient::insertActionTrace(
    std::vector<cass_byte_t> globalSeq,
    std::string&& actionTrace,
    const std::string& actionType,
    const std::string& receiver,
    const std::string& account)
{
    bool errorHandled = false;
    auto f = [&, this]()
    {
        if (errorHandled) return;
        std::lock_guard<std::mutex> lock(db_mtx);

        failed.create<eosio::insert_action_trace_object>([&]( auto& obj ) {
            obj.setGlobalSeq(globalSeq);
            obj.actionTrace.resize(actionTrace.size());
            std::copy(actionTrace.begin(), actionTrace.end(),
                obj.actionTrace.begin());

            obj.actionType.resize(actionType.size());
            std::copy(actionType.begin(), actionType.end(),
                obj.actionType.begin());
            obj.receiver.resize(receiver.size());
            std::copy(receiver.begin(), receiver.end(),
                obj.receiver.begin());
            obj.account.resize(account.size());
            std::copy(account.begin(), account.end(),
                obj.account.begin());
        });
        errorHandled = true; //needs to be set so only one object will be written to db even if multiple waitFuture fail
    };
    exit_scope guard(f);

    auto statement = cass_prepared_bind(gPreparedInsertActionTrace_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_statement_bind_bytes_by_name(statement, "part_key", globalSeq.data(), globalSeq.size());
    cass_statement_bind_bytes_by_name(statement, "global_seq", globalSeq.data(), globalSeq.size());
    cass_statement_bind_string_by_name(statement, "doc", actionTrace.c_str());
    cass_statement_bind_string_by_name(statement, "action_type", actionType.c_str());
    cass_statement_bind_string_by_name(statement, "receiver", receiver.c_str());
    cass_statement_bind_string_by_name(statement, "account", account.c_str());
    executeWait(std::move(gStatement), f);

    guard.reset();
}

void CassandraClient::insertActionTraceWithParent(
    std::vector<cass_byte_t> globalSeq,
    std::vector<cass_byte_t> parent,
    const std::string& actionType,
    const std::string& receiver,
    const std::string& account)
{
    bool errorHandled = false;
    auto f = [&, this]()
    {
        if (errorHandled) return;
        std::lock_guard<std::mutex> lock(db_mtx);

        failed.create<eosio::insert_action_trace_object>([&]( auto& obj ) {
            obj.setGlobalSeq(globalSeq);
            obj.setParent(parent);

            obj.actionType.resize(actionType.size());
            std::copy(actionType.begin(), actionType.end(),
                obj.actionType.begin());
            obj.receiver.resize(receiver.size());
            std::copy(receiver.begin(), receiver.end(),
                obj.receiver.begin());
            obj.account.resize(account.size());
            std::copy(account.begin(), account.end(),
                obj.account.begin());
        });
        errorHandled = true; //needs to be set so only one object will be written to db even if multiple waitFuture fail
    };
    exit_scope guard(f);

    auto statement = cass_prepared_bind(gPreparedInsertActionTraceWithParent_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_statement_bind_bytes_by_name(statement, "part_key", globalSeq.data(), globalSeq.size());
    cass_statement_bind_bytes_by_name(statement, "global_seq", globalSeq.data(), globalSeq.size());
    cass_statement_bind_bytes_by_name(statement, "parent", parent.data(), parent.size());
    cass_statement_bind_string_by_name(statement, "action_type", actionType.c_str());
    cass_statement_bind_string_by_name(statement, "receiver", receiver.c_str());
    cass_statement_bind_string_by_name(statement, "account", account.c_str());
    executeWait(std::move(gStatement), f);

    guard.reset();
}

void CassandraClient::insertBlock(
    const std::string& id,
    std::vector<cass_byte_t> blockNumBuffer,
    std::string&& block,
    bool irreversible)
{
    bool errorHandled = false;
    auto f = [&, this]()
    {
        if (errorHandled) return;
        std::lock_guard<std::mutex> lock(db_mtx);

        failed.create<eosio::insert_block_object>([&]( auto& obj ) {
            obj.blockId.resize(id.size());
            std::copy(id.begin(), id.end(), obj.blockId.begin());
            obj.setBlockNum(blockNumBuffer);
            obj.block.resize(block.size());
            std::copy(block.begin(), block.end(), obj.block.begin());
            obj.irreversible = irreversible;
        });
        errorHandled = true; //needs to be set so only one object will be written to db even if multiple waitFuture fail
    };
    exit_scope guard(f);

    CassStatement* statement = nullptr;
    if (irreversible) {
        statement = cass_prepared_bind(gPreparedInsertIrreversibleBlock_.get());
    }
    else {
        statement = cass_prepared_bind(gPreparedInsertBlock_.get());
    }
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_statement_bind_string_by_name(statement, "id", id.c_str());
    cass_statement_bind_bytes_by_name(statement, "block_num", blockNumBuffer.data(), blockNumBuffer.size());
    cass_statement_bind_string_by_name(statement, "doc", block.c_str());
    executeWait(std::move(gStatement), f);

    if (irreversible) {
        statement = cass_prepared_bind(gPreparedUpdateIrreversible_.get());
        gStatement.reset(statement);
        cass_statement_bind_bytes_by_name(statement, "block_num", blockNumBuffer.data(), blockNumBuffer.size());
        executeWait(std::move(gStatement), f);
    }
    guard.reset();
}

void CassandraClient::insertTransaction(
    const std::string& id,
    std::string&& transaction)
{
    bool errorHandled = false;
    auto f = [&, this]()
    {
        if (errorHandled) return;
        std::lock_guard<std::mutex> lock(db_mtx);

        failed.create<eosio::insert_transaction_object>([&]( auto& obj ) {
            obj.transactionId.resize(id.size());
            std::copy(id.begin(), id.end(), obj.transactionId.begin());
            obj.transaction.resize(transaction.size());
            std::copy(transaction.begin(), transaction.end(), obj.transaction.begin());
        });
        errorHandled = true; //needs to be set so only one object will be written to db even if multiple waitFuture fail
    };
    exit_scope guard(f);

    auto statement = cass_prepared_bind(gPreparedInsertTransaction_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_statement_bind_string_by_name(statement, "id", id.c_str());
    cass_statement_bind_string_by_name(statement, "doc", transaction.c_str());
    executeWait(std::move(gStatement), f);

    guard.reset();
}

void CassandraClient::insertTransactionTrace(
    const std::string& id,
    std::vector<cass_byte_t> blockNumBuffer,
    fc::time_point blockTime,
    std::string&& transactionTrace)
{
    bool errorHandled = false;
    auto f = [&, this]()
    {
        if (errorHandled) return;
        std::lock_guard<std::mutex> lock(db_mtx);

        failed.create<eosio::insert_transaction_trace_object>([&]( auto& obj ) {
            obj.transactionId.resize(id.size());
            std::copy(id.begin(), id.end(), obj.transactionId.begin());
            obj.setBlockNum(blockNumBuffer);
            obj.blockTime = blockTime;
            obj.transactionTrace.resize(transactionTrace.size());
            std::copy(transactionTrace.begin(), transactionTrace.end(), obj.transactionTrace.begin());
        });
        errorHandled = true; //needs to be set so only one object will be written to db even if multiple waitFuture fail
    };
    exit_scope guard(f);

    auto statement = cass_prepared_bind(gPreparedInsertTransactionTrace_.get());
    auto gStatement = statement_guard(statement, cass_statement_free);
    cass_uint32_t blockDate = cass_date_from_epoch(blockTime.sec_since_epoch());
    cass_statement_bind_string_by_name(statement, "id", id.c_str());
    cass_statement_bind_bytes_by_name(statement, "block_num", blockNumBuffer.data(), blockNumBuffer.size());
    cass_statement_bind_uint32_by_name(statement, "block_date", blockDate);
    cass_statement_bind_string_by_name(statement, "doc", transactionTrace.c_str());
    executeWait(std::move(gStatement), f);

    guard.reset();
}

void CassandraClient::clearFollowingShards(
    const eosio::chain::account_name& account,
    int64_t prevShardId)
{
    std::string selectQuery = "select * from " + account_action_trace_shard_table +
        " where account_name=? and shard_id>?;";
    auto selectStatement = cass_statement_new(selectQuery.c_str(), 2);
    auto gStatement = statement_guard(selectStatement, cass_statement_free);
    cass_statement_bind_string(selectStatement, 0, std::string(account).c_str());
    cass_statement_bind_int64(selectStatement, 1, prevShardId);
    auto gFuture = execute(std::move(gStatement));
    if (cass_future_error_code(gFuture.get()) != CASS_OK) {
        elog("Failed to select shards: ${query}", ("query", selectQuery));
        appbase::app().quit();
        return;
    }
    const CassResult* result = cass_future_get_result(gFuture.get());
    auto gResult = result_guard(result, cass_result_free);
    if (result == nullptr) {
        return;
    }

    auto rowCount = cass_result_row_count(result);
    if (rowCount > 0) {
        ilog("Deleting ${n} shards for ${acc}", ("n", rowCount)("acc", std::string(account)));
        return;
    }
    cass_int64_t lastShardId = 0;
    CassIterator* iterator = cass_iterator_from_result(result);
    auto gIterator = iterator_guard(iterator, cass_iterator_free);
    while (cass_iterator_next(iterator)) {
        const CassRow* row = cass_iterator_get_row(iterator);
        const CassValue* column1 = cass_row_get_column_by_name(row, "account_name");
        const CassValue* column2 = cass_row_get_column_by_name(row, "shard_id");

        const char* accountName;
        size_t accountNameLength;
        cass_value_get_string(column1, &accountName, &accountNameLength);

        cass_int64_t shardId;
        cass_value_get_int64(column2, &shardId);
        lastShardId = shardId;

        std::string deleteShardQuery = "delete from " + account_action_trace_table +
            " where account_name=? and shard_id=?;";
        auto deleteShardStatement = cass_statement_new(deleteShardQuery.c_str(), 2);
        gStatement = statement_guard(deleteShardStatement, cass_statement_free);
        cass_statement_bind_string(deleteShardStatement, 0, accountName);
        cass_statement_bind_int64(deleteShardStatement, 1, shardId);
        gFuture = execute(std::move(gStatement));
        if (cass_future_error_code(gFuture.get()) != CASS_OK) {
            elog("Delete from shard failed: ${query}", ("query", deleteShardQuery));
            appbase::app().quit();
            return;
        }
    }

    std::string deleteQuery = "delete from " + account_action_trace_shard_table +
        " where account_name=? and shard_id>? and shard_id<=?;";
    auto deleteStatement = cass_statement_new(deleteQuery.c_str(), 3);
    gStatement = statement_guard(deleteStatement, cass_statement_free);
    cass_statement_bind_string(deleteStatement, 0, std::string(account).c_str());
    cass_statement_bind_int64(deleteStatement, 1, prevShardId);
    cass_statement_bind_int64(deleteStatement, 2, lastShardId);
    gFuture = execute(std::move(gStatement));
    if (cass_future_error_code(gFuture.get()) != CASS_OK) {
        elog("Delete shards failed: ${query}", ("query", deleteQuery));
        appbase::app().quit();
        return;
    }
}


void CassandraClient::resetKeyspace()
{
    auto gFuture = execute("DROP KEYSPACE " + keyspace_ + ";");
    auto cassFuture = gFuture.get();
    auto ec = cass_future_error_code(cassFuture);
    if (ec != CASS_OK) {
        const char* message;
        size_t message_length;
        cass_future_error_message(cassFuture, &message, &message_length);
        if (ec == CASS_ERROR_SERVER_CONFIG_ERROR) {
            wlog("Cassandra returned Config error: ${desc}.\nMost likely this happened because you run on clean cassandra.",
                ("desc", std::string(message, message_length)));
        }
        else {
            elog("Unable to run query: ${desc}", ("desc", std::string(message, message_length)));
            appbase::app().quit();
        }
    }
    init();
}


bool CassandraClient::checkTimeout(future_guard& gFuture) const
{
    auto ec = cass_future_error_code(gFuture.get());
    return (ec == CASS_ERROR_LIB_REQUEST_TIMED_OUT ||
        ec == CASS_ERROR_SERVER_WRITE_TIMEOUT);
}

future_guard CassandraClient::execute(const std::string& query)
{
    auto statement = cass_statement_new(query.c_str(), 0);
    auto gStatement = statement_guard(statement, cass_statement_free);
    return execute(std::move(gStatement));
}

future_guard CassandraClient::execute(batch_guard& b)
{
    auto resultFuture = cass_session_execute_batch(gSession_.get(), b.get());
    return future_guard(resultFuture, cass_future_free);
}

future_guard CassandraClient::execute(batch_guard&& b)
{
    auto resultFuture = cass_session_execute_batch(gSession_.get(), b.get());
    return future_guard(resultFuture, cass_future_free);
}

future_guard CassandraClient::execute(statement_guard& gStatement)
{
    auto resultFuture = cass_session_execute(gSession_.get(), gStatement.get());
    return future_guard(resultFuture, cass_future_free);
}

future_guard CassandraClient::execute(statement_guard&& gStatement)
{
    auto resultFuture = cass_session_execute(gSession_.get(), gStatement.get());
    return future_guard(resultFuture, cass_future_free);
}

void CassandraClient::executeWait(batch_guard&& gBatch, const std::function<void()>& onError)
{
    auto gFuture = execute(gBatch);
    size_t n = 0;
    while (n++ < max_retries_ && checkTimeout(gFuture)) {
        ilog("Retrying to execute batch");
        std::this_thread::sleep_for(std::chrono::milliseconds(retry_delay_ms_));
        gFuture = execute(gBatch);
    }
    waitFuture(std::move(gFuture), onError);
}

void CassandraClient::executeWait(statement_guard&& gStatement, const std::function<void()>& onError)
{
    auto gFuture = execute(gStatement);
    size_t n = 0;
    while (n++ < max_retries_ && checkTimeout(gFuture)) {
        ilog("Retrying to execute statement");
        std::this_thread::sleep_for(std::chrono::milliseconds(retry_delay_ms_));
        gFuture = execute(gStatement);
    }
    waitFuture(std::move(gFuture), onError);
}

void CassandraClient::waitFuture(future_guard&& gFuture)
{
    auto cassFuture = gFuture.get();
    if (cass_future_error_code(cassFuture) != CASS_OK) {
        const char* message;
        size_t message_length;
        cass_future_error_message(cassFuture, &message, &message_length);
        elog("Unable to run query: ${desc}", ("desc", std::string(message, message_length)));
        appbase::app().quit();
    }
}

void CassandraClient::waitFuture(future_guard&& gFuture, const std::function<void()>& onError)
{
    auto cassFuture = gFuture.get();
    if (cass_future_error_code(cassFuture) != CASS_OK) {
        onError();
        
        const char* message;
        size_t message_length;
        cass_future_error_message(cassFuture, &message, &message_length);
        elog("Unable to run query: ${desc}", ("desc", std::string(message, message_length)));
        appbase::app().quit();
    }
}