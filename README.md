# cassandra_history_plugin (alpha)
Nodeos plugin for archiving blockchain data into Apache Cassandra.  
**Currently the plugin only work with [official eosio repository](https://github.com/EOSIO/eos).**  
## Installation
1. Get `cassandra_history_plugin` source code into nodeos sources directory.  
```sh
$ git clone https://github.com/atticlab/cassandra_history_plugin.git plugins/elasticsearch_plugin  
```  
2. Add subdirectory to `plugins/CMakeLists.txt`.

```cmake
...
add_subdirectory(mongo_db_plugin)
add_subdirectory(login_plugin)
add_subdirectory(cassandra_history_plugin) # add this line.
...
```
3. Add the following line to `programs/nodeos/CMakeLists.txt`.

```cmake
target_link_libraries( ${NODE_EXECUTABLE_NAME}
        PRIVATE appbase
        PRIVATE -Wl,${whole_archive_flag} login_plugin               -Wl,${no_whole_archive_flag}
        PRIVATE -Wl,${whole_archive_flag} history_plugin             -Wl,${no_whole_archive_flag}
        ...
        # add this line.
        PRIVATE -Wl,${whole_archive_flag} cassandra_history_plugin   -Wl,${no_whole_archive_flag}
        ...
```
4. Install cassandra-cpp-driver and dependencies for ubuntu 16.04 .  
```sh
$ wget http://security.ubuntu.com/ubuntu/pool/main/o/openssl/libssl1.1_1.1.0g-2ubuntu4.3_amd64.deb
$ dpkg -i libssl1.1_1.1.0g-2ubuntu4.3_amd64.deb
$ aptitude install libuv1
$ wget http://downloads.datastax.com/cpp-driver/ubuntu/18.04/cassandra/v2.11.0/cassandra-cpp-driver_2.11.0-1_amd64.deb
$ sudo dpkg -i cassandra-cpp-driver_2.11.0-1_amd64.deb
$ wget http://downloads.datastax.com/cpp-driver/ubuntu/18.04/cassandra/v2.11.0/cassandra-cpp-driver-dev_2.11.0-1_amd64.deb
$ sudo dpkg -i cassandra-cpp-driver-dev_2.11.0-1_amd64.deb
```  
5. Build nodeos.   
```sh
$ ./eosio_build.sh -s EOS
```
## Configure nodeos (add the following to config.ini). 
```plain
....
abi-serializer-max-time-ms = 1000000
.....
plugin = eosio::cassandra_history_plugin
read-mode = read-only
cassandra-url = 1.1.1.1
cassandra-keyspace = eos_history
cassandra-queue-size = 2048
......
# do not store block data
cassandra-filter-out = eosio:onblock:
cassandra-filter-out = gu2tembqgage::
cassandra-filter-out = blocktwitter::
cassandra-filter-out = 1hello1world::
cassandra-filter-out = betdicealert::
cassandra-filter-out = myeosgateway::
cassandra-filter-out = eosonthefly1::
cassandra-filter-out = cryptohongbo::
cassandra-filter-out = experimentms::
cassandra-filter-out = eosplayaloud::
cassandra-filter-out = message.bank::
cassandra-filter-out = eospromoter1::
cassandra-filter-out = eospromotera::
cassandra-filter-out = watchdoggiee::
cassandra-filter-out = eoseosaddddd::
cassandra-filter-out = eosblackdrop::
......
```  

## Create keyspace in cassandra.
1. Create file cmd.cqlsh.  
```plain
USE eos_history;
CREATE TABLE action_trace ( global_seq varint, block_date date, block_time timestamp, parent varint, doc text, PRIMARY KEY(block_date, block_time, global_seq));
CREATE INDEX ON eos_history.action_trace (global_seq);

CREATE TABLE account_action_trace_shard ( account_name text, shard_id timestamp, PRIMARY KEY(account_name, shard_id));
CREATE TABLE account_action_trace (shard_id timestamp, account_name text, global_seq varint, block_time timestamp, parent varint,
    PRIMARY KEY((account_name, shard_id), block_time, global_seq));

CREATE TABLE transaction (id text, doc text, PRIMARY KEY(id));
CREATE TABLE transaction_trace (id text, block_num varint, block_date date, doc text, PRIMARY KEY(id));

CREATE TABLE block (id text, block_num varint, irreversible boolean, doc text, PRIMARY KEY(id));
CREATE INDEX ON eos_history.block (block_num);

CREATE TABLE lib (part_key int, block_num varint, PRIMARY KEY(part_key));

CREATE TABLE account (name text, creator text, account_create_time timestamp, abi text, PRIMARY KEY(name));
CREATE TABLE account_public_key (name text, permission text, key text, PRIMARY KEY(name, permission));
CREATE INDEX ON eos_history.account_public_key (key);
CREATE TABLE account_controlling_account (name text, controlling_name text, permission text, PRIMARY KEY(name, permission));
CREATE INDEX ON eos_history.account_controlling_account (controlling_name);
```  
2. Create keyspace and tables from the file in cqlsh.  
```sh
$ cqlsh
cqlsh> CREATE KEYSPACE eos_history WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } ;   
cqlsh> SOURCE 'cmd.cqlsh'
```  

## Start nodeos

1. For the first time. 
```sh
$ /nodeos_bin_dir/nodeos --delete-all-blocks --genesis-json genesis.json
```  
2. After receiving about 5000 blocks restart nodeos  




