file(GLOB HEADERS "include/eosio/cassandra_history_plugin/*.hpp")
add_library( cassandra_history_plugin
             cassandra_history_plugin.cpp
             cassandra_client.cpp
             ${HEADERS} )

target_link_libraries( cassandra_history_plugin appbase chain_plugin eosio_chain fc cassandra )
target_include_directories( cassandra_history_plugin PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include" )
