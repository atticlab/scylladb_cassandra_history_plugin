#pragma once

#include <chainbase/chainbase.hpp>
#include <fc/array.hpp>


namespace eosio
{
    using chain::account_name;
    using namespace boost::multi_index;

    class account_action_trace_shard_object : public chainbase::object<0, account_action_trace_shard_object> {
        OBJECT_CTOR(account_action_trace_shard_object)

        id_type      id;
        account_name account;
        int64_t      timestamp; //current shard_id
        uint32_t     counter;
    };

    struct by_id;
    struct by_account;
    using account_action_trace_shard_multi_index = chainbase::shared_multi_index_container<
        account_action_trace_shard_object,
        indexed_by<
            ordered_unique<tag<by_id>, BOOST_MULTI_INDEX_MEMBER(account_action_trace_shard_object, account_action_trace_shard_object::id_type, id)>,
            ordered_unique<tag<by_account>, BOOST_MULTI_INDEX_MEMBER(account_action_trace_shard_object, account_name, account)>
        >
    >;
    typedef chainbase::generic_index<account_action_trace_shard_multi_index> account_action_trace_shard_index;
}

CHAINBASE_SET_INDEX_TYPE( eosio::account_action_trace_shard_object, eosio::account_action_trace_shard_multi_index )

FC_REFLECT( eosio::account_action_trace_shard_object, (account)(timestamp)(counter) )