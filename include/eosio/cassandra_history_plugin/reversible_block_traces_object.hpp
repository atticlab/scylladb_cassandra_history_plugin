#pragma once

#include <chainbase/chainbase.hpp>


namespace eosio {
   using namespace boost::multi_index;

   class reversible_block_traces_object : public chainbase::object<0, reversible_block_traces_object> {
      OBJECT_CTOR(reversible_block_traces_object,(traces))

      id_type                  id;
      uint32_t           blockNum;
      chain::shared_blob   traces;
   };

   struct by_id;
   struct by_block_num;
   using reversible_block_traces_multi_index = chainbase::shared_multi_index_container<
      reversible_block_traces_object,
      indexed_by<
         ordered_unique<tag<by_id>, BOOST_MULTI_INDEX_MEMBER(reversible_block_traces_object, reversible_block_traces_object::id_type, id)>,
         ordered_unique<tag<by_block_num>, BOOST_MULTI_INDEX_MEMBER(reversible_block_traces_object, uint32_t, blockNum)>
      >
   >;
   typedef chainbase::generic_index<reversible_block_traces_multi_index> reversible_block_traces_index;
} //namespace eosio

CHAINBASE_SET_INDEX_TYPE( eosio::reversible_block_traces_object, eosio::reversible_block_traces_multi_index )

FC_REFLECT( eosio::reversible_block_traces_object, (blockNum)(traces) )