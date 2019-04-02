/**
 *  @file
 *  @copyright defined in eos/LICENSE.txt
 */
#pragma once
#include <appbase/application.hpp>
#include <eosio/chain_plugin/chain_plugin.hpp> 

namespace eosio {

using namespace appbase;

/**
 *  This is a template plugin, intended to serve as a starting point for making new plugins
 */
class cassandra_history_plugin : public appbase::plugin<cassandra_history_plugin> {
public:
   APPBASE_PLUGIN_REQUIRES((chain_plugin))
   cassandra_history_plugin();
   virtual ~cassandra_history_plugin();
 
   virtual void set_program_options(options_description&, options_description& cfg) override;
 
   void plugin_initialize(const variables_map& options);
   void plugin_startup();
   void plugin_shutdown();

private:
   std::unique_ptr<class cassandra_history_plugin_impl> my;
};

}
