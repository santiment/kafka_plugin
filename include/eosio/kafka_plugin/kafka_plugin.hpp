/**
 *
 *
 */
#pragma once

#include <eosio/chain_plugin/chain_plugin.hpp>
#include <appbase/application.hpp>
#include <memory>

namespace eosio {

using kafka_plugin_impl_ptr = std::shared_ptr<class kafka_plugin_impl>;

/**
 * Provides persistence to Apache Kafka for:
 * actions
 * transaction_traces
 * transactions
 */
class kafka_plugin : public plugin<kafka_plugin> {
public:
   APPBASE_PLUGIN_REQUIRES((chain_plugin))

   kafka_plugin();
   virtual ~kafka_plugin();

   virtual void set_program_options(options_description& cli, options_description& cfg) override;

   void plugin_initialize(const variables_map& options);
   void plugin_startup();
   void plugin_shutdown();

private:
   kafka_plugin_impl_ptr my;
};

}

