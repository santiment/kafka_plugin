/**
 *  @file
 *  @copyright defined in eos/LICENSE.txt
 */
//
#include <stdlib.h>
#include <eosio/kafka_plugin/kafka_producer.hpp>
#include <eosio/kafka_plugin/kafka_plugin.hpp>

#include <eosio/chain/eosio_contract.hpp>
#include <eosio/chain/config.hpp>
#include <eosio/chain/exceptions.hpp>
#include <eosio/chain/transaction.hpp>
#include <eosio/chain/types.hpp>

#include <fc/io/json.hpp>
#include <fc/utf8.hpp>
#include <fc/variant.hpp>

#include <chrono>
#include <boost/signals2/connection.hpp>
#include <thread>
#include <mutex>
#include <boost/thread/condition_variable.hpp>

#include <prometheus/exposer.h>
#include <prometheus/registry.h>

#include <queue>

namespace fc { class variant; }

namespace eosio {

    using chain::account_name;
    using chain::action_name;
    using chain::block_id_type;
    using chain::permission_name;
    using chain::transaction;
    using chain::signed_transaction;
    using chain::signed_block;
    using chain::transaction_id_type;
    using chain::packed_transaction;

static appbase::abstract_plugin& _kafka_plugin = app().register_plugin<kafka_plugin>();
using kafka_producer_ptr = std::shared_ptr<class kafka_producer>;

    class PrometheusExposer {
    private:
        prometheus::Exposer exposer;
        std::shared_ptr<prometheus::Registry> registry;
        prometheus::Family<prometheus::Gauge>& gaugeFamily;
        prometheus::Gauge& blockGauge;
        prometheus::Gauge& abnormalityBlockGauge;
        prometheus::Gauge& blockWithPreviousTimestampGauge;
        prometheus::Gauge& blockWithPreviousActionIDGauge;
    public:
        PrometheusExposer(const std::string& hostPort)
            :
            exposer({hostPort}),
            registry(std::make_shared<prometheus::Registry>()),
            gaugeFamily(prometheus::BuildGauge()
                                       .Name("block_number_reached")
                                       .Help("The last block number being exported by the Kafka plugin")
                                       .Register(*registry)),
            blockGauge(gaugeFamily.Add(
                {{"name", "blockCounter"}})),
            abnormalityBlockGauge(gaugeFamily.Add(
                {{"name", "abnormalityBlockCounter"}})),
            blockWithPreviousTimestampGauge(gaugeFamily.Add(
                {{"name", "blockWithPreviousTimestamp"}})),
            blockWithPreviousActionIDGauge(gaugeFamily.Add(
              {{"name", "blockWithPreviousActionID"}}))
        {
            exposer.RegisterCollectable(registry);
        }

        prometheus::Gauge& getBlockGauge() {
            return blockGauge;
        }
        prometheus::Gauge& getAbnormalityBlockGauge() {
            return abnormalityBlockGauge;
        }
        prometheus::Gauge& getBlockWithPreviousTimestampGauge() {
            return blockWithPreviousTimestampGauge;
        }
        prometheus::Gauge& getBlockWithPreviousActionIDGauge() {
            return blockWithPreviousActionIDGauge;
        }
    };

    class kafka_plugin_impl {
    public:
        kafka_plugin_impl();

        ~kafka_plugin_impl();

        fc::optional<boost::signals2::scoped_connection> accepted_block_connection;
        fc::optional<boost::signals2::scoped_connection> irreversible_block_connection;
        fc::optional<boost::signals2::scoped_connection> applied_transaction_connection;
        chain_plugin *chain_plug;
        struct trasaction_info_st {
            uint64_t block_number;
            fc::time_point block_time;
            chain::transaction_trace_ptr trace;
            fc::variant tracesVar;
        };

        void consume_blocks();

        void applied_transaction(const chain::transaction_trace_ptr &);

        void process_applied_transaction(const trasaction_info_st &);

        void _process_applied_transaction(const trasaction_info_st &);

        void init(const variables_map &options);

        static void kafkaCallbackFunction(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque);
        static void handle_kafka_exception();

        bool configured{false};

        uint32_t start_block_num = 0;
        bool start_block_reached = false;

        size_t queue_size = 10000;
        std::deque<trasaction_info_st> transaction_trace_queue;
        std::deque<trasaction_info_st> transaction_trace_process_queue;

        std::mutex mtx;
        std::condition_variable condition;
        std::thread consume_thread;
        std::atomic<bool> startup{true};

        kafka_producer_ptr producer;
        const uint64_t KAFKA_BLOCK_REACHED_LOG_INTERVAL = 1000;
        static bool kafkaTriggeredQuit;
        std::shared_ptr<PrometheusExposer> prometheusExposer;
        uint64_t blockTimestampReached = 0;
        uint64_t actionIDReached = 0;
    };

    bool kafka_plugin_impl::kafkaTriggeredQuit = false;

    namespace {

        template<typename Queue, typename Entry>
        void queue(std::mutex &mtx, std::condition_variable &condition, Queue &queue, const Entry &e,
                   size_t queue_size) {
            int sleep_time = 100;
            size_t last_queue_size = 0;
            std::unique_lock lock(mtx);
            if (queue.size() > queue_size) {
                lock.unlock();
                condition.notify_one();
                if (last_queue_size < queue.size()) {
                    sleep_time += 100;
                } else {
                    sleep_time -= 100;
                    if (sleep_time < 0) sleep_time = 100;
                }
                last_queue_size = queue.size();
                std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
                lock.lock();
            }
            queue.emplace_back(e);
            lock.unlock();
            condition.notify_one();
        }

    }


    void filterSetcodeData(vector<chain::action_trace>& vecActions) {
        for(auto& actTrace : vecActions) {
            if("setcode" == actTrace.act.name.to_string() &&
                "eosio" == actTrace.act.account.to_string()) {
                chain::setcode sc = actTrace.act.data_as<chain::setcode>();
                sc.code.clear();
                actTrace.act.data = fc::raw::pack(sc);
                dlog("'setcode' action is cleared of code data. Block number is: ${block_number}",
                     ("block_number", actTrace.block_num));
            }
        }
    }

    void kafka_plugin_impl::applied_transaction(const chain::transaction_trace_ptr &t) {
        try {
            filterSetcodeData(t->action_traces);

            trasaction_info_st transactioninfo = trasaction_info_st{
                    .block_number = t->block_num,
                    .block_time = t->block_time,
                    .trace =chain::transaction_trace_ptr(t),
                    .tracesVar = chain_plug->chain().to_variant_with_abi(*t, chain_plug->get_abi_serializer_max_time())
            };
            trasaction_info_st &info_t = transactioninfo;

            queue(mtx, condition, transaction_trace_queue, info_t, queue_size);
        } catch (fc::exception &e) {
            elog("FC Exception while applied_transaction ${e}", ("e", e.to_string()));
        } catch (std::exception &e) {
            elog("STD Exception while applied_transaction ${e}", ("e", e.what()));
        } catch (...) {
            elog("Unknown exception while applied_transaction");
        }
    }

    void kafka_plugin_impl::consume_blocks() {
        try {

            size_t transaction_trace_size = 1;
            while (!kafkaTriggeredQuit ||
                   transaction_trace_size > 0) {
                std::unique_lock lock(mtx);
                while (transaction_trace_queue.empty() &&
                       !kafkaTriggeredQuit) {
                    condition.wait(lock);
                }
                // capture for processing
                transaction_trace_size = transaction_trace_queue.size();
                if (transaction_trace_size > 0) {
                    transaction_trace_process_queue = move(transaction_trace_queue);
                    transaction_trace_queue.clear();
                }

                lock.unlock();

                while (!transaction_trace_process_queue.empty()) {
                    const auto &t = transaction_trace_process_queue.front();
                    process_applied_transaction(t);
                    transaction_trace_process_queue.pop_front();
                }
            }
            ilog("kafka_plugin consume thread shutdown gracefully");
        } catch (fc::exception &e) {
            elog("FC Exception while consuming block ${e}", ("e", e.to_string()));
        } catch (std::exception &e) {
            elog("STD Exception while consuming block ${e}", ("e", e.what()));
        } catch (...) {
            elog("Unknown exception while consuming block");
        }
    }

    void kafka_plugin_impl::process_applied_transaction(const trasaction_info_st &t) {
        try {
            if (!start_block_reached) {
               if (t.block_number >= start_block_num) {
                    start_block_reached = true;
                }
            }
            else {
                _process_applied_transaction(t);
            }
        } catch (fc::exception &e) {
            elog("FC Exception while processing applied transaction trace: ${e}", ("e", e.to_detail_string()));
            handle_kafka_exception();
        } catch (std::exception &e) {
            elog("STD Exception while processing applied transaction trace: ${e}", ("e", e.what()));
            handle_kafka_exception();
        } catch (...) {
            elog("Unknown exception while processing applied transaction trace");
            handle_kafka_exception();
        }
    }

    // This will return the largest sequence id of this batch of actions.
    uint64_t getLargestActionID(const vector<chain::action_trace>& vecActions) {
        uint64_t largestActionID = 0;
        for( const auto actionTrace : vecActions) {
            if(largestActionID < actionTrace.receipt->global_sequence) {
                largestActionID = actionTrace.receipt->global_sequence;
            }
        }
        return largestActionID;
    }

     void kafka_plugin_impl::_process_applied_transaction(const trasaction_info_st &t) {
       if(t.trace->action_traces.empty()) {
           dlog("Apply transaction with id: ${id} is skipped. No actions inside. Block number is: ${block_number}",
                ("id", t.trace->id.str())
                ("block_number", t.block_number));
           return;
       }
       if (t.trace->receipt->status != chain::transaction_receipt_header::executed) {
           // Failed transactions are also reported. Ignore those.
           return;
       }
       if( t.block_number < prometheusExposer->getBlockGauge().Value()) { // Late applied action for irreversible block.
           prometheusExposer->getAbnormalityBlockGauge().Set(prometheusExposer->getBlockGauge().Value());
           return;
       }
       else {
           prometheusExposer->getBlockGauge().Set(t.block_number);
       }

        uint64_t blockTimeEpochMilliSeconds = t.block_time.time_since_epoch().count() / 1000;
        // Correct the block timestamp if it is in the past
        if(blockTimeEpochMilliSeconds < blockTimestampReached) {
           blockTimeEpochMilliSeconds = blockTimestampReached;
           prometheusExposer->getBlockWithPreviousTimestampGauge().Set(t.block_number);
        }
        else {
           blockTimestampReached = blockTimeEpochMilliSeconds;
        }

       auto& actionTraces = t.trace->action_traces;

       uint64_t actionID = getLargestActionID(actionTraces);

        if(actionID <= actionIDReached) {
            prometheusExposer->getBlockWithPreviousActionIDGauge().Set(t.block_number);
        }
        else {
            actionIDReached = actionID;
        }

       std::stringstream sstream;
       sstream << actionID;

       // Store the block time at an upper layer. This allows us to easily correct if it varies for actions inside.
       string transaction_metadata_json =
                    "{\"block_number\":" + std::to_string(t.block_number) + ",\"block_time\":" + std::to_string(blockTimeEpochMilliSeconds) +
                    ",\"trace\":" + fc::json::to_string(t.tracesVar).c_str() + "}";

       producer->trx_kafka_sendmsg(KAFKA_TRX_APPLIED,
                                   (char*)transaction_metadata_json.c_str(),
                                   sstream.str());
    }

    kafka_plugin_impl::kafka_plugin_impl()
    :producer(new kafka_producer)
    {
    }

    kafka_plugin_impl::~kafka_plugin_impl() {
       if (!startup) {
          try {
             ilog( "kafka_db_plugin shutdown in process please be patient this can take a few minutes" );
             condition.notify_one();

             consume_thread.join();
             producer->trx_kafka_destroy();
          } catch( std::exception& e ) {
             elog( "Exception on kafka_plugin shutdown of consume thread: ${e}", ("e", e.what()));
          }
       }
    }

    void kafka_plugin_impl::init(const variables_map &options) {
        std::string prometheustHostPort = "0.0.0.0:8080";

        if(options.count("prometheus-uri")) {
            prometheustHostPort = options.at("prometheus-uri").as<std::string>();
        }
        ilog("Starting Prometheus exposer at port: " + prometheustHostPort);
        prometheusExposer.reset(new PrometheusExposer(prometheustHostPort));

        ilog("Starting kafka plugin thread");
        consume_thread = std::thread([this] { consume_blocks(); });
        startup = false;
    }

    void kafka_plugin_impl::handle_kafka_exception() {
        // Trigger quit once only
        if(kafkaTriggeredQuit) {
            return;
        }
        // For the time being quit on all
        elog( "Kafka plugin triggers Quit due to error.");
        app().quit();
        kafkaTriggeredQuit = true;
    }

    void kafka_plugin_impl::kafkaCallbackFunction(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) {
        if(nullptr == rkmessage ||
           0 != rkmessage->err )
        {
            elog("Kafka message delivery failed: ${e}", ("e", rd_kafka_err2str(rkmessage->err)));
            handle_kafka_exception();
        }
    }

////////////
// kafka_plugin
////////////

    kafka_plugin::kafka_plugin()
            : my(new kafka_plugin_impl()) {
    }

    kafka_plugin::~kafka_plugin() {
        ilog("kafka_plugin::~kafka_plugin()");
        plugin_shutdown();
    }

    void kafka_plugin::set_program_options(options_description &cli, options_description &cfg) {
        cfg.add_options()
                ("accept_trx_topic", bpo::value<std::string>(),
                 "The topic for accepted transaction.")
                ("applied_trx_topic", bpo::value<std::string>(),
                 "The topic for appiled transaction.")
                ("kafka-uri,k", bpo::value<std::string>(),
                 "the kafka brokers uri, as 192.168.31.225:9092")
                ("kafka-queue-size", bpo::value<uint32_t>()->default_value(256),
                 "The target queue size between nodeos and kafka plugin thread.")
                ("kafka-block-start", bpo::value<uint32_t>()->default_value(256),
                 "If specified then only abi data pushed to kafka until specified block is reached.")
                ("prometheus-uri", bpo::value<std::string>(),
                 "Host:port on which to open Prometheus Exposer.")
                 ;
    }

    void kafka_plugin::plugin_initialize(const variables_map &options) {
        char *accept_trx_topic = NULL;
        char *applied_trx_topic = NULL;
        char *brokers_str = NULL;

        try {
            if (options.count("kafka-uri")) {
                brokers_str = (char *) (options.at("kafka-uri").as<std::string>().c_str());
                if (options.count("accept_trx_topic") != 0) {
                    accept_trx_topic = (char *) (options.at("accept_trx_topic").as<std::string>().c_str());
                }
                if (options.count("applied_trx_topic") != 0) {
                    applied_trx_topic = (char *) (options.at("applied_trx_topic").as<std::string>().c_str());
                }
                ilog("brokers_str:${j}", ("j", brokers_str));
                if(accept_trx_topic) {
                    ilog("accept_trx_topic:${j}", ("j", accept_trx_topic));
                }

                ilog("applied_trx_topic:${j}", ("j", applied_trx_topic));

                if (0 != my->producer->trx_kafka_init(brokers_str, accept_trx_topic, applied_trx_topic, my->kafkaCallbackFunction)) {
                    elog("trx_kafka_init fail");
                    my->handle_kafka_exception();
                } else{
                    ilog("trx_kafka_init ok");
                }
            }

            if (options.count("kafka-uri")) {
                ilog("initializing kafka_plugin");
                my->configured = true;

                if( options.count( "kafka-queue-size" )) {
                    my->queue_size = options.at( "kafka-queue-size" ).as<uint32_t>();
                }
                if( options.count( "kafka-block-start" )) {
                    my->start_block_num = options.at( "kafka-block-start" ).as<uint32_t>();
                }
                if( my->start_block_num == 0 ) {
                    my->start_block_reached = true;
                }

                // hook up to signals on controller
                //chain_plugin* chain_plug = app().find_plugiin<chain_plugin>();
                my->chain_plug = app().find_plugin<chain_plugin>();
                EOS_ASSERT(my->chain_plug, chain::missing_chain_plugin_exception, "");
                auto &chain = my->chain_plug->chain();

                my->applied_transaction_connection.emplace(
                        chain.applied_transaction.connect([&](std::tuple<const chain::transaction_trace_ptr&, const signed_transaction&> tupleTrx) {
                            my->applied_transaction(std::get<0>(tupleTrx));
                        }));
                my->init(options);
            } else {
                wlog( "eosio::kafka_plugin configured, but no --kafka-uri specified." );
                wlog( "kafka_plugin disabled." );
            }

        }

        FC_LOG_AND_RETHROW()
    }

    void kafka_plugin::plugin_startup() {
        ilog("kafka_plugin::plugin_startup()");
    }

    void kafka_plugin::plugin_shutdown() {
        if(my) {
            ilog("kafka_plugin::plugin_shutdown()");
            my->kafkaTriggeredQuit = true;
            my->applied_transaction_connection.reset();
            my.reset();
        }
    }

} // namespace eosio


