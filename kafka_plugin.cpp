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
        prometheus::Gauge& pendingBlocksGauge;
        prometheus::Gauge& oldestPendingBlockGauge;
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
            pendingBlocksGauge(gaugeFamily.Add(
                {{"name", "pendingBlocksCounter"}})),
            oldestPendingBlockGauge(gaugeFamily.Add(
                {{"name", "oldestPendingBlock"}})),
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
        prometheus::Gauge& getPendingBlocksGauge() {
            return pendingBlocksGauge;
        }
        prometheus::Gauge& getOldestPendingBlockGauge() {
            return oldestPendingBlockGauge;
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
        };

        void consume_blocks();

        void accepted_block(const chain::block_state_ptr &);

        void applied_irreversible_block(const chain::block_state_ptr &);

        void applied_transaction(const chain::transaction_trace_ptr &);

        void process_applied_transaction(const trasaction_info_st &);

        void _process_applied_transaction(const trasaction_info_st &);

        void process_accepted_block(const chain::block_state_ptr &);

        void _process_accepted_block(const chain::block_state_ptr &);

        void process_irreversible_block(const chain::block_state_ptr &);

        void _process_irreversible_block(const chain::block_state_ptr &);

        void init(const variables_map &options);

        void _process_applied_block(std::map<transaction_id_type, trasaction_info_st>& trxsInThisBlock,
                                    const vector<chain::transaction_receipt>& trxReceipts,
                                    uint64_t blockNumber, uint64_t blockTime);

        static void kafkaCallbackFunction(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque);
        static void handle_kafka_exception();

        bool configured{false};

        uint32_t start_block_num = 0;
        bool start_block_reached = false;

        size_t queue_size = 10000;
        std::deque<chain::transaction_metadata_ptr> transaction_metadata_queue;
        std::deque<chain::transaction_metadata_ptr> transaction_metadata_process_queue;
        std::deque<trasaction_info_st> transaction_trace_queue;
        std::deque<trasaction_info_st> transaction_trace_process_queue;
        std::deque<chain::block_state_ptr> block_state_queue;
        std::deque<chain::block_state_ptr> block_state_process_queue;
        std::deque<chain::block_state_ptr> irreversible_block_state_queue;
        std::deque<chain::block_state_ptr> irreversible_block_state_process_queue;
        std::mutex mtx;
        std::condition_variable condition;
        std::thread consume_thread;
        std::atomic<bool> done{false};
        std::atomic<bool> startup{true};

        kafka_producer_ptr producer;
        const uint64_t KAFKA_BLOCK_REACHED_LOG_INTERVAL = 1000;
        static bool kafkaTriggeredQuit;
        std::shared_ptr<PrometheusExposer> prometheusExposer;
        std::map<uint64_t, std::map<transaction_id_type, trasaction_info_st>> appliedTrxPerBlock;
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

    void kafka_plugin_impl::applied_transaction(const chain::transaction_trace_ptr &t) {
        try {
            trasaction_info_st transactioninfo = trasaction_info_st{
                    .block_number = t->block_num,
                    .block_time = t->block_time,
                    .trace =chain::transaction_trace_ptr(t)
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

    void kafka_plugin_impl::applied_irreversible_block(const chain::block_state_ptr &bs) {
        try {
            queue(mtx, condition, irreversible_block_state_queue, bs, queue_size);
        } catch (fc::exception &e) {
            elog("FC Exception while applied_irreversible_block ${e}", ("e", e.to_string()));
        } catch (std::exception &e) {
            elog("STD Exception while applied_irreversible_block ${e}", ("e", e.what()));
        } catch (...) {
            elog("Unknown exception while applied_irreversible_block");
        }
    }


    void kafka_plugin_impl::accepted_block(const chain::block_state_ptr &bs) {
        try {
            queue(mtx, condition, block_state_queue, bs, queue_size);
        } catch (fc::exception &e) {
            elog("FC Exception while accepted_block ${e}", ("e", e.to_string()));
        } catch (std::exception &e) {
            elog("STD Exception while accepted_block ${e}", ("e", e.what()));
        } catch (...) {
            elog("Unknown exception while accepted_block");
        }
    }

    void kafka_plugin_impl::consume_blocks() {
        try {

            while (true && !app().is_quiting()) {
                std::unique_lock lock(mtx);
                while (transaction_metadata_queue.empty() &&
                       transaction_trace_queue.empty() &&
                       block_state_queue.empty() &&
                       irreversible_block_state_queue.empty() &&
                       !done) {
                    condition.wait(lock);
                }
                // capture for processing
                size_t transaction_metadata_size = transaction_metadata_queue.size();
                if (transaction_metadata_size > 0) {
                    transaction_metadata_process_queue = move(transaction_metadata_queue);
                    transaction_metadata_queue.clear();
                }
                size_t transaction_trace_size = transaction_trace_queue.size();
                if (transaction_trace_size > 0) {
                    transaction_trace_process_queue = move(transaction_trace_queue);
                    transaction_trace_queue.clear();
                }

                size_t block_state_size = block_state_queue.size();
                if (block_state_size > 0) {
                    block_state_process_queue = move(block_state_queue);
                    block_state_queue.clear();
                }
                size_t irreversible_block_size = irreversible_block_state_queue.size();
                if (irreversible_block_size > 0) {
                    irreversible_block_state_process_queue = move(irreversible_block_state_queue);
                    irreversible_block_state_queue.clear();
                }

                lock.unlock();

                // warn if queue size greater than 75%
                if (transaction_metadata_size > (queue_size * 0.75) ||
                    transaction_trace_size > (queue_size * 0.75) ||
                    block_state_size > (queue_size * 0.75) ||
                    irreversible_block_size > (queue_size * 0.75)) {
//            wlog("queue size: ${q}", ("q", transaction_metadata_size + transaction_trace_size ));
                } else if (done) {
                    ilog("draining queue, size: ${q}", ("q", transaction_metadata_size + transaction_trace_size));
                }

                while (!transaction_trace_process_queue.empty()) {
                    const auto &t = transaction_trace_process_queue.front();
                    process_applied_transaction(t);
                    transaction_trace_process_queue.pop_front();
                }

                // process blocks
                while (!block_state_process_queue.empty()) {
                    const auto &bs = block_state_process_queue.front();
                    process_accepted_block(bs);
                    block_state_process_queue.pop_front();
                }

                // process irreversible blocks
                while (!irreversible_block_state_process_queue.empty()) {
                    const auto &bs = irreversible_block_state_process_queue.front();
                    process_irreversible_block(bs);
                    irreversible_block_state_process_queue.pop_front();
                }

                if (transaction_metadata_size == 0 &&
                    transaction_trace_size == 0 &&
                    block_state_size == 0 &&
                    irreversible_block_size == 0 &&
                    done) {
                    break;
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
            if (start_block_reached) {
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


    void kafka_plugin_impl::process_irreversible_block(const chain::block_state_ptr &bs) {
        try {
            if (start_block_reached) {
                _process_irreversible_block(bs);
            }
        } catch (fc::exception &e) {
            elog("FC Exception while processing irreversible block: ${e}", ("e", e.to_detail_string()));
            handle_kafka_exception();
        } catch (std::exception &e) {
            elog("STD Exception while processing irreversible block: ${e}", ("e", e.what()));
            handle_kafka_exception();
        } catch (...) {
            elog("Unknown exception while processing irreversible block");
            handle_kafka_exception();
        }
    }

    void kafka_plugin_impl::process_accepted_block(const chain::block_state_ptr &bs) {
        try {
            if (!start_block_reached) {
                if (bs->block_num >= start_block_num) {
                    start_block_reached = true;
                }
            }
            if (start_block_reached) {
                _process_accepted_block(bs);
            }
        } catch (fc::exception &e) {
            elog("FC Exception while processing accepted block trace ${e}", ("e", e.to_string()));
            handle_kafka_exception();
        } catch (std::exception &e) {
            elog("STD Exception while processing accepted block trace ${e}", ("e", e.what()));
            handle_kafka_exception();
        } catch (...) {
            elog("Unknown exception while processing accepted block trace");
            handle_kafka_exception();
        }
    }

    // This will return the sequence id of the last action. Due to inner actions we need to recurse inside.
/*    uint64_t getLargestActionID(const vector<chain::action_trace>& vecActions) {
        int lastIndex = vecActions.size()-1;
        if( vecActions[lastIndex].inline_traces.empty()) {
            return vecActions[lastIndex].receipt.global_sequence;
        } else {
            return getLastActionID(vecActions[lastIndex].inline_traces);
        }
    } */

    uint64_t getLargestActionID(const vector<chain::action_trace>& vecActions) {
        uint64_t largestActionID = 0;
        for( const auto actionTrace : vecActions) {
            if(largestActionID < actionTrace.receipt->global_sequence) {
                largestActionID = actionTrace.receipt->global_sequence;
            }
        }
        return largestActionID;
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

     void kafka_plugin_impl::_process_applied_transaction(const trasaction_info_st &t) {
       if(t.trace->action_traces.empty()) {
           dlog("Apply transaction with id: ${id} is skipped. No actions inside. Block number is: ${block_number}",
                ("id", t.trace->id.str())
                ("block_number", t.block_number));
           return;
       }

       if( t.block_number <= prometheusExposer->getBlockGauge().Value()) { // Late applied action for irreversible block.
           prometheusExposer->getAbnormalityBlockGauge().Set(prometheusExposer->getBlockGauge().Value());
           return;
       }

       // Only register the transaction. Process it once the block is irreversible.
       appliedTrxPerBlock[t.block_number].insert(std::make_pair(t.trace->id, t));
     }

     void kafka_plugin_impl::_process_applied_block(std::map<transaction_id_type, trasaction_info_st>& trxsInThisBlock,
                                                    const vector<chain::transaction_receipt>& trxReceipts,
                                                    uint64_t blockNumber,
                                                    uint64_t blockTimeEpochMilliSeconds)
     {

         // Correct the block timestamp if it is in the past
         if(blockTimeEpochMilliSeconds < blockTimestampReached) {
             blockTimeEpochMilliSeconds = blockTimestampReached;
              prometheusExposer->getBlockWithPreviousTimestampGauge().Set(blockNumber);
         }
         else {
             blockTimestampReached = blockTimeEpochMilliSeconds;
         }

         // As the received transaction receipts are not ordered by global sequence, we need to re-order before sending.
         std::map<uint64_t, chain::transaction_trace_ptr> orderedActions;

       // Iterate over all the receipts received in this block. Get the complete actions out of the previously
       // stored map for this block.
       for(const chain::transaction_receipt& receipt : trxReceipts) {
           transaction_id_type trxId;

           if(receipt.status != chain::transaction_receipt_header::executed) {
               continue;
           }
           if( receipt.trx.contains<packed_transaction>() ) {
              const auto& pt = receipt.trx.get<packed_transaction>();
              // get id via get_raw_transaction() as packed_transaction.id() mutates internal transaction state
              const auto& raw = pt.get_raw_transaction();
              const auto& trx = fc::raw::unpack<transaction>( raw );
              trxId = trx.id();
           } else {
              trxId = receipt.trx.get<transaction_id_type>();
           }

           std::map<transaction_id_type, trasaction_info_st>::iterator iterTrx = trxsInThisBlock.find(trxId);
           if(iterTrx == trxsInThisBlock.end()) {
               elog( "Can not find stored transaction with id: ${e}, status is ${status}, block id is: ${block_id}",
                     ("e", trxId.str())
                     ("status", std::string(receipt.status))
                     ("block_id", blockNumber));
               continue;
           }

           auto& transactionTrace = iterTrx->second.trace;
           auto& actionTraces = transactionTrace->action_traces;

           filterSetcodeData(actionTraces);
           uint64_t actionID = getLargestActionID(actionTraces);

           orderedActions.insert(std::make_pair(actionID, iterTrx->second.trace));
       }


        for(std::map<uint64_t, chain::transaction_trace_ptr>::iterator iter = orderedActions.begin();
            iter != orderedActions.end();
            ++iter)
        {
            uint64_t actionID = iter->first;
            chain::transaction_trace_ptr transactionTrace = iter->second;

            if(actionID <= actionIDReached) {
                prometheusExposer->getBlockWithPreviousActionIDGauge().Set(blockNumber);
            }
            else {
                actionIDReached = actionID;
            }

           std::stringstream sstream;
           sstream << actionID;

           // If the application is quitting it is unsafe to touch the chain_plugin. Return.
           if(app().is_quiting()) {
               return;
           }
           auto &chain = chain_plug->chain();
           fc::variant tracesVar = chain.to_variant_with_abi(*transactionTrace, chain_plug->get_abi_serializer_max_time());

           // Store the block time at an upper layer. This allows us to easily correct if it varies for actions inside.
           string transaction_metadata_json =
                        "{\"block_number\":" + std::to_string(blockNumber) + ",\"block_time\":" + std::to_string(blockTimeEpochMilliSeconds) +
                        ",\"trace\":" + fc::json::to_string(tracesVar).c_str() + "}";

           producer->trx_kafka_sendmsg(KAFKA_TRX_APPLIED,
                                       (char*)transaction_metadata_json.c_str(),
                                       sstream.str());
       }
    }

    void kafka_plugin_impl::_process_accepted_block( const chain::block_state_ptr& bs )
    {
    }

    void kafka_plugin_impl::_process_irreversible_block(const chain::block_state_ptr& bs)
    {
        // We get notified for each and every block. Wait for the notification for the exactly next block.
        if( bs->block_num == prometheusExposer->getBlockGauge().Value() + 1 ||
            0 == prometheusExposer->getBlockGauge().Value() )
        {
            prometheusExposer->getBlockGauge().Set(bs->block_num);
        }
        else
        {
            // Previous or future block
            // TODO Find out why we are notified more than once per block
            return;
        }

        // This block has become 'irreversible'. If any actions have been registered process them now.
        std::map<uint64_t, std::map<transaction_id_type, trasaction_info_st>>::iterator iter = appliedTrxPerBlock.find(bs->block_num);

        if(iter != appliedTrxPerBlock.end())
        {
            _process_applied_block(iter->second,
                                   bs->block->transactions,
                                   bs->block_num,
                                   bs->block->timestamp.to_time_point().time_since_epoch().count() / 1000);
            appliedTrxPerBlock.erase(iter);
        }

        // Update our monitoring system
        prometheusExposer->getPendingBlocksGauge().Set(appliedTrxPerBlock.size());
        prometheusExposer->getOldestPendingBlockGauge().Set(appliedTrxPerBlock.empty() ? 0 : appliedTrxPerBlock.begin()->first);
    }

    kafka_plugin_impl::kafka_plugin_impl()
    :producer(new kafka_producer)
    {
    }

    kafka_plugin_impl::~kafka_plugin_impl() {
       if (!startup) {
          try {
             ilog( "kafka_db_plugin shutdown in process please be patient this can take a few minutes" );
             done = true;
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
            : my(new kafka_plugin_impl) {
    }

    kafka_plugin::~kafka_plugin() {
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

                my->accepted_block_connection.emplace( chain.accepted_block.connect( [&]( const chain::block_state_ptr& bs ) {
                            my->accepted_block(bs);
                        }));

                my->irreversible_block_connection.emplace(
                        chain.irreversible_block.connect([&](const chain::block_state_ptr &bs) {
                            my->applied_irreversible_block(bs);
                        }));

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
    }

    void kafka_plugin::plugin_shutdown() {

        my->accepted_block_connection.reset();
        my->irreversible_block_connection.reset();
        my->applied_transaction_connection.reset();
        my.reset();

    }

} // namespace eosio


