#pragma once
#include "marketState.h"
#include <bsl_memory.h>
#include <bsl_vector.h>
#include <bslstl_sharedptr.h>
#include <bslstl_string.h>
#include <kafka/KafkaProducer.h>
#include <rmqa_consumer.h>
#include <rmqa_producer.h>
#include <rmqa_topology.h>
#include <rmqa_vhost.h>
#include <rmqp_consumer.h>
#include <rmqp_producer.h>
#include <rmqt_confirmresponse.h>
#include <rmqt_consumerconfig.h>
#include <rmqt_exchange.h>
#include <rmqt_exchangetype.h>
#include <rmqt_fieldvalue.h>
#include <rmqt_message.h>
#include <rmqt_properties.h>
#include <rmqt_result.h>
#include <string_view>

extern const bsl::string ExchangeName;

struct ListenStuff {
  BloombergLP::rmqa::VHost &vhost;
  BloombergLP::rmqt::ExchangeHandle &exch;
  BloombergLP::rmqa::Topology &topology;
};

struct OrderListenhandle {
  BloombergLP::rmqt::QueueHandle responseQueue;
  bsl::shared_ptr<BloombergLP::rmqa::Consumer> responseConsumer;
  BloombergLP::rmqt::QueueHandle fillQueue;
  bsl::shared_ptr<BloombergLP::rmqa::Consumer> fillConsumer;
};

static constexpr int64_t failureId = 0;

int64_t registerWithMarket(ListenStuff, BloombergLP::rmqa::Producer &,
                           std::string_view,
                           kafka::clients::producer::KafkaProducer &);

OrderListenhandle startListeningToOrderResponses(ListenStuff,
                                                 const bsl::string &,
                                                 MarketState &);

bool sendOrderToMarket(BloombergLP::rmqa::Producer &,
                       const bsl::string &,
                       AtomicCounter &, MarketOrder, MarketState &);
