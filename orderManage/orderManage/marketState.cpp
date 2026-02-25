#include "marketConnect.h"
#include <bsl_optional.h>
#include <bsl_vector.h>
#include <kafka/Types.h>
#include <sys/types.h>
#include <cassert>
#include <future>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <utility>
using namespace BloombergLP;

struct KafkaDeliveryCBSharedData {
  KafkaDeliveryCBSharedData(bsl::shared_ptr<bsl::vector<uint8_t>>&& data) : data_(std::move(data)) {}

  void operator()(const kafka::clients::producer::RecordMetadata &metadata,
                  const kafka::Error &error) {
    if (!error) {
      std::cout << "Message delivered: " << metadata.toString() << '\n';
    } else {
      std::cerr << "Message failed to be delivered: " << error.message()
                << '\n';
    }
  }
  bsl::shared_ptr<bsl::vector<uint8_t>> data_;
};

struct KafkaDeliveryCBSingleData {
  KafkaDeliveryCBSingleData(uint8_t* data) : data_(data) {}

  void operator()(const kafka::clients::producer::RecordMetadata &metadata,
                  const kafka::Error &error) {
    if (!error) {
      std::cout << "Message delivered: " << metadata.toString() << '\n';
    } else {
      std::cerr << "Message failed to be delivered: " << error.message()
                << '\n';
    }
    delete[] data_;
  }
  uint8_t* data_;
};

// Default constructor: wraps a real KafkaProducer in the send function.
MarketState::MarketState() {
  // Construct in-place via Properties ctor (KafkaProducer is not movable).
  kafka::Properties props(
      {{"bootstrap.servers", {"localhost:9092"}}, {"enable.idempotence", {"true"}}});
  d_kafkaProducer = std::make_shared<kafka::clients::producer::KafkaProducer>(props);
  auto producer = d_kafkaProducer;
  d_kafkaSend = [producer](const void *data, std::size_t len,
                            const kafka::Topic &topic) {
    uint8_t *rawData = new uint8_t[len];
    std::memcpy(rawData, data, len);
    producer->send(
        kafka::clients::producer::ProducerRecord(
            topic, kafka::NullKey, kafka::Value(rawData, len)),
        KafkaDeliveryCBSingleData(rawData));
  };
}

const bsl::string ExchangeName = "MarketExchange";

bool sendMessage(rmqa::Producer &producer, const rmqt::Message &message,
                 const bsl::string &routingKey) {
  auto confirmCallback = [](const rmqt::Message &message,
                            const bsl::string &routingKey,
                            const rmqt::ConfirmResponse &response) {
    if (response.status() == rmqt::ConfirmResponse::ACK) {
      // Message is now guaranteed to be safe with the broker.
    } else {
      std::cerr << "Message not confirmed: " << message.guid()
                << " for routing key " << routingKey << " " << response << "\n";
    }
  };
  const rmqp::Producer::SendStatus sendResult =
      producer.send(message, routingKey, confirmCallback);

  if (sendResult != rmqp::Producer::SENDING) {
    if (sendResult == rmqp::Producer::DUPLICATE) {
      std::cerr << "Failed to send message: " << message.guid()
                << " because an identical GUID is already outstanding\n";
    } else {
      std::cerr << "Unknown send failure for message: " << message.guid()
                << "\n";
    }
    return false;
  }
  return true;
}

template <class T>
void messageToArray(T message, bsl::vector<uint8_t> &buffer) {
  const std::size_t size = message.ByteSizeLong();
  buffer.clear();
  buffer.resize(size);
  message.SerializeWithCachedSizesToArray(buffer.data());
}

bsl::string uri(){
  std::random_device dev;
  std::uniform_int_distribution<uint64_t> dist;
  return bsl::to_string(dist(dev))+'-'+bsl::to_string(dist(dev));
}

int64_t registerWithMarket(ListenStuff listenStuff, rmqa::Producer &producer,
                        std::string_view username,
                        kafka::clients::producer::KafkaProducer &kafkaProducer) {
  auto &[vhost, exch, topology] = listenStuff;
  MarketProto::SignupMSG signupMSG;
  signupMSG.set_name(std::string(username));
  bsl::shared_ptr<bsl::vector<uint8_t>> rawData =
      bsl::make_shared<bsl::vector<uint8_t>>();
  messageToArray(signupMSG, *rawData);
  rmqt::Properties properties;
  rmqt::QueueHandle responseQueue = topology.addQueue(uri(), rmqt::AutoDelete::ON);
  properties.replyTo = responseQueue.lock()->name();
  topology.bind(exch, responseQueue, properties.replyTo.value());
  std::promise<int64_t> setUpPromise;
  std::future<int64_t> setUpFuture = setUpPromise.get_future();
  auto config = rmqt::ConsumerConfig();
  config.setExclusiveFlag(rmqt::Exclusive::ON);
  std::atomic<bool> signupResponseReceived{false};
  rmqt::Result<rmqa::Consumer> activateConsumerResult = vhost.createConsumer(
      topology, responseQueue,
      [&kafkaProducer, &setUpPromise, &signupResponseReceived](rmqp::MessageGuard &messageGuard) {
        if (signupResponseReceived.exchange(true)) {
          std::cerr << "Received unexpected duplicate signup response; ignoring.\n";
          messageGuard.ack();
          return;
        }
        const rmqt::Message &message = messageGuard.message();
        MarketProto::SignupResponseMSG signupResponseMSG;
        if (!signupResponseMSG.ParseFromArray(message.payload(),
                                              message.payloadSize())) {
          std::cerr << "Failed to parse message\n";
          setUpPromise.set_value(failureId);
        } else {
          setUpPromise.set_value(signupResponseMSG.assignedid());
        }
        auto msgLen = static_cast<std::size_t>(signupResponseMSG.ByteSizeLong());
        uint8_t *rawBuf = new uint8_t[msgLen];
        signupResponseMSG.SerializeWithCachedSizesToArray(rawBuf);
        kafkaProducer.send(
            kafka::clients::producer::ProducerRecord(
                KafkaTopic::SignupResponse, kafka::NullKey,
                kafka::Value(rawBuf, msgLen)),
            KafkaDeliveryCBSingleData(rawBuf));
        messageGuard.ack();
      },config);
  if (!activateConsumerResult) {
    std::cerr << "Error creating connection: " << activateConsumerResult.error()
              << "\n";
    return failureId;
  }
  rmqt::Message message = rmqt::Message(rawData, properties);
  if (!sendMessage(producer, message, "signup")) {
    return failureId;
  }
  // Send signup to Kafka
  {
    auto msgLen = static_cast<std::size_t>(signupMSG.ByteSizeLong());
    uint8_t *rawBuf = new uint8_t[msgLen];
    signupMSG.SerializeWithCachedSizesToArray(rawBuf);
    kafkaProducer.send(
        kafka::clients::producer::ProducerRecord(
            KafkaTopic::Signup, kafka::NullKey, kafka::Value(rawBuf, msgLen)),
        KafkaDeliveryCBSingleData(rawBuf));
  }
  if (!producer.waitForConfirms(bsls::TimeInterval(5))) {
    std::cerr << "Timeout expired for sending message to signup to market\n";
  }
  if (setUpFuture.wait_for(std::chrono::seconds(5)) ==
      std::future_status::timeout) {
    std::cerr << "Did not hear back from market\n";
    return failureId;
  }
  activateConsumerResult.value()->cancelAndDrain();
  int64_t queueRet = setUpFuture.get();
  if (queueRet==failureId) {
    std::cerr << "Market did not confirm\n";
    return failureId;
  }
  return queueRet;
}

OrderListenhandle startListeningToOrderResponses(ListenStuff listenStuff,
                                                 const bsl::string &id,
                                                 MarketState &marketState) {
  OrderListenhandle ret;
  auto &[vhost, exch, topology] = listenStuff;
  bsl::string responseQueueKey = id + ".orderResponse";
  std::cout << responseQueueKey << '\n';
  ret.responseQueue = topology.addQueue(responseQueueKey, rmqt::AutoDelete::OFF,
                                        rmqt::Durable::ON);
  topology.bind(exch, ret.responseQueue, responseQueueKey);
  rmqt::Result<rmqa::Consumer> responsesConsumerResult = vhost.createConsumer(
      topology, ret.responseQueue,
      [&marketState](rmqp::MessageGuard &messageGuard) {
        const rmqt::Message &message = messageGuard.message();
        assert(message.properties().correlationId.has_value());
        MarketProto::OrderResponseMSG respMesg;
        respMesg.ParseFromArray(message.payload(), message.payloadSize());
        marketState.processOrderResponse(
            bsl::stoll(message.properties().correlationId.value()), respMesg);
        messageGuard.ack();
      });
  if (!responsesConsumerResult) {
    std::cerr << "Error creating connection: "
              << responsesConsumerResult.error() << "\n";
    return ret;
  }
  ret.responseConsumer = responsesConsumerResult.value();
  ret.fillQueue = topology.addQueue(id + ".orderFill", rmqt::AutoDelete::OFF,
                                    rmqt::Durable::ON);
  topology.bind(exch, ret.fillQueue, id + ".orderFill");
  rmqt::Result<rmqa::Consumer> fillConsumerResult = vhost.createConsumer(
      topology, ret.fillQueue,
      [&marketState](rmqp::MessageGuard &messageGuard) {
        const rmqt::Message &message = messageGuard.message();
        MarketProto::OrderFillMSG orderFillMSG;
        orderFillMSG.ParseFromArray(message.payload(), message.payloadSize());
        marketState.processOrderFill(orderFillMSG);
        messageGuard.ack();
      });
  if (!fillConsumerResult) {
    std::cerr << "Error creating connection: " << fillConsumerResult.error()
              << "\n";
    return ret;
  }
  ret.fillConsumer = fillConsumerResult.value();
  return ret;
}

bool sendOrderToMarket(rmqa::Producer &producer, const bsl::string &id,
                       AtomicCounter &correlationIdGenerator, MarketOrder order,
                       MarketState &marketState) {
  int64_t correlationId = correlationIdGenerator.get();
  MarketProto::OrderMSG orderMSG = order.toOrderMSG();
  bsl::shared_ptr<bsl::vector<uint8_t>> rawData =
      bsl::make_shared<bsl::vector<uint8_t>>();
  messageToArray(orderMSG, *rawData);
  rmqt::Properties properties;
  properties.correlationId = bsl::to_string(correlationId);
  properties.replyTo = id;
  rmqt::Message message = rmqt::Message(rawData, properties);
  marketState.processOrderToMarket(correlationId, order, rawData->data(), rawData->size());
  if (!sendMessage(producer, message, "order")) {
    return false;
  }
  return true;
}
