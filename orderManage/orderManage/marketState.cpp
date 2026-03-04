#include "kafkaTopics.h"
#include "marketConnect.h"
#include "logMessages.pb.h"
#include <bsl_optional.h>
#include <bsl_vector.h>
#include <cassert>
#include <future>
#include <iostream>
#include <kafka/Types.h>
#include <random>
#include <string>
#include <sys/types.h>
#include <utility>
using namespace BloombergLP;

struct KafkaDeliveryCBSharedData {
  KafkaDeliveryCBSharedData(bsl::shared_ptr<bsl::vector<uint8_t>> &&data)
      : data_(std::move(data)) {}

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
  KafkaDeliveryCBSingleData(uint8_t *data) : data_(data) {}

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
  uint8_t *data_;
};

struct KafkaDeliveryCBNoeData {
  void operator()(const kafka::clients::producer::RecordMetadata &metadata,
                  const kafka::Error &error) {
    if (!error) {
      std::cout << "Message delivered: " << metadata.toString() << '\n';
    } else {
      std::cerr << "Message failed to be delivered: " << error.message()
                << '\n';
    }
  }
};

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
bsl::shared_ptr<bsl::vector<uint8_t>> messageToArray(const T& message) {
  bsl::shared_ptr<bsl::vector<uint8_t>> rawData =
      bsl::make_shared<bsl::vector<uint8_t>>();
  auto& buffer = *rawData;
  const std::size_t size = message.ByteSizeLong();
  buffer.clear();
  buffer.resize(size);
  auto success = message.SerializeToArray(buffer.data(),buffer.size());
  assert(success);
  return rawData;
}

template <class T>
bool logKafkaMessage(const kafka::Topic& topic, const kafka::Key key, const T& message, kafka::clients::producer::KafkaProducer &logProducer){
  auto rawSuccessLogData = messageToArray(message);
  kafka::Value successValue(rawSuccessLogData->data(), rawSuccessLogData->size());
  try {
    logProducer.send(kafka::clients::producer::ProducerRecord(
                         topic, key, successValue),
                     KafkaDeliveryCBSharedData(std::move(rawSuccessLogData)));
  } catch (const kafka::KafkaException &e) {
    std::cout << e.what() << '\n';
    return false;
  }
  return true;
}

bsl::string uri() {
  std::random_device dev;
  std::uniform_int_distribution<uint64_t> dist;
  return bsl::to_string(dist(dev)) + '-' + bsl::to_string(dist(dev));
}

int64_t
registerWithMarket(ListenStuff listenStuff, rmqa::Producer &producer,
                   kafka::clients::producer::KafkaProducer &kafkaProducer,
                   kafka::Key username) {
  auto &[vhost, exch, topology] = listenStuff;
  LogProto::RabbitMSGIntent logMessage;
  MarketProto::SignupMSG& signupMSG = *logMessage.mutable_signup();
  signupMSG.set_name(username.toString());
  auto rawData = messageToArray(signupMSG);
  rmqt::Properties properties;
  bsl::string repQueueName = uri();
  rmqt::QueueHandle responseQueue =
      topology.addQueue(repQueueName, rmqt::AutoDelete::ON);
  properties.replyTo = repQueueName;
  topology.bind(exch, responseQueue, properties.replyTo.value());
  std::promise<int64_t> setUpPromise;
  std::future<int64_t> setUpFuture = setUpPromise.get_future();
  auto config = rmqt::ConsumerConfig();
  config.setExclusiveFlag(rmqt::Exclusive::ON);
  std::atomic<bool> signupResponseReceived{false};
  rmqt::Result<rmqa::Consumer> activateConsumerResult = vhost.createConsumer(
      topology, responseQueue,
      [&kafkaProducer, &setUpPromise,
       &signupResponseReceived, username](rmqp::MessageGuard &messageGuard) {
        if (signupResponseReceived.exchange(true)) {
          std::cerr
              << "Received unexpected duplicate signup response; ignoring.\n";
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
        kafkaProducer.send(kafka::clients::producer::ProducerRecord(
                               KafkaTopic::SignupResponse, username,
                               kafka::Value(message.payload(), message.payloadSize())),
                           KafkaDeliveryCBNoeData(),kafka::clients::producer::KafkaProducer::SendOption::
                ToCopyRecordValue);
        messageGuard.ack();
      },
      config);
  if (!activateConsumerResult) {
    std::cerr << "Error creating connection: " << activateConsumerResult.error()
              << "\n";
    return failureId;
  }
  rmqt::Message message = rmqt::Message(rawData, properties);
  logMessage.set_guid(message.guid().data(),16);
  logKafkaMessage(KafkaTopic::Signup, username, logMessage, kafkaProducer);
  if (!sendMessage(producer, message, "signup")) {
    return failureId;
  }
  LogProto::RabbitMSGSuccess successMessage;
  successMessage.set_guid(message.guid().data(),16);
  logKafkaMessage(KafkaTopic::Signup, username, successMessage, kafkaProducer);
  if (!producer.waitForConfirms(bsls::TimeInterval(5))) {
    std::cerr << "Timeout expired for sending message to signup to market\n";
    return failureId;
  }
  if (setUpFuture.wait_for(std::chrono::seconds(5)) ==
      std::future_status::timeout) {
    std::cerr << "Did not hear back from market\n";
    return failureId;
  }
  activateConsumerResult.value()->cancelAndDrain();
  int64_t queueRet = setUpFuture.get();
  if (queueRet == failureId) {
    std::cerr << "Market did not confirm\n";
    return failureId;
  }
  return queueRet;
}

OrderListenhandle startListeningToOrderResponses(
    ListenStuff listenStuff,
    kafka::clients::producer::KafkaProducer &logProducer, const bsl::string &id, kafka::Key username,
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
      [&marketState, &logProducer, username](rmqp::MessageGuard &messageGuard) {
        const rmqt::Message &message = messageGuard.message();
        assert(message.properties().correlationId.has_value());
        MarketProto::OrderResponseMSG respMesg;
        auto success = respMesg.ParseFromArray(message.payload(), message.payloadSize());
        assert(success);
        logProducer.send(
            kafka::clients::producer::ProducerRecord(
                KafkaTopic::OrderResponse, username,
                kafka::Value(message.payload(), message.payloadSize())),
            KafkaDeliveryCBNoeData(),
            kafka::clients::producer::KafkaProducer::SendOption::
                ToCopyRecordValue);
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
      [&marketState, &logProducer, username](rmqp::MessageGuard &messageGuard) {
        const rmqt::Message &message = messageGuard.message();
        MarketProto::OrderFillMSG orderFillMSG;
        auto success = orderFillMSG.ParseFromArray(message.payload(), message.payloadSize());
        assert(success);
        logProducer.send(
            kafka::clients::producer::ProducerRecord(
                KafkaTopic::OrderFill, username,
                kafka::Value(message.payload(), message.payloadSize())),
            KafkaDeliveryCBNoeData(),
            kafka::clients::producer::KafkaProducer::SendOption::
                ToCopyRecordValue);
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

bool sendOrderToMarket(rmqa::Producer &marketProducer,
                       kafka::clients::producer::KafkaProducer &logProducer,
                       bsl::string id, kafka::Key username, AtomicCounter &corrIdGenerator,
                       MarketOrder order, MarketState &marketState) {
  int64_t correlationId = corrIdGenerator.get();
  MarketProto::OrderMSG orderMSG = order.toOrderMSG();
  auto rawData = messageToArray(orderMSG);
  rmqt::Properties properties;
  properties.correlationId = bsl::to_string(correlationId);
  properties.replyTo = std::move(id);
  rmqt::Message message = rmqt::Message(rawData, properties);
  LogProto::RabbitMSGIntent logMessage;
  logMessage.set_guid(message.guid().data(),16);
  logMessage.mutable_order()->CopyFrom(orderMSG);
  logKafkaMessage(KafkaTopic::Order,username,logMessage,logProducer);
  marketState.processOrderToMarket(correlationId, order);
  if (!sendMessage(marketProducer, message, "order")) {
    return false;
  }
  LogProto::RabbitMSGSuccess successMsg;
  successMsg.set_guid(message.guid().data(),16);
  logKafkaMessage(KafkaTopic::Order,username,successMsg,logProducer);
  return true;
}
