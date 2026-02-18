#include "marketMessages.pb.h"
#include "printMarketProto.h"
#include <atomic>
#include <bsl_memory.h>
#include <bsl_optional.h>
#include <bsl_vector.h>
#include <bslstl_sharedptr.h>
#include <bslstl_string.h>
#include <cassert>
#include <chrono>
#include <future>
#include <iostream>
#include <cstdint>
#include <kafka/KafkaProducer.h>
#include <kafka/Types.h>
#include <memory>
#include <mutex>
#include <random>
#include <rmqa_connectionstring.h>
#include <rmqa_consumer.h>
#include <rmqa_producer.h>
#include <rmqa_rabbitcontext.h>
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
#include <rmqt_vhostinfo.h>
#include <string>
#include <sys/types.h>
#include <thread>
#include <unordered_map>
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

kafka::clients::producer::KafkaProducer initKafka(){
  std::string brokers = "localhost:9092";
  kafka::Properties props(
      {{"bootstrap.servers", {brokers}}, {"enable.idempotence", {"true"}}});
  return kafka::clients::producer::KafkaProducer(props);

}
namespace KafkaTopic {
  static const kafka::Topic Signup = "signup";
  static const kafka::Topic SignupResponse = "signup-response";
  static const kafka::Topic Order = "order";
  static const kafka::Topic OrderResponse = "order-response";
  static const kafka::Topic OrderFill = "order-fill";
}

void kafkaSend(kafka::clients::producer::KafkaProducer &producer, bsl::shared_ptr<bsl::vector<uint8_t>> &&data,
               kafka::Topic topic, kafka::Key key) {
                auto val = kafka::Value(data->data(), data->size());
  producer.send(
      kafka::clients::producer::ProducerRecord(
          topic, key, val),
      KafkaDeliveryCBSharedData(std::move(data)));
}

template<class T>
void kafkaSend(kafka::clients::producer::KafkaProducer &producer, T &message,
               kafka::Topic topic, kafka::Key key) {
  auto len = message.ByteSizeLong();
  uint8_t* rawData = new uint8_t[len];
  message.SerializeWithCachedSizesToArray(rawData);
  producer.send(
      kafka::clients::producer::ProducerRecord(
          topic, key, kafka::Value(rawData, len)),
      KafkaDeliveryCBSingleData(rawData));
}


constexpr int maxSymbolSize = 5;
struct MarketOrder {
  MarketOrder(std::string_view symbolI, int64_t amountI, int64_t priceI,
              bool buySideI)
      : symbol(symbolI), amount(amountI), price(priceI), buySide(buySideI) {
    assert(symbol.size() <= maxSymbolSize);
    assert(amount > 0);
    assert(price >= 0);
  }
  OrderMSG toOrderMSG() const {
    OrderMSG ret;
    ret.set_symbol(symbol);
    ret.set_quantity(amount);
    ret.set_price(price);
    ret.set_buyside(buySide);
    return ret;
  }
  std::string_view symbol;
  int64_t amount;
  int64_t price;
  bool buySide;
};

class AtomicCounter {
public:
  int64_t get() { return n++; }

private:
  std::atomic<int64_t> n = 0;
};

struct ActiveMarketOrder {
  ActiveMarketOrder(MarketOrder origin, int64_t initFilled)
      : symbol(origin.symbol), amount(origin.amount), filled(initFilled),
        price(origin.price), buySide(origin.buySide) {}
  std::string_view symbol;
  int64_t amount;
  std::atomic<int64_t> filled;
  int64_t price;
  bool buySide;
};

std::ostream &operator<<(std::ostream &out, MarketOrder order) {
  return out << (order.buySide ? "Buy" : "Sell") << "ing " << order.amount
             << " of " << order.symbol << " for " << FormatAsMoney(order.price);
}

class MarketState {
public:
  void processOrderToMarket(int64_t correlationId, const MarketOrder &order,
                            bsl::shared_ptr<bsl::vector<uint8_t>> &&rawData) {
    kafkaSend(kafkaProducer,std::move(rawData),KafkaTopic::Order, kafka::NullKey);
    std::lock_guard lk(corrMutex);
    ordersInFlight.emplace(correlationId, order);
  }
  void processOrderResponse(int64_t correlationId,
                            const OrderResponseMSG &respMesg) {
    kafkaSend(kafkaProducer,respMesg,KafkaTopic::OrderResponse,kafka::NullKey);
    std::cout << "order response start -----------------------\n";
    std::cout << "order received. correlation Id: " << correlationId << '\n';
    auto originOrderHandle = [&]() {
      std::lock_guard lk(corrMutex);
      return ordersInFlight.extract(correlationId);
    }();
    assert(!originOrderHandle.empty());
    MarketOrder orginOrder = originOrderHandle.mapped();
    if (!respMesg.successful()) {
      std::cout << "The order was not successful\n";
      return;
    }
    int64_t amountFilled = respMesg.amountfilled();
    std::cout << respMesg.amountfilled() << " out of " << orginOrder.amount
              << " shares were filled for a total cost of "
              << FormatAsMoney(respMesg.price()) << '\n';
    if (respMesg.has_orderid()) {
      assert(respMesg.amountfilled() < orginOrder.amount);
      int64_t orderId = respMesg.orderid();
      std::cout << "Order id: " << orderId << "\n";
      [&]() {
        std::lock_guard lk(idsMutex);
        ordersOnMarket.emplace(orderId, std::make_unique<ActiveMarketOrder>(
                                            orginOrder, amountFilled));
      }();
    } else {
      assert(respMesg.amountfilled() == orginOrder.amount);
      std::cout << "filling the entire order\n";
    }
  }
  void processOrderFill(const OrderFillMSG &orderFillMSG) {
    kafkaSend(kafkaProducer,orderFillMSG,KafkaTopic::OrderFill,kafka::NullKey);
    int64_t numFilled = orderFillMSG.filled();
    int64_t orderId = orderFillMSG.orderid();
    std::cout << "order fill start ||||||||||||||||||||||\n";
    std::cout << "order filled. order id: " << orderId << '\n';
    std::cout << numFilled << " shares were filled.\n";
    std::cout << "order fill end ||||||||||||||||||||||\n";
    auto orderFound = [&]() {
      auto sleepTime = std::chrono::milliseconds(10);
      while (true) {
        std::shared_lock lk(idsMutex);
        auto orderFoundIter = ordersOnMarket.find(orderId);
        if (orderFoundIter != ordersOnMarket.end()) {
          return orderFoundIter->second.get();
        }
        lk.unlock();
        std::this_thread::sleep_for(
            sleepTime); // this is a hack IDK how to do it better, but there is
                        // definity a way.
        std::cout << "Sleep time\n";
        sleepTime *= 2; // At least have exponential backoff to prevent
                        // situation where (many) readers checking if written
                        // prevent writer from ever writing.
      }
    }();
    if ((orderFound->filled += numFilled) ==
        orderFound->amount) { // safe because filled is atomic.
      std::lock_guard lk(idsMutex);
      ordersOnMarket.erase(orderId);
      // Not using iterator because could give up lock, another thread inserts
      // causing rehash breaking the iterator, before relocking.
    }
  }
  MarketState(const MarketState &) = delete;
  MarketState() = default;

private:
  std::mutex corrMutex;
  std::unordered_map<int64_t, MarketOrder> ordersInFlight;
  std::shared_mutex idsMutex;
  std::unordered_map<int64_t, std::unique_ptr<ActiveMarketOrder>>
      ordersOnMarket;
  public:
  kafka::clients::producer::KafkaProducer kafkaProducer = initKafka();
};

constexpr const char *ExchangeName = "MarketExchange";

bool sendMessage(rmqa::Producer &producer, const rmqt::Message &message,
                 const bsl::string &routingKey) {
  auto confirmCallback = [](const rmqt::Message &message,
                            const bsl::string &routingKey,
                            const rmqt::ConfirmResponse &response) {
    // https://www.rabbitmq.com/confirms.html#when-publishes-are-confirmed
    if (response.status() == rmqt::ConfirmResponse::ACK) {
      // Message is now guaranteed to be safe with the broker.
      // Now is the time to reply to the request, commit the
      // database transaction, or ack the RabbitMQ message which
      // triggered this publish
    } else {
      // Send error response, rollback transaction, nack message
      // and/or raise an alarm for investigation - your message is not
      // delivered to all (or perhaps any) of the queues it was
      // intended for.
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
struct ListenStuff {
  rmqa::VHost &vhost;
  rmqt::ExchangeHandle &exch;
  rmqa::Topology &topology;
};

bsl::string uri(){
  std::random_device dev;
  std::uniform_int_distribution<uint64_t> dist;
  return bsl::to_string(dist(dev))+'-'+bsl::to_string(dist(dev));
}

static constexpr int64_t failureId = 0;
int64_t registerWithMarket(ListenStuff listenStuff, rmqa::Producer &producer,
                        std::string_view username, kafka::clients::producer::KafkaProducer& kafkaProducer) {
  auto &[vhost, exch, topology] = listenStuff;
  SignupMSG signupMSG;
  signupMSG.set_name(username);
  bsl::shared_ptr<bsl::vector<uint8_t>> rawData =
      bsl::make_shared<bsl::vector<uint8_t>>();
  messageToArray(signupMSG, *rawData);
  rmqt::Properties properties;
  rmqt::QueueHandle responseQueue = topology.addQueue(uri(), rmqt::AutoDelete::ON, rmqt::Durable::ON);
  properties.replyTo = responseQueue.lock()->name();
  topology.bind(exch, responseQueue, properties.replyTo.value());
  std::promise<int64_t> setUpPromise;
  std::future<int64_t> setUpFuture = setUpPromise.get_future();
  auto config = rmqt::ConsumerConfig();
  config.setExclusiveFlag(rmqt::Exclusive::ON);
  rmqt::Result<rmqa::Consumer> activateConsumerResult = vhost.createConsumer(
      topology, responseQueue,
      [&setUpPromise](rmqp::MessageGuard &messageGuard) {
        const rmqt::Message &message = messageGuard.message();
        SignupResponseMSG signupResponseMSG;
        if (!signupResponseMSG.ParseFromArray(message.payload(),
                                              message.payloadSize())) {
          std::cerr << "Failed to parse message\n";
          setUpPromise.set_value(failureId);
        } else {
          setUpPromise.set_value(signupResponseMSG.assignedid());
        }
        messageGuard.ack(); // don't do this until everything handled properly.
      },config);
  if (!activateConsumerResult) {
    // A fatal error such as `exch` not being present in `topology`
    // A disconnection, or  will never permanently fail an operation
    std::cerr << "Error creating connection: " << activateConsumerResult.error()
              << "\n";
    return failureId;
  }
  std::cout << "replyto: " << properties.replyTo << '\n';
  rmqt::Message message = rmqt::Message(rawData, properties);
  if (!sendMessage(producer, message, "signup")) {
    return failureId;
  }
  kafkaSend(kafkaProducer,std::move(rawData),KafkaTopic::Signup, kafka::NullKey);
  if (!producer.waitForConfirms(bsls::TimeInterval(5))) {
    std::cerr << "Timeout expired for sending message to signup to market\n";
  }
  if (setUpFuture.wait_for(std::chrono::seconds(5)) ==
      std::future_status::timeout) {
    std::cerr << "Did not hear back from server\n";
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

struct OrderListenhandle {
  rmqt::QueueHandle responseQueue;
  bsl::shared_ptr<rmqa::Consumer> responseConsumer;
  rmqt::QueueHandle fillQueue;
  bsl::shared_ptr<rmqa::Consumer> fillConsumer;
};

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
        OrderResponseMSG respMesg;
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
        OrderFillMSG orderFillMSG;
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
  OrderMSG orderMSG = order.toOrderMSG();
  bsl::shared_ptr<bsl::vector<uint8_t>> rawData =
      bsl::make_shared<bsl::vector<uint8_t>>();
  messageToArray(orderMSG, *rawData);
  rmqt::Properties properties;
  properties.correlationId = bsl::to_string(correlationId);
  properties.replyTo = id;
  rmqt::Message message = rmqt::Message(rawData, properties);
  if (!sendMessage(producer, message, "order")) {
    return false;
  }
  marketState.processOrderToMarket(correlationId, order, std::move(rawData));
  return true;
}

int main(int argc, char **argv) {
  AtomicCounter correlationIdGenerator;
  // if (argc < 2) {
  //   std::cerr << "USAGE: " << argv[0] << " <amqp uri>\n";
  //   return 1;
  // }
  // amqp://localhost
  MarketState marketState;
  const char *const amqpuri = "amqp://localhost"; // argv[1];
  rmqa::RabbitContext rabbit;

  bsl::optional<rmqt::VHostInfo> vhostInfo =
      rmqa::ConnectionString::parse(amqpuri);

  if (!vhostInfo) {
    std::cerr << "Failed to parse connection string: " << amqpuri << "\n";
    return 1;
  }

  std::cout << "What is your name?\n";
  const std::string username = []() {
    std::string ret;
    if (!std::getline(std::cin, ret)) {
      std::cerr << "Failed to read username\n";
      std::exit(1);
    }
    return ret;
  }();

  // Returns immediately, setup performed on a different thread
  bsl::shared_ptr<rmqa::VHost> vhost = rabbit.createVHostConnection(
      username, // Connection Name Visible in management UI
      vhostInfo.value());
  // How many messages can be awaiting confirmation before `send` blocks
  constexpr uint16_t maxOutstandingConfirms = 10;

  rmqa::Topology topology;
  rmqt::ExchangeHandle exch = topology.addPassiveExchange(ExchangeName);
  if (exch.expired()) {
    std::cerr << "Server does not exist yet\n";
    return 1;
  }
  rmqt::Result<rmqa::Producer> producerResult =
      vhost->createProducer(topology, exch, maxOutstandingConfirms);
  if (!producerResult) {
    std::cerr << "Error creating connection: " << producerResult.error()
              << "\n";
    return 1;
  }
  ListenStuff listenStuff{*vhost, exch, topology};
  bsl::shared_ptr<rmqa::Producer> producer = producerResult.value();
  const auto numId = registerWithMarket(listenStuff, *producer, username,marketState.kafkaProducer);
  std::cout << numId << '\n';
  if(numId==failureId){
    std::cout << "Failed to register with market\n";
    return 1;
  }
  const auto id = bsl::to_string(numId);
  auto listenHandles =
      startListeningToOrderResponses(listenStuff, id, marketState);
  if (!(listenHandles.responseConsumer && listenHandles.fillConsumer)) {
    return 1;
  }
  sendOrderToMarket(*producer, id, correlationIdGenerator,
                    MarketOrder("slugs", 10, 1000, false), marketState);
  sendOrderToMarket(*producer, id, correlationIdGenerator,
                    MarketOrder("slugs", 10, 1000, true), marketState);
  std::cout << "Market confirmed\n";
  while (true) {
    std::this_thread::sleep_for(std::chrono::seconds(5));
  }
  return 0;
}
