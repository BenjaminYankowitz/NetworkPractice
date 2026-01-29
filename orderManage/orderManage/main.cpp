#include <atomic>
#include <bslstl_sharedptr.h>
#include <bslstl_string.h>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <rmqa_connectionstring.h>
#include <rmqa_consumer.h>
#include <rmqa_producer.h>
#include <rmqa_rabbitcontext.h>
#include <rmqa_topology.h>
#include <rmqa_vhost.h>
#include <rmqp_consumer.h>
#include <rmqp_producer.h>
#include <rmqt_confirmresponse.h>
#include <rmqt_exchange.h>
#include <rmqt_exchangetype.h>
#include <rmqt_fieldvalue.h>
#include <rmqt_message.h>
#include <rmqt_properties.h>
#include <rmqt_result.h>
#include <rmqt_vhostinfo.h>

#include <bsl_memory.h>
#include <bsl_optional.h>
#include <bsl_vector.h>

#include "marketMessages.pb.h"
#include <condition_variable>
#include <cstdlib>
#include <iostream>
#include <random>
#include <sys/types.h>
#include <thread>
#include <unordered_map>
using namespace BloombergLP;

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

class FormatAsMoney {
public:
  FormatAsMoney(long amnt) : amnt_(amnt) {}
  friend std::ostream &operator<<(std::ostream &out, FormatAsMoney money);

private:
  int amnt_;
};
std::ostream &operator<<(std::ostream &out, FormatAsMoney money) {
  int dollars = money.amnt_ / 100;
  int cents = money.amnt_ % 100;
  out << "$" << dollars;
  if (cents != 0) {
    out << '.';
    if (cents < 10) {
      out << '0';
    }
    out << cents;
  }
  return out;
}
std::ostream &operator<<(std::ostream &out, MarketOrder order) {
  return out << (order.buySide ? "Buy" : "Sell") << "ing " << order.amount
             << " of " << order.symbol << " for " << FormatAsMoney(order.price);
}

class MarketState {
public:
  void processOrderToMarket(int64_t correlationId, const MarketOrder& order) {
    std::lock_guard lk(corrMutex);
    ordersInFlight.emplace(correlationId, order);
  }
  void processOrderResponse(int64_t correlationId, const OrderResponseMSG& respMesg) {
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
        ordersOnMarket.emplace(orderId, std::make_unique<ActiveMarketOrder>(orginOrder, amountFilled));
      }();
    } else {
      assert(respMesg.amountfilled() == orginOrder.amount);
      std::cout << "filling the entire order\n";
    }
  }
  void processOrderFill(const OrderFillMSG& orderFillMSG){
    int64_t numFilled = orderFillMSG.filled();
    int64_t orderId = orderFillMSG.orderid();
    std::cout << "order fill start ||||||||||||||||||||||\n";
    std::cout << "order filled. order id: " << orderId << '\n';
    std::cout << numFilled << " shares were filled.\n";
    std::cout << "order fill end ||||||||||||||||||||||\n";
    auto orderFound = [&]() {
      std::shared_lock lk(idsMutex);
      while (true) {
        auto orderFoundIter = ordersOnMarket.find(orderId);
        if (orderFoundIter != ordersOnMarket.end()) {
          return orderFoundIter->second.get();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        std::cout << "Sleep time\n"; // this is a hack IDK how to do it better, but there is definity a way.
      }
    }();
      if((orderFound->filled+=numFilled)==orderFound->amount){ // safe because filled is atomic.
        std::lock_guard lk(idsMutex);
        ordersOnMarket.erase(orderId); 
        // Not using iterator because could give up lock, another thread inserts causing rehash breaking the iterator, before relocking.
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

int64_t getId() {
  std::random_device dev;
  std::uniform_int_distribution<int64_t> dist;
  return dist(dev);
}

bool handleInitialResponse(const rmqt::Message &message) {
  SignupResponseMSG signupResponseMSG;
  if (!signupResponseMSG.ParseFromArray(message.payload(),
                                        message.payloadSize())) {
    std::cerr << "Failed to parse message\n";
    return false;
  } else if (signupResponseMSG.result() == success) {
    std::cout << "Connected\n";
    return true;
  } else if (signupResponseMSG.result() == tooLong) {
    std::cerr << "Username too long\n";
    return false;
  } else if (signupResponseMSG.result() == taken) {
    std::cerr << "Username taken\n";
    return false;
  } else if (signupResponseMSG.result() == malformed) {
    std::cerr << "message malformed\n";
    return false;
  } else {
    std::cerr << "Unknown error\n";
    return false;
  }
}

struct ListenStuff {
  rmqa::VHost &vhost;
  rmqt::ExchangeHandle &exch;
  rmqa::Topology &topology;
};

bool registerWithMarket(ListenStuff listenStuff, rmqa::Producer &producer,
                        std::string_view username, const bsl::string &id) {
  auto &[vhost, exch, topology] = listenStuff;
  SignupMSG signupMSG;
  signupMSG.set_name(username);
  bsl::shared_ptr<bsl::vector<uint8_t>> rawData =
      bsl::make_shared<bsl::vector<uint8_t>>();
  messageToArray(signupMSG, *rawData);
  rmqt::Properties properties;
  properties.replyTo = id;
  properties.correlationId = id;
  std::cout << properties.replyTo << '\n';
  rmqt::Message message = rmqt::Message(rawData, properties);
  rmqt::QueueHandle responseQueue = topology.addQueue(
      id + ".activate", rmqt::AutoDelete::ON, rmqt::Durable::ON);
  topology.bind(exch, responseQueue, id + ".activate");
  std::condition_variable cv;
  std::mutex m;
  bool didQueue = true;
  bool queueRet = false;
  rmqt::Result<rmqa::Consumer> activateConsumerResult = vhost.createConsumer(
      topology, responseQueue,
      [&cv, &m, &didQueue, &queueRet](rmqp::MessageGuard &messageGuard) {
        {
          std::lock_guard lk(m);
          const rmqt::Message &message = messageGuard.message();
          bool success = handleInitialResponse(message);
          messageGuard.ack();
          if (!success) {
            didQueue = false;
          }
          queueRet = true;
        }
        cv.notify_one();
      });
  if (!activateConsumerResult) {
    // A fatal error such as `exch` not being present in `topology`
    // A disconnection, or  will never permanently fail an operation
    std::cerr << "Error creating connection: " << activateConsumerResult.error()
              << "\n";
    return 1;
  }
  bsl::shared_ptr<rmqa::Consumer> activateConsumer =
      activateConsumerResult.value();
  if (!sendMessage(producer, message, "signup")) {
    return 1;
  }
  if (!producer.waitForConfirms(bsls::TimeInterval(5))) {
    std::cerr << "Timeout expired for sending message to signup to market\n";
  }

  std::unique_lock lk(m);
  cv.wait_for(lk, std::chrono::seconds(5), [&queueRet]() { return queueRet; });
  activateConsumer->cancelAndDrain();
  if (!queueRet) {
    std::cerr << "Market did not confirm\n";
    return false;
  }
  if (!didQueue) {
    std::cerr << "Could not add to queue\n";
    return false;
  }
  return true;
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
  marketState.processOrderToMarket(correlationId, order);
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
  const bsl::string id = bsl::to_string(getId());
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
  if (!registerWithMarket(listenStuff, *producer, username, id)) {
    return 1;
  }
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
