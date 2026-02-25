#pragma once
#include "kafkaTopics.h"
#include "marketMessages.pb.h"
#include "printMarketProto.h"
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iostream>
#include <kafka/KafkaProducer.h>
#include <kafka/Types.h>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

// Kafka send function type: caller-owned bytes, callee must copy if async.
using KafkaSendFn =
    std::function<void(const void *data, std::size_t len, const kafka::Topic &topic)>;

constexpr int maxSymbolSize = 5;

struct MarketOrder {
  MarketOrder(std::string symbolI, int64_t amountI, int64_t priceI,
              bool buySideI)
      : symbol(std::move(symbolI)), amount(amountI), price(priceI), buySide(buySideI) {
    assert(symbol.size() <= maxSymbolSize);
    assert(amount > 0);
    assert(price >= 0);
  }
  MarketProto::OrderMSG toOrderMSG() const {
    MarketProto::OrderMSG ret;
    ret.set_symbol(std::string(symbol));
    ret.set_quantity(amount);
    ret.set_price(price);
    ret.set_buyside(buySide);
    return ret;
  }
  std::string symbol;
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
  ActiveMarketOrder(MarketOrder&& origin, int64_t initFilled)
      : symbol(std::move(origin.symbol)), amount(origin.amount), filled(initFilled),
        price(origin.price), buySide(origin.buySide) {}
  std::string symbol;
  int64_t amount;
  std::atomic<int64_t> filled;
  int64_t price;
  bool buySide;
};

inline std::ostream &operator<<(std::ostream &out, MarketOrder order) {
  return out << (order.buySide ? "Buy" : "Sell") << "ing " << order.amount
             << " of " << order.symbol << " for " << FormatAsMoney(order.price);
}

class MarketState {
public:
  // Test constructor: supply a custom (e.g. no-op) send function.
  explicit MarketState(KafkaSendFn fn) : d_kafkaSend(std::move(fn)) {}

  // Production constructor: creates a real KafkaProducer. Defined in main.cpp.
  MarketState();

  MarketState(const MarketState &) = delete;

  void processOrderToMarket(int64_t correlationId, const MarketOrder &order,
                            const void *data, std::size_t len) {
    d_kafkaSend(data, len, KafkaTopic::Order);
    std::lock_guard lk(corrMutex);
    ordersInFlight.emplace(correlationId, order);
  }

  void processOrderResponse(int64_t correlationId,
                            const MarketProto::OrderResponseMSG &respMesg) {
    {
      auto msgLen = static_cast<std::size_t>(respMesg.ByteSizeLong());
      std::vector<uint8_t> buf(msgLen);
      respMesg.SerializeWithCachedSizesToArray(buf.data());
      d_kafkaSend(buf.data(), msgLen, KafkaTopic::OrderResponse);
    }
    auto originOrderHandle = [&]() {
      std::lock_guard lk(corrMutex);
      return ordersInFlight.extract(correlationId);
    }();
    assert(!originOrderHandle.empty());
    MarketOrder orginOrder = originOrderHandle.mapped();
    if (!respMesg.successful()) {
      std::cout << "order received. correlation Id: " << correlationId << '\n';
      std::cout << "The order was not successful\n";
      return;
    }
    int64_t amountFilled = respMesg.amountfilled();
    if (respMesg.has_orderid()) {
      assert(respMesg.amountfilled() < orginOrder.amount);
      int64_t orderId = respMesg.orderid();
      std::cout << "Order id: " << orderId << "\n";
      std::lock_guard lk(idsMutex);
      ordersOnMarket.emplace(orderId, std::make_unique<ActiveMarketOrder>(
                                          std::move(orginOrder), amountFilled));
    } else {
      assert(respMesg.amountfilled() == orginOrder.amount);
    }
  }

  void processOrderFill(const MarketProto::OrderFillMSG &orderFillMSG) {
    {
      auto msgLen = static_cast<std::size_t>(orderFillMSG.ByteSizeLong());
      std::vector<uint8_t> buf(msgLen);
      orderFillMSG.SerializeWithCachedSizesToArray(buf.data());
      d_kafkaSend(buf.data(), msgLen, KafkaTopic::OrderFill);
    }
    int64_t numFilled = orderFillMSG.filled();
    assert(numFilled>0);
    int64_t orderId = orderFillMSG.orderid();
    auto orderFound = [&]() -> std::shared_ptr<ActiveMarketOrder> {
      auto totalSleep = std::chrono::milliseconds(0);
      auto sleepTime = std::chrono::milliseconds(10);
      while (true) {
        std::shared_lock lk(idsMutex);
        auto orderFoundIter = ordersOnMarket.find(orderId);
        if (orderFoundIter != ordersOnMarket.end()) {
          return orderFoundIter->second;
        }
        lk.unlock();
        std::this_thread::sleep_for(sleepTime); 
        std::cout << "Sleep time\n";
        totalSleep+=sleepTime;
        if(totalSleep>std::chrono::seconds(5)){
          return nullptr;
        }
        sleepTime *= 2;
      }
    }();
    if(!orderFound){
      assert(false); // add 
    }
    const auto amnt = orderFound->amount;
    if ((orderFound->filled += numFilled) == amnt) {
      std::lock_guard lk(idsMutex);
      ordersOnMarket.erase(orderId);
      assert(orderFound->filled == amnt);
    }
  }

  // Accessors for testing and diagnostics.
  bool hasOrderInFlight(int64_t corrId) {
    std::lock_guard lk(corrMutex);
    return ordersInFlight.count(corrId) > 0;
  }
  bool hasOrderOnMarket(int64_t orderId) {
    std::shared_lock lk(idsMutex);
    return ordersOnMarket.count(orderId) > 0;
  }
  int64_t getOrderFilled(int64_t orderId) {
    std::shared_lock lk(idsMutex);
    auto it = ordersOnMarket.find(orderId);
    if (it == ordersOnMarket.end())
      return -1;
    return it->second->filled.load();
  }
  bool ordersOnMarketEmpty() {
    std::shared_lock lk(idsMutex);
    return ordersOnMarket.empty();
  }

  // Returns the real KafkaProducer; only valid on default-constructed instances.
  kafka::clients::producer::KafkaProducer &getKafkaProducer() {
    return *d_kafkaProducer;
  }

private:
  std::shared_ptr<kafka::clients::producer::KafkaProducer> d_kafkaProducer;
  KafkaSendFn d_kafkaSend;
  std::mutex corrMutex;
  std::unordered_map<int64_t, MarketOrder> ordersInFlight;
  std::shared_mutex idsMutex;
  std::unordered_map<int64_t, std::shared_ptr<ActiveMarketOrder>>
      ordersOnMarket;
};
