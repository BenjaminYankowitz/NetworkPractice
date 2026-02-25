#include "marketState.h"
#include <future>
#include <gtest/gtest.h>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <vector>

// No-op KafkaSendFn for unit tests: Kafka is not required.
static KafkaSendFn noopKafkaSend = [](const void *, std::size_t,
                                      const kafka::Topic &) {};

// ---------------------------------------------------------------------------
// AtomicCounter tests
// ---------------------------------------------------------------------------

TEST(AtomicCounterTests, Monotonic) {
  AtomicCounter counter;
  EXPECT_EQ(counter.get(), 0);
  EXPECT_EQ(counter.get(), 1);
  EXPECT_EQ(counter.get(), 2);
}

TEST(AtomicCounterTests, ThreadSafe) {
  AtomicCounter counter;
  constexpr int numThreads = 50;
  constexpr int callsPerThread = 20; // 50*20 = 1000 total
  std::vector<std::thread> threads;
  std::vector<std::vector<int64_t>> results(numThreads);

  for (int t = 0; t < numThreads; ++t) {
    threads.emplace_back([&counter, &results, t]() {
      for (int i = 0; i < callsPerThread; ++i) {
        results[t].push_back(counter.get());
      }
    });
  }
  for (auto &th : threads)
    th.join();

  // All 1000 values must be unique (no duplicates).
  std::unordered_set<int64_t> seen;
  for (auto &vec : results) {
    for (int64_t v : vec) {
      EXPECT_TRUE(seen.insert(v).second) << "Duplicate counter value: " << v;
    }
  }
  EXPECT_EQ(seen.size(), numThreads * callsPerThread);
}

// ---------------------------------------------------------------------------
// MarketOrder tests
// ---------------------------------------------------------------------------

TEST(MarketOrderTests, ToProtoMappingBuy) {
  MarketOrder order("AAPL", 42, 15099, true);
  auto msg = order.toOrderMSG();
  EXPECT_EQ(msg.symbol(), "AAPL");
  EXPECT_EQ(msg.quantity(), 42);
  EXPECT_EQ(msg.price(), 15099);
  EXPECT_TRUE(msg.buyside());
}

TEST(MarketOrderTests, ToProtoMappingSell) {
  MarketOrder order("GOOG", 7, 200000, false);
  auto msg = order.toOrderMSG();
  EXPECT_EQ(msg.symbol(), "GOOG");
  EXPECT_EQ(msg.quantity(), 7);
  EXPECT_EQ(msg.price(), 200000);
  EXPECT_FALSE(msg.buyside());
}

// ---------------------------------------------------------------------------
// MarketState: processOrderToMarket
// ---------------------------------------------------------------------------

TEST(MarketStateTests, OrderToMarketAddsInFlight) {
  MarketState state(noopKafkaSend);
  MarketOrder order("AAPL", 10, 1000, true);
  state.processOrderToMarket(42, order, nullptr, 0);
  EXPECT_TRUE(state.hasOrderInFlight(42));
  EXPECT_FALSE(state.hasOrderInFlight(99)); // different id not present
}

// ---------------------------------------------------------------------------
// MarketState: processOrderResponse (full fill)
// ---------------------------------------------------------------------------

TEST(MarketStateTests, OrderResponseFullFillRemovesInFlight) {
  MarketState state(noopKafkaSend);
  MarketOrder order("AAPL", 10, 1000, true);
  state.processOrderToMarket(1, order, nullptr, 0);

  MarketProto::OrderResponseMSG resp;
  resp.set_successful(true);
  resp.set_amountfilled(10); // fully filled
  resp.set_price(10000);
  // No orderID set -> full fill

  state.processOrderResponse(1, resp);

  EXPECT_FALSE(state.hasOrderInFlight(1));
  // Nothing should be on the market
  EXPECT_TRUE(state.ordersOnMarketEmpty());
}

// ---------------------------------------------------------------------------
// MarketState: processOrderResponse (unsuccessful)
// ---------------------------------------------------------------------------

TEST(MarketStateTests, OrderResponseUnsuccessfulRemovesInFlightOnly) {
  MarketState state(noopKafkaSend);
  MarketOrder order("AAPL", 10, 1000, true);
  state.processOrderToMarket(1, order, nullptr, 0);

  MarketProto::OrderResponseMSG resp;
  resp.set_successful(false);
  state.processOrderResponse(1, resp);

  EXPECT_FALSE(state.hasOrderInFlight(1));
  EXPECT_TRUE(state.ordersOnMarketEmpty());
}

// ---------------------------------------------------------------------------
// MarketState: processOrderResponse (partial fill)
// ---------------------------------------------------------------------------

TEST(MarketStateTests, OrderResponsePartialFillMovesToMarket) {
  MarketState state(noopKafkaSend);
  MarketOrder order("AAPL", 10, 1000, true);
  state.processOrderToMarket(1, order, nullptr, 0);

  MarketProto::OrderResponseMSG resp;
  resp.set_successful(true);
  resp.set_amountfilled(3); // 3 of 10 filled immediately
  resp.set_orderid(55);     // resting order id
  resp.set_price(3000);

  state.processOrderResponse(1, resp);

  EXPECT_FALSE(state.hasOrderInFlight(1));
  EXPECT_TRUE(state.hasOrderOnMarket(55));
  EXPECT_EQ(state.getOrderFilled(55), 3);
}

// ---------------------------------------------------------------------------
// MarketState: processOrderFill (partial — order stays)
// ---------------------------------------------------------------------------

TEST(MarketStateTests, OrderFillPartialUpdatesCount) {
  MarketState state(noopKafkaSend);
  MarketOrder order("AAPL", 10, 1000, true);
  state.processOrderToMarket(1, order, nullptr, 0);

  // Partial response: 3 filled at once, 7 rest on market with id=77
  MarketProto::OrderResponseMSG resp;
  resp.set_successful(true);
  resp.set_amountfilled(3);
  resp.set_orderid(77);
  resp.set_price(3000);
  state.processOrderResponse(1, resp);

  // Fill 4 more (total = 7, still < 10)
  MarketProto::OrderFillMSG fillMsg;
  fillMsg.set_orderid(77);
  fillMsg.set_filled(4);
  state.processOrderFill(fillMsg);

  EXPECT_TRUE(state.hasOrderOnMarket(77));
  EXPECT_EQ(state.getOrderFilled(77), 7);
}

// ---------------------------------------------------------------------------
// MarketState: processOrderFill (completing fill — order removed)
// ---------------------------------------------------------------------------

TEST(MarketStateTests, OrderFillCompletingRemovesOrder) {
  MarketState state(noopKafkaSend);
  MarketOrder order("AAPL", 10, 1000, true);
  state.processOrderToMarket(1, order, nullptr, 0);

  // 3 filled immediately, 7 rest on market with id=88
  MarketProto::OrderResponseMSG resp;
  resp.set_successful(true);
  resp.set_amountfilled(3);
  resp.set_orderid(88);
  resp.set_price(3000);
  state.processOrderResponse(1, resp);

  // Final fill: the remaining 7 shares
  MarketProto::OrderFillMSG fillMsg;
  fillMsg.set_orderid(88);
  fillMsg.set_filled(7);
  state.processOrderFill(fillMsg);

  EXPECT_FALSE(state.hasOrderOnMarket(88));
}

// ---------------------------------------------------------------------------
// MarketState: concurrent response and fill
// ---------------------------------------------------------------------------

TEST(MarketStateTests, ConcurrentResponseAndFill) {
  // processOrderFill calls d_kafkaSend(OrderFill) *before* its spin-wait.
  // We hook that send to signal Thread A so it only calls processOrderResponse
  // after Thread B has committed to the spin-wait path.
  std::promise<void> fillKafkaDone;
  auto fillKafkaDoneFuture = fillKafkaDone.get_future();
  std::once_flag signalOnce;
  KafkaSendFn syncedSend = [&](const void *, std::size_t,
                                const kafka::Topic &topic) {
    if (topic == KafkaTopic::OrderFill)
      std::call_once(signalOnce, [&] { fillKafkaDone.set_value(); });
  };

  MarketState state(syncedSend);
  MarketOrder order("AAPL", 10, 1000, true);
  state.processOrderToMarket(1, order, nullptr, 0);

  // Response: 0 immediately filled, order rests with id=99
  MarketProto::OrderResponseMSG resp;
  resp.set_successful(true);
  resp.set_amountfilled(0);
  resp.set_orderid(99);
  resp.set_price(0);

  // Fill: all 10 shares
  MarketProto::OrderFillMSG fillMsg;
  fillMsg.set_orderid(99);
  fillMsg.set_filled(10);

  // Thread A waits until Thread B has done its Kafka send (just before spin-wait).
  std::thread threadA([&]() {
    fillKafkaDoneFuture.wait();
    state.processOrderResponse(1, resp);
  });

  std::thread threadB([&]() { state.processOrderFill(fillMsg); });

  threadA.join();
  threadB.join();

  // After both complete: order fully filled, removed from market.
  EXPECT_FALSE(state.hasOrderOnMarket(99));
  EXPECT_FALSE(state.hasOrderInFlight(1));
}
