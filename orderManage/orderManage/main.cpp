#include "marketConnect.h"
#include <bsl_optional.h>
#include <chrono>
#include <iostream>
#include <rmqa_connectionstring.h>
#include <rmqa_rabbitcontext.h>
#include <rmqt_vhostinfo.h>
#include <string>
#include <thread>
using namespace BloombergLP;

int main(int argc, char **argv) {
  AtomicCounter correlationIdGenerator;
  MarketState marketState;
  const char *const amqpuri = "amqp://localhost";
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

  bsl::shared_ptr<rmqa::VHost> vhost =
      rabbit.createVHostConnection(username, vhostInfo.value());
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
  kafka::Properties props({{"bootstrap.servers", {"localhost:9092"}},
                           {"enable.idempotence", {"true"}}});
  kafka::clients::producer::KafkaProducer kafkaProducer(props);
  const auto numId =
      registerWithMarket(listenStuff, *producer, kafkaProducer, username);
  std::cout << numId << '\n';
  if (numId == failureId) {
    std::cout << "Failed to register with market\n";
    return 1;
  }
  const auto id = bsl::to_string(numId);
  auto listenHandles = startListeningToOrderResponses(
      listenStuff, kafkaProducer, id, marketState);
  if (!(listenHandles.responseConsumer && listenHandles.fillConsumer)) {
    return 1;
  }
  sendOrderToMarket(*producer, kafkaProducer, id, correlationIdGenerator,
                    MarketOrder("slugs", 10, 1000, false), marketState);
  sendOrderToMarket(*producer, kafkaProducer, id, correlationIdGenerator,
                    MarketOrder("slugs", 10, 1000, true), marketState);
  std::cout << "Market confirmed\n";
  while (true) {
    std::this_thread::sleep_for(std::chrono::seconds(5));
  }
  return 0;
}
