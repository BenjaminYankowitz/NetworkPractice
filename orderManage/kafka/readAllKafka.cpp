#include "kafkaTopics.h"
#include "marketMessages.pb.h"
#include "printMarketProto.h"
#include <csignal>
#include <iostream>
#include <kafka/Interceptors.h>
#include <kafka/KafkaConsumer.h>
#include <kafka/Types.h>
#include <string>
#include <string_view>
static std::atomic_bool running = true;

static void sigterm(int) { running = false; }

struct Settings {
  bool startFromBeginning = false;
  bool endAtCurrent = false;
};

std::string_view toView(kafka::ConstBuffer buffer) {
  return std::string_view(static_cast<const char *>(buffer.data()),
                          buffer.size());
}

template <class T> void printValue(kafka::ConstBuffer value) {
  T msg;
  msg.ParseFromArray(value.data(), value.size());
  std::cout << msg << '\n';
}

void printValue(const std::string_view &topic, kafka::ConstBuffer value) {
  using namespace KafkaTopic;
  if (topic == Signup) {
    printValue<MarketProto::SignupMSG>(value);
  } else if (topic == SignupResponse) {
    printValue<MarketProto::SignupResponseMSG>(value);
  } else if (topic == Order) {
    printValue<MarketProto::OrderMSG>(value);
  } else if (topic == OrderResponse) {
    printValue<MarketProto::OrderResponseMSG>(value);
  } else if (topic == OrderFill) {
    printValue<MarketProto::OrderFillMSG>(value);
  } else {
    std::cout << "unknown key\n";
  }
}

void printAllMessages(const std::string &brokers, const std::string &group_id,
                      const std::set<std::string> &topics, Settings settings) {
  const kafka::Properties props(
      {{"bootstrap.servers", {brokers}}, {"group.id", {group_id}}});

  kafka::clients::consumer::KafkaConsumer consumer(props);
  consumer.subscribe(topics);
  if (settings.startFromBeginning) {
    consumer.seekToBeginning();
  }
  std::cout << "Consuming from ' ";
  for(const auto& topic : topics){
    std::cout << topic << ' ';
  }
  std::cout << "' (Ctrl-C to quit)...\n";
  auto endOffsets = consumer.endOffsets(consumer.assignment());
  while (running) {
    auto records = consumer.poll(std::chrono::milliseconds(100));
    for (const auto &record : records) {
      const auto endOffset = endOffsets.find(std::make_pair(record.topic(), record.partition()));
      const bool newMessage = endOffset==endOffsets.end() || record.offset() >= endOffset->second;
      if (settings.endAtCurrent && newMessage) {
        continue;
      }
      if (!record.error()) {
        if (newMessage) {
          std::cout << "Got a new message..." << '\n';
        }
        std::cout << '\n';
        std::cout << "    Topic    : " << record.topic() << '\n';
        std::cout << "    Partition: " << record.partition() << '\n';
        std::cout << "    Offset   : " << record.offset() << '\n';
        std::cout << "    Timestamp: " << record.timestamp().toString() << '\n';
        std::cout << "    Headers  : " << toString(record.headers()) << '\n';
        std::cout << "    Key   [" << record.key().toString() << "]" << '\n';
        std::cout << "    Value [\n";
        printValue(record.topic(), record.value());
        std::cout << "    ]\n";
      } else {
        std::cerr << record.toString() << '\n';
      }
    }
    if (settings.endAtCurrent) {
      bool done = true;
      for (const auto &[topicPartition, offset] : endOffsets) {
        auto cOffset = consumer.position(topicPartition);
        if (cOffset < offset) {
          done = false;
          break;
        }
      }
      if (done) {
        running = false;
      }
    }
  }
}


int main(int argc, char **argv) {
  std::string brokers = "localhost:9092";
  std::string group_id = "my-group";
  std::set<kafka::Topic> topics = [](){
    using namespace KafkaTopic;
    return std::set{Signup,SignupResponse,Order,OrderResponse,OrderFill};
  }();
  const Settings settings = [argc, argv]() {
    Settings settings;
    for (int i = 0; i < argc; i++) {
      std::string_view current = argv[i];
      if (current == "-B") {
        settings.startFromBeginning = true;
      } else if (current == "-A") {
        settings.startFromBeginning = true;
        settings.endAtCurrent = true;
      }
    }
    return settings;
  }();
  struct sigaction sa {};
  sa.sa_handler = sigterm;
  sigemptyset(&sa.sa_mask);
  sigaction(SIGINT, &sa, nullptr);
  sigaction(SIGTERM, &sa, nullptr);
  try {
    printAllMessages(brokers,group_id,topics,settings);
  } catch (const kafka::KafkaException &exception) {
    std::cout << "could not subscribe: " << exception.what() << '\n';
    return 1;
  }
  return 0;
}
