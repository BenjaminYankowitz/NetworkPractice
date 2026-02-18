#include "marketMessages.pb.h"
#include "printMarketProto.h"
#include <csignal>
#include <iostream>
#include <kafka/KafkaConsumer.h>
#include <kafka/Types.h>
#include <string>
#include <string_view>
static std::atomic_bool running = true;

static void sigterm(int) { running = false; }

namespace KafkaTopic { // create new header file shared with order manage
static const kafka::Topic Signup = "signup";
static const kafka::Topic SignupResponse = "signup-response";
static const kafka::Topic Order = "order";
static const kafka::Topic OrderResponse = "order-response";
static const kafka::Topic OrderFill = "order-fill";
} // namespace KafkaTopic

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

void printValue(const std::string_view &key, kafka::ConstBuffer value) {
  using namespace KafkaTopic;
  if (key == Signup) {
    printValue<SignupMSG>(value);
  } else if (key == SignupResponse) {
    printValue<SignupResponseMSG>(value);
  } else if (key == Order) {
    printValue<OrderMSG>(value);
  } else if (key == OrderResponse) {
    printValue<OrderResponseMSG>(value);
  } else if (key == OrderFill) {
    printValue<OrderFillMSG>(value);
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
      const bool newMessage =
          record.offset() >=
          endOffsets.at(std::make_pair(record.topic(), record.partition()));
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
