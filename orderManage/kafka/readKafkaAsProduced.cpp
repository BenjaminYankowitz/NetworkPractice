#include <iostream>
#include <kafka/Types.h>
#include <string>
#include <csignal>
#include <kafka/KafkaConsumer.h>

static std::atomic_bool running = true;

static void sigterm(int) {
    running = false;
}

int main() {
    std::string brokers = "localhost:9092";
    std::string group_id = "my-group";
    kafka::Topic topic = "test-topic";

    struct sigaction sa{};
    sa.sa_handler = sigterm;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
    const kafka::Properties props(
        {{"bootstrap.servers", {brokers}}, {"group.id", {group_id}}});
    kafka::clients::consumer::KafkaConsumer consumer(props);
    consumer.subscribe({topic});
    std::cout << "Consuming from '" << topic << "' (Ctrl-C to quit)...\n";

    while (running) {
        auto records = consumer.poll(std::chrono::milliseconds(100));
        for (const auto& record: records) {
            if (!record.error()) {
                std::cout << "Got a new message..." << '\n';
                std::cout << "    Topic    : " << record.topic() << '\n';
                std::cout << "    Partition: " << record.partition() << '\n';
                std::cout << "    Offset   : " << record.offset() << '\n';
                std::cout << "    Timestamp: " << record.timestamp().toString() << '\n';
                std::cout << "    Headers  : " << toString(record.headers()) << '\n';
                std::cout << "    Key   [" << record.key().toString() << "]" << '\n';
                std::cout << "    Value [" << record.value().toString() << "]" << '\n';
            } else {
                std::cerr << record.toString() << '\n';
            }
        }
    }
    return 0;
}

