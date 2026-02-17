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
        {{"bootstrap.servers", {brokers}}, {"group.id", {group_id}}, {"enable.idempotence", {"true"}}});
    kafka::clients::consumer::KafkaConsumer consumer(props);
    consumer.subscribe({topic});
    consumer.seekToBeginning();
    std::cout << "Consuming from '" << topic << "' (Ctrl-C to quit)...\n";
    auto endOffsets = consumer.endOffsets(consumer.assignment());
    while (running) {
        auto records = consumer.poll(std::chrono::milliseconds(10));
        for (const auto& record: records) {
            if(record.offset()>endOffsets.at(std::make_pair(record.topic(),record.partition()))){
                continue;
            }
            if (!record.error()) {
                std::cout << '\n';
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
        bool done = true;
        for(const auto& [topicPartition,offset]  : endOffsets){
            auto cOffset = consumer.position(topicPartition);
            if(cOffset<offset){
                done = false;
            }
        }
        if(done){
            running = false;
        }
    }
    return 0;
}

