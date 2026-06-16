#include <kafka/AdminClient.h>

void clearTopics(const std::string &brokers) {
  const kafka::Properties props({{"bootstrap.servers", {brokers}}});
  kafka::clients::admin::AdminClient admin(props);
  // Delete all topics
  auto topics = admin.listTopics();
  if(topics.error){
    std::cerr << "Topics List failed: " << topics.error.message() << '\n';
  }
  auto deleteResult = admin.deleteTopics(topics.topics);
  if (deleteResult.error) {
    std::cerr << "Delete failed: " << deleteResult.error.message() << '\n';
    return;
  }
  // std::this_thread::sleep_for(std::chrono::seconds(1));

  // // Recreate each topic (numPartitions=1, replicationFactor=1)
  // auto createResult = admin.createTopics(topics, 1, 1);
  // if (createResult.error) {
  //     std::cerr << "Create failed: " << createResult.error.message() << '\n';
  // }
}

int main() {
    clearTopics("localhost:9092");
}