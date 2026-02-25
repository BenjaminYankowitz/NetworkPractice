add_library(OrderManage.KafkaTopics INTERFACE)
target_sources(OrderManage.KafkaTopics PUBLIC FILE_SET HEADERS FILES kafkaTopics.h)
add_library(OrderManage::KafkaTopics ALIAS OrderManage.KafkaTopics)

add_executable(readAllKafka readAllKafka.cpp)
target_link_libraries(readAllKafka PkgConfig::RDKAFKA PrintMarketProto OrderManage.KafkaTopics)

add_executable(deleteAllKafka deleteAllKafka.cpp)
target_link_libraries(deleteAllKafka PkgConfig::RDKAFKA)
