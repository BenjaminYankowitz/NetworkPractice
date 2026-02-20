add_library(KafkaTopics INTERFACE kafkaTopics.h)
target_include_directories(KafkaTopics INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(readAllKafka readAllKafka.cpp)
target_link_libraries(readAllKafka PkgConfig::RDKAFKA PrintMarketProto KafkaTopics)

add_executable(deleteAllKafka deleteAllKafka.cpp)
target_link_libraries(deleteAllKafka PkgConfig::RDKAFKA)
