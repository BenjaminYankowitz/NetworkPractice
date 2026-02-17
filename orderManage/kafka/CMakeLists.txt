add_executable(readKafkaAsProduced readKafkaAsProduced.cpp)
target_link_libraries(readKafkaAsProduced PkgConfig::RDKAFKA)

add_executable(readAllKafka readAllKafka.cpp)
target_link_libraries(readAllKafka PkgConfig::RDKAFKA)
