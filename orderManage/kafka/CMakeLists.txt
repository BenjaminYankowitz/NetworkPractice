add_executable(readAllKafka readAllKafka.cpp)
target_link_libraries(readAllKafka PkgConfig::RDKAFKA PrintMarketProto)

add_executable(deleteAllKafka deleteAllKafka.cpp)
target_link_libraries(deleteAllKafka PkgConfig::RDKAFKA)
