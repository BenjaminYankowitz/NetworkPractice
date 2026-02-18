add_executable(readAllKafka readAllKafka.cpp)
target_link_libraries(readAllKafka PkgConfig::RDKAFKA PrintMarketProto)
