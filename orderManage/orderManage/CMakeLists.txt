find_package(rmqcpp REQUIRED)

add_library(MarketConnect marketState.cpp)
set_property(TARGET MarketConnect PROPERTY CXX_STANDARD 17)
target_link_libraries(MarketConnect PUBLIC
    MarketProto rmqcpp::rmq PkgConfig::RDKAFKA PrintMarketProto KafkaTopics)

add_executable(orderManage main.cpp)
set_property(TARGET orderManage PROPERTY CXX_STANDARD 17)
target_link_libraries(orderManage PRIVATE MarketConnect)

add_subdirectory(tests)
