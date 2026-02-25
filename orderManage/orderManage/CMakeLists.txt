find_package(rmqcpp REQUIRED)

add_library(MarketConnect)
target_sources(MarketConnect 
PUBLIC FILE_SET HEADERS FILES marketState.h marketConnect.h
PRIVATE marketState.cpp)
set_property(TARGET MarketConnect PROPERTY CXX_STANDARD 17)
target_link_libraries(MarketConnect PUBLIC
    MarketProto rmqcpp::rmq PkgConfig::RDKAFKA PrintMarketProto OrderManage::KafkaTopics)

add_executable(orderManage main.cpp)
set_property(TARGET orderManage PROPERTY CXX_STANDARD 17)
target_link_libraries(orderManage PRIVATE MarketConnect)

add_subdirectory(tests)
