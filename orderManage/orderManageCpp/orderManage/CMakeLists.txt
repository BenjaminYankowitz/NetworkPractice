add_library(MarketConnect)
target_sources(MarketConnect
PUBLIC FILE_SET HEADERS FILES marketState.h marketConnect.h
PRIVATE marketState.cpp)
target_link_libraries(MarketConnect PUBLIC
    rmqcpp::rmq unofficial::modern-cpp-kafka::modern-cpp-kafka PrintMarketProto OrderManage::KafkaTopics LogProto)

add_executable(orderManage main.cpp)
target_link_libraries(orderManage PRIVATE MarketConnect)

add_subdirectory(tests)
