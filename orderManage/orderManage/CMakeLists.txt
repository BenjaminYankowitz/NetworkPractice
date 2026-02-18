find_package(rmqcpp REQUIRED)

add_executable(orderManage main.cpp)
set_property(TARGET orderManage PROPERTY CXX_STANDARD 17)
target_link_libraries(orderManage PRIVATE MarketProto rmqcpp::rmq PkgConfig::RDKAFKA PrintMarketProto)
