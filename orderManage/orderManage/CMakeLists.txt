find_package(rmqcpp REQUIRED)

target_link_libraries(MarketProto
    PUBLIC
        protobuf::libprotobuf
)

add_executable(orderManage main.cpp)
set_property(TARGET orderManage PROPERTY CXX_STANDARD 17)
target_link_libraries(orderManage PRIVATE MarketProto rmqcpp::rmq PkgConfig::RDKAFKA)
