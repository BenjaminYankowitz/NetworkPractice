find_package(rmqcpp REQUIRED)

target_link_libraries(ExchangeProto
    PUBLIC
        protobuf::libprotobuf
)

add_executable(orderManage main.cpp)
set_property(TARGET orderManage PROPERTY CXX_STANDARD 17)
target_link_libraries(orderManage rmqcpp::rmq ExchangeProto)
