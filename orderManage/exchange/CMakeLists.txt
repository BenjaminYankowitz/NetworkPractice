add_library(ExchangeProto STATIC exchangeMessages.proto)

target_include_directories(ExchangeProto INTERFACE ${CMAKE_CURRENT_BINARY_DIR})
protobuf_generate(TARGET ExchangeProto LANGUAGE python PROTOC_OUT_DIR ${CMAKE_CURRENT_SOURCE_DIR})
protobuf_generate(TARGET ExchangeProto LANGUAGE cpp)

# set_property(TARGET orderManage PROPERTY CXX_STANDARD 17)