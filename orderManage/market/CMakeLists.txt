add_library(MarketProto STATIC marketMessages.proto)

target_include_directories(MarketProto INTERFACE ${CMAKE_CURRENT_BINARY_DIR})
protobuf_generate(TARGET MarketProto LANGUAGE python PROTOC_OUT_DIR ${CMAKE_CURRENT_SOURCE_DIR})
protobuf_generate(TARGET MarketProto LANGUAGE cpp)

# set_property(TARGET orderManage PROPERTY CXX_STANDARD 17)