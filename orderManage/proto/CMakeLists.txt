add_library(MarketProto STATIC marketMessages.proto)
target_include_directories(MarketProto INTERFACE ${CMAKE_CURRENT_BINARY_DIR})
protobuf_generate(TARGET MarketProto LANGUAGE cpp)