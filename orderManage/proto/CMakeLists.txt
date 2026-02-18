add_library(MarketProto STATIC marketMessages.proto)
target_include_directories(MarketProto INTERFACE ${CMAKE_CURRENT_BINARY_DIR})
protobuf_generate(TARGET MarketProto LANGUAGE cpp)
target_link_libraries(MarketProto
    PUBLIC
        protobuf::libprotobuf
)

add_library(PrintMarketProto INTERFACE printMarketProto.h)
target_link_libraries(PrintMarketProto INTERFACE MarketProto)
target_include_directories(PrintMarketProto
                           INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}
                           )