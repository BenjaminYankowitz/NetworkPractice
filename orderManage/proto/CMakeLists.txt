add_library(MarketProto)
target_sources(MarketProto 
INTERFACE FILE_SET HEADERS BASE_DIRS ${CMAKE_CURRENT_BINARY_DIR} 
FILES ${CMAKE_CURRENT_BINARY_DIR}/marketMessages.pb.h
PRIVATE marketMessages.proto)
protobuf_generate(TARGET MarketProto LANGUAGE cpp)

target_link_libraries(MarketProto
    PUBLIC
        protobuf::libprotobuf
)

add_library(PrintMarketProto INTERFACE)
target_sources(PrintMarketProto INTERFACE FILE_SET HEADERS FILES printMarketProto.h)
target_link_libraries(PrintMarketProto INTERFACE MarketProto)