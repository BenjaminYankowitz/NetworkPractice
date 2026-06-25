set(PROTO_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../proto")

add_library(MarketProto)
target_sources(MarketProto
INTERFACE FILE_SET HEADERS BASE_DIRS ${CMAKE_CURRENT_BINARY_DIR}
FILES ${CMAKE_CURRENT_BINARY_DIR}/marketMessages.pb.h
PUBLIC ${PROTO_DIR}/marketMessages.proto)
target_link_libraries(MarketProto
    PUBLIC
        protobuf::libprotobuf)
protobuf_generate(TARGET MarketProto LANGUAGE cpp IMPORT_DIRS ${PROTO_DIR})

add_library(LogProto)
target_sources(LogProto
INTERFACE FILE_SET HEADERS BASE_DIRS ${CMAKE_CURRENT_BINARY_DIR}
FILES ${CMAKE_CURRENT_BINARY_DIR}/logMessages.pb.h
PUBLIC ${PROTO_DIR}/logMessages.proto)
target_link_libraries(LogProto
    PUBLIC
        MarketProto protobuf::libprotobuf)
protobuf_generate(TARGET LogProto LANGUAGE cpp IMPORT_DIRS ${PROTO_DIR})

add_library(PrintMarketProto INTERFACE)
target_sources(PrintMarketProto INTERFACE FILE_SET HEADERS FILES printMarketProto.h)
target_link_libraries(PrintMarketProto INTERFACE MarketProto)

add_library(PrintLogProto INTERFACE)
target_sources(PrintLogProto INTERFACE FILE_SET HEADERS FILES printLogProto.h)
target_link_libraries(PrintLogProto INTERFACE LogProto PrintMarketProto)