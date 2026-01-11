cmake_minimum_required(VERSION 3.25)
project(RabbitChat LANGUAGES C CXX)

find_package(rmqcpp REQUIRED)
find_package(Protobuf CONFIG REQUIRED)

add_library(myproto STATIC message.proto )
target_include_directories(myproto INTERFACE ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(myproto
    PUBLIC
        protobuf::libprotobuf
)

protobuf_generate(TARGET myproto LANGUAGE cpp)
add_executable(client client.cpp)
set_property(TARGET client PROPERTY CXX_STANDARD 17)
target_link_libraries(client rmqcpp::rmq myproto)
