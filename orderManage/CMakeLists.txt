cmake_minimum_required(VERSION 4.2.3)
project(OrderManage LANGUAGES CXX)

enable_testing()
find_package(Protobuf CONFIG REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(RDKAFKA REQUIRED IMPORTED_TARGET rdkafka)

# rmqcpp is vendored as a git submodule. Only src/rmq is added (not the
# upstream top-level CMakeLists) to skip its examples, tests, and mocks,
# so the find_package calls upstream does in its parent dirs happen here.
set(ENABLE_COMPRESSION OFF)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/external/rmqcpp/cmake")
find_package(Threads REQUIRED)
find_package(ZLIB REQUIRED)
find_package(Boost REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(bal REQUIRED)
add_subdirectory(external/rmqcpp/src/rmq)
add_library(rmqcpp::rmq ALIAS rmq)

add_subdirectory(proto)
add_subdirectory(kafka)
add_subdirectory(market)
add_subdirectory(orderManage)