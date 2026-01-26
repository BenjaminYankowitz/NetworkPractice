cmake_minimum_required(VERSION 3.25)
project(OrderManage LANGUAGES CXX PYTHON)

find_package(Protobuf CONFIG REQUIRED)
add_subdirectory(exchange)
add_subdirectory(orderManage)
