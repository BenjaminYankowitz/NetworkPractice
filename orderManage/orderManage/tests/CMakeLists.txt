find_package(GTest CONFIG REQUIRED)

add_executable(orderManageTests test_market_state.cpp)
set_property(TARGET orderManageTests PROPERTY CXX_STANDARD 17)

# Include the parent directory so "marketState.h" is found.
target_include_directories(orderManageTests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)

target_link_libraries(orderManageTests PRIVATE
    GTest::gtest_main
    PrintMarketProto
    KafkaTopics
    PkgConfig::RDKAFKA
)

include(GoogleTest)
gtest_discover_tests(orderManageTests)
