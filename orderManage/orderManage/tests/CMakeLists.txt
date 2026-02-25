find_package(GTest CONFIG REQUIRED)

add_executable(orderManageTests test_market_state.cpp)
set_property(TARGET orderManageTests PROPERTY CXX_STANDARD 17)

target_link_libraries(orderManageTests PRIVATE
    MarketConnect
    GTest::gtest_main
    PrintMarketProto
)

include(GoogleTest)
gtest_discover_tests(orderManageTests)
