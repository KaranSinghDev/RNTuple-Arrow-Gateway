include(GoogleTest)

set(FIXTURE_PATH "${CMAKE_BINARY_DIR}/engine/tests/fixture.root")

add_executable(test_flight_roundtrip
    test_flight_roundtrip.cpp
    ../service.cpp
)
target_include_directories(test_flight_roundtrip PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)
target_link_libraries(test_flight_roundtrip PRIVATE
    rag::engine
    ArrowFlight::arrow_flight_shared
    GTest::gtest_main
)
target_compile_definitions(test_flight_roundtrip PRIVATE
    FIXTURE_PATH="${FIXTURE_PATH}")
rag_set_compile_options(test_flight_roundtrip)
add_dependencies(test_flight_roundtrip engine_fixture)
gtest_discover_tests(test_flight_roundtrip)
