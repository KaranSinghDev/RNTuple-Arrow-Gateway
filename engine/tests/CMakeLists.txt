include(GoogleTest)

# Scaffold smoke test (carried forward from M0)
add_executable(test_hello test_hello.cpp)
target_link_libraries(test_hello PRIVATE GTest::gtest_main rag::engine)
rag_set_compile_options(test_hello)
gtest_discover_tests(test_hello)

# Fixture generator — produces a small .root file used by all engine tests.
add_executable(gen_fixture data/gen_fixture.cpp)
target_link_libraries(gen_fixture PRIVATE ROOT::ROOTNTuple)
rag_set_compile_options(gen_fixture)

set(FIXTURE_PATH "${CMAKE_CURRENT_BINARY_DIR}/fixture.root")
set(FIXTURE_ROWS 1000)

add_custom_command(
    OUTPUT  "${FIXTURE_PATH}"
    COMMAND gen_fixture "${FIXTURE_PATH}" "${FIXTURE_ROWS}"
    DEPENDS gen_fixture
    COMMENT "Generating RNTuple fixture (${FIXTURE_ROWS} rows)"
)
add_custom_target(engine_fixture ALL DEPENDS "${FIXTURE_PATH}")

# Schema tests
add_executable(test_schema test_schema.cpp)
target_link_libraries(test_schema PRIVATE GTest::gtest_main rag::engine)
target_compile_definitions(test_schema PRIVATE
    FIXTURE_PATH="${FIXTURE_PATH}")
rag_set_compile_options(test_schema)
add_dependencies(test_schema engine_fixture)
gtest_discover_tests(test_schema)

# Batch builder / round-trip tests
add_executable(test_batch_builder test_batch_builder.cpp)
target_link_libraries(test_batch_builder PRIVATE GTest::gtest_main rag::engine)
target_compile_definitions(test_batch_builder PRIVATE
    FIXTURE_PATH="${FIXTURE_PATH}"
    FIXTURE_ROWS=${FIXTURE_ROWS})
rag_set_compile_options(test_batch_builder)
add_dependencies(test_batch_builder engine_fixture)
gtest_discover_tests(test_batch_builder)
