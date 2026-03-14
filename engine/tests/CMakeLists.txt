enable_testing()
include(GoogleTest)

add_executable(test_hello test_hello.cpp)
target_link_libraries(test_hello PRIVATE GTest::gtest_main rag::engine)
rag_set_compile_options(test_hello)
gtest_discover_tests(test_hello)
