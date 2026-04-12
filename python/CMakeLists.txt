find_package(Python3 REQUIRED COMPONENTS Interpreter Development)

pybind11_add_module(_native src/bindings.cpp)
target_link_libraries(_native PRIVATE rag::engine Arrow::arrow_shared)
rag_set_compile_options(_native)

# Place _native.so next to __init__.py so the package is importable.
set_target_properties(_native PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/rag_gateway"
)

# Copy the Python shim into the build tree.
configure_file(rag_gateway/__init__.py
               ${CMAKE_CURRENT_BINARY_DIR}/rag_gateway/__init__.py COPYONLY)

# Pytest: run from the build tree so `import rag_gateway` resolves correctly.
set(FIXTURE_PATH "${CMAKE_BINARY_DIR}/engine/tests/fixture.root")

add_test(
    NAME Python.RoundTrip
    COMMAND ${Python3_EXECUTABLE} -m pytest
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_roundtrip.py -v
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)
set_tests_properties(Python.RoundTrip PROPERTIES
    ENVIRONMENT
        "FIXTURE_PATH=${FIXTURE_PATH};FIXTURE_ROWS=1000;PYTHONPATH=${CMAKE_CURRENT_BINARY_DIR}"
)

# List-column tests via pybind11
set(FIXTURE_LISTS_PATH "${CMAKE_BINARY_DIR}/engine/tests/fixture_lists.root")
add_test(
    NAME Python.ListColumns
    COMMAND ${Python3_EXECUTABLE} -m pytest
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_list_columns.py -v
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)
set_tests_properties(Python.ListColumns PROPERTIES
    ENVIRONMENT
        "FIXTURE_LISTS_PATH=${FIXTURE_LISTS_PATH};PYTHONPATH=${CMAKE_CURRENT_BINARY_DIR}"
)
