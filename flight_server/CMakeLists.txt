add_executable(rag-flight-server
    main.cpp
    service.cpp
)
target_include_directories(rag-flight-server PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(rag-flight-server PRIVATE
    rag::engine
    ArrowFlight::arrow_flight_shared
)
rag_set_compile_options(rag-flight-server)

if(RAG_BUILD_TESTS)
    add_subdirectory(tests)

    if(RAG_BUILD_PYTHON)
        find_package(Python3 REQUIRED COMPONENTS Interpreter)
        set(FLIGHT_SERVER_PATH "${CMAKE_BINARY_DIR}/flight_server/rag-flight-server")
        set(FIXTURE_PATH_FLIGHT "${CMAKE_BINARY_DIR}/engine/tests/fixture.root")
        set(FIXTURE_LISTS_PATH_FLIGHT "${CMAKE_BINARY_DIR}/engine/tests/fixture_lists.root")

        add_test(
            NAME Python.FlightRoundTrip
            COMMAND ${Python3_EXECUTABLE} -m pytest
                    ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_flight_python.py -v
            WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        )
        set_tests_properties(Python.FlightRoundTrip PROPERTIES
            ENVIRONMENT
                "FLIGHT_SERVER_PATH=${FLIGHT_SERVER_PATH};FIXTURE_PATH=${FIXTURE_PATH_FLIGHT};FIXTURE_ROWS=1000"
        )

        add_test(
            NAME Python.FlightListColumns
            COMMAND ${Python3_EXECUTABLE} -m pytest
                    ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_flight_lists.py -v
            WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        )
        set_tests_properties(Python.FlightListColumns PROPERTIES
            ENVIRONMENT
                "FLIGHT_SERVER_PATH=${FLIGHT_SERVER_PATH};FIXTURE_LISTS_PATH=${FIXTURE_LISTS_PATH_FLIGHT}"
        )
    endif()
endif()
