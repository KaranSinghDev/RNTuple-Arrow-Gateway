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
endif()
