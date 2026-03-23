add_library(rag_engine SHARED
    src/schema.cpp
    src/batch_builder.cpp
    src/reader.cpp
)
add_library(rag::engine ALIAS rag_engine)

target_include_directories(rag_engine
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(rag_engine
    PUBLIC
        Arrow::arrow_shared
        ROOT::ROOTNTuple
)

rag_set_compile_options(rag_engine)

if(RAG_BUILD_TESTS)
    add_subdirectory(tests)
endif()
