add_library(rag_engine INTERFACE)
add_library(rag::engine ALIAS rag_engine)

target_include_directories(rag_engine INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

if(RAG_BUILD_TESTS)
    add_subdirectory(tests)
endif()
