cmake_minimum_required(VERSION 3.22)
project(RAG_Gateway VERSION 0.1.0 LANGUAGES CXX)

option(RAG_BUILD_TESTS     "Build unit tests"           ON)
option(RAG_BUILD_PYTHON    "Build Python bindings"      OFF)
option(RAG_BUILD_FLIGHT    "Build Arrow Flight server"  OFF)
option(RAG_BUILD_BENCHMARK "Build benchmarks"           OFF)
option(RAG_WERROR          "Treat warnings as errors"   OFF)
option(RAG_SANITIZE        "Enable ASan + UBSan"        OFF)

include(cmake/RAGCompileOptions.cmake)
include(cmake/RAGDependencies.cmake)

add_subdirectory(engine)

if(RAG_BUILD_FLIGHT)
  add_subdirectory(flight_server)
endif()

if(RAG_BUILD_PYTHON)
  add_subdirectory(python)
endif()

if(RAG_BUILD_BENCHMARK)
  add_subdirectory(benchmark)
endif()
