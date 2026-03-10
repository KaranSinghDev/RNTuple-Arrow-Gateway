find_package(ROOT 6.36 CONFIG REQUIRED COMPONENTS Core ROOTNTuple)

find_package(Arrow CONFIG REQUIRED)

if(RAG_BUILD_FLIGHT)
  find_package(ArrowFlight CONFIG REQUIRED)
endif()

if(RAG_BUILD_PYTHON)
  find_package(pybind11 CONFIG REQUIRED)
endif()

if(RAG_BUILD_TESTS)
  find_package(GTest CONFIG REQUIRED)
endif()

if(RAG_BUILD_BENCHMARK)
  find_package(benchmark CONFIG REQUIRED)
endif()
