find_package(benchmark CONFIG REQUIRED)

# ── Fixture generator ──────────────────────────────────────────────────────────
add_executable(gen_bench_fixture data/gen_bench_fixture.cpp)
target_link_libraries(gen_bench_fixture PRIVATE ROOT::ROOTNTuple)
rag_set_compile_options(gen_bench_fixture)

set(BENCH_DATA_DIR "${CMAKE_CURRENT_BINARY_DIR}/data")
file(MAKE_DIRECTORY "${BENCH_DATA_DIR}")

set(FIXTURE_SMALL  "${BENCH_DATA_DIR}/bench_small.root")   # ~100 MB  2 M rows
set(FIXTURE_MEDIUM "${BENCH_DATA_DIR}/bench_medium.root")  # ~500 MB 10 M rows
set(FIXTURE_LARGE  "${BENCH_DATA_DIR}/bench_large.root")   # ~1   GB 20 M rows

set(ROWS_SMALL   2000000)
set(ROWS_MEDIUM 10000000)
set(ROWS_LARGE  20000000)

foreach(SIZE small medium large)
    string(TOUPPER ${SIZE} SZ)
    add_custom_command(
        OUTPUT  "${FIXTURE_${SZ}}"
        COMMAND gen_bench_fixture "${FIXTURE_${SZ}}" "${ROWS_${SZ}}"
        DEPENDS gen_bench_fixture
        COMMENT "Generating bench fixture (${SIZE}: ${ROWS_${SZ}} rows)"
    )
    add_custom_target(bench_fixture_${SIZE} DEPENDS "${FIXTURE_${SZ}}")
endforeach()

add_custom_target(bench_fixtures
    DEPENDS bench_fixture_small bench_fixture_medium bench_fixture_large)

# ── C++ path benchmark ─────────────────────────────────────────────────────────
add_executable(bench_cpp_path bench_cpp_path.cpp)
target_link_libraries(bench_cpp_path PRIVATE
    rag::engine
    benchmark::benchmark
    benchmark::benchmark_main
)
target_compile_definitions(bench_cpp_path PRIVATE
    FIXTURE_SMALL="${FIXTURE_SMALL}"
    FIXTURE_MEDIUM="${FIXTURE_MEDIUM}"
    FIXTURE_LARGE="${FIXTURE_LARGE}"
)
rag_set_compile_options(bench_cpp_path)
add_dependencies(bench_cpp_path bench_fixtures)

# ── Python / Flight benchmarks (run manually via run_benchmarks.sh) ────────────
if(RAG_BUILD_PYTHON)
    find_package(Python3 REQUIRED COMPONENTS Interpreter)

    set(RESULTS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/results")
    file(MAKE_DIRECTORY "${RESULTS_DIR}")

    # Expose paths so the shell script can forward them as env vars.
    configure_file(
        "${CMAKE_CURRENT_SOURCE_DIR}/run_benchmarks.sh.in"
        "${CMAKE_CURRENT_BINARY_DIR}/run_benchmarks.sh"
        @ONLY
    )
endif()
