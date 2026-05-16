// C++ path benchmark: raw RNTupleReader loop vs RAG ReadAll.
//
// Three fixture sizes injected at compile time:
//   FIXTURE_SMALL   (~100 MB, 2 M rows)
//   FIXTURE_MEDIUM  (~500 MB, 10 M rows)
//   FIXTURE_LARGE   (~1 GB,  20 M rows)
//
// Run:
//   ./bench_cpp_path --benchmark_format=csv --benchmark_out=results/cpp_path.csv
//   ./bench_cpp_path --benchmark_format=json --benchmark_out=results/cpp_path.json

#include <benchmark/benchmark.h>
#include <rag/reader.hpp>

#include <arrow/array.h>
#include <arrow/chunked_array.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <ROOT/RNTupleReader.hxx>

#include <cstdint>
#include <vector>

static std::int64_t TableBytes(const arrow::Table& t) {
    std::int64_t total = 0;
    for (int c = 0; c < t.num_columns(); ++c)
        for (auto& chunk : t.column(c)->chunks())
            for (auto& buf : chunk->data()->buffers)
                if (buf) total += buf->size();
    return total;
}

static std::int64_t BatchBytes(const arrow::RecordBatch& b) {
    std::int64_t total = 0;
    for (int c = 0; c < b.num_columns(); ++c)
        for (auto& buf : b.column_data(c)->buffers)
            if (buf) total += buf->size();
    return total;
}

static const char* kFixtures[3] = {
    FIXTURE_SMALL,
    FIXTURE_MEDIUM,
    FIXTURE_LARGE,
};
static const char* kLabels[3] = { "100MB", "500MB", "1GB" };

// ── Baseline: raw RNTupleReader, GetView per column, iterate all entries ───────
// This mirrors what a physicist would write today with no Arrow dependency.
static void BM_RawRNTuple(benchmark::State& state) {
    const char* path = kFixtures[state.range(0)];
    std::int64_t bytes_per_iter = 0;
    for (auto _ : state) {
        auto reader = ROOT::RNTupleReader::Open("bench", path);

        auto vi32  = reader->GetView<std::int32_t>("i32");
        auto vi64  = reader->GetView<std::int64_t>("i64");
        auto vf32  = reader->GetView<float>("f32");
        auto vf64  = reader->GetView<double>("f64");
        auto vb    = reader->GetView<bool>("b");
        auto vvi32 = reader->GetView<std::vector<std::int32_t>>("vi32");
        auto vvf32 = reader->GetView<std::vector<float>>("vf32");

        std::int64_t n = static_cast<std::int64_t>(reader->GetNEntries());
        std::int64_t ichecksum = 0;
        double       fchecksum = 0.0;
        std::int64_t bytes     = 0;
        for (std::int64_t i = 0; i < n; ++i) {
            ichecksum += vi32(i);                          bytes += 4;
            ichecksum += vi64(i);                          bytes += 8;
            fchecksum += vf32(i);                          bytes += 4;
            fchecksum += vf64(i);                          bytes += 8;
            ichecksum += static_cast<int>(vb(i));          bytes += 1;
            for (auto v : vvi32(i)) { ichecksum += v;      bytes += 4; }
            for (auto v : vvf32(i)) { fchecksum += v;      bytes += 4; }
        }
        benchmark::DoNotOptimize(ichecksum);
        benchmark::DoNotOptimize(fchecksum);
        bytes_per_iter = bytes;  // same every iteration; overwrite is correct
    }
    state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) * bytes_per_iter);
    state.SetLabel(kLabels[state.range(0)]);
}
BENCHMARK(BM_RawRNTuple)
    ->Arg(0)->Arg(1)->Arg(2)
    ->Unit(benchmark::kMillisecond)
    ->MinTime(5.0);

// ── RAG ReadAll: Open + ReadAll → arrow::Table ────────────────────────────────
static void BM_RAGReadAll(benchmark::State& state) {
    const char* path = kFixtures[state.range(0)];
    std::int64_t total_bytes = 0;
    for (auto _ : state) {
        auto file  = rag::RNTupleFile::Open(path, "bench").ValueOrDie();
        auto table = file->ReadAll().ValueOrDie();
        benchmark::DoNotOptimize(table.get());
        total_bytes = TableBytes(*table);
    }
    state.SetBytesProcessed(
        static_cast<std::int64_t>(state.iterations()) * total_bytes);
    state.SetLabel(kLabels[state.range(0)]);
}
BENCHMARK(BM_RAGReadAll)
    ->Arg(0)->Arg(1)->Arg(2)
    ->Unit(benchmark::kMillisecond)
    ->MinTime(5.0);

// ── RAG streaming: Open + NextBatch loop, count rows ─────────────────────────
static void BM_RAGStreaming(benchmark::State& state) {
    const char* path = kFixtures[state.range(0)];
    std::int64_t total_bytes = 0;
    for (auto _ : state) {
        auto file = rag::RNTupleFile::Open(path, "bench").ValueOrDie();
        std::int64_t bytes = 0;
        while (true) {
            auto batch = file->NextBatch().ValueOrDie();
            if (!batch) break;
            bytes += BatchBytes(*batch);
            benchmark::DoNotOptimize(batch.get());
        }
        total_bytes = bytes;
    }
    state.SetBytesProcessed(
        static_cast<std::int64_t>(state.iterations()) * total_bytes);
    state.SetLabel(kLabels[state.range(0)]);
}
BENCHMARK(BM_RAGStreaming)
    ->Arg(0)->Arg(1)->Arg(2)
    ->Unit(benchmark::kMillisecond)
    ->MinTime(5.0);

// ── Experimental: RAG ReadAllBulk — CreateBulk + AdoptBuffer per cluster ─────
// Primitives (i32/i64/f32/f64) bulk-read; bool and list columns fall back to
// the per-entry path. Apples-to-apples vs BM_RAGReadAll (same schema, same data).
static void BM_RAGReadAllBulk(benchmark::State& state) {
    const char* path = kFixtures[state.range(0)];
    std::int64_t total_bytes = 0;
    for (auto _ : state) {
        auto file  = rag::RNTupleFile::Open(path, "bench").ValueOrDie();
        auto table = file->ReadAllBulk().ValueOrDie();
        benchmark::DoNotOptimize(table.get());
        total_bytes = TableBytes(*table);
    }
    state.SetBytesProcessed(
        static_cast<std::int64_t>(state.iterations()) * total_bytes);
    state.SetLabel(kLabels[state.range(0)]);
}
BENCHMARK(BM_RAGReadAllBulk)
    ->Arg(0)->Arg(1)->Arg(2)
    ->Unit(benchmark::kMillisecond)
    ->MinTime(5.0);
