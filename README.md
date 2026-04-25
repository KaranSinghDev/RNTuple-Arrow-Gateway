# rntuple-arrow-gateway

A C++ library and server to read CERN's ROOT **RNTuple** physics data files and expose the data as **Apache Arrow** — both in-process to Python, and over the network through an **Arrow Flight** (gRPC) streaming server.

It's a v0.1 prototype, not a production library yet. The C++ engine and the Flight server work; the Python `pybind11` path is also working and correctness-verified, but it's slower than `uproot` right now — that's the main thing I'm still working on.

---

## Why Specifically build this ?

High-energy physics experiments at CERN (ATLAS, CMS, LHCb, ALICE) produce petabytes of detector data per year, and that volume is going to grow a lot once the High-Luminosity LHC's Run 4 starts in June 2030.

For the last ~25 years that data has lived in ROOT's `TTree` format. CERN's response to the HL-LHC data problem is **RNTuple**, a strictly columnar successor: the on-disk binary format was finalised in ROOT 6.34 (Nov 2024) and the production API shipped in ROOT 6.36 (May 2025).

When I went looking for a clean way to read RNTuple data into the Apache Arrow ecosystem (pandas, Spark, DuckDB, and basically every modern data tool), I found two things:

- No standalone, experiment-neutral C++ library that reads RNTuple and produces Arrow tables. ALICE's O2 framework has internal code that does this (see [Prior art](#prior-art)), but it's coupled to O2 and not reusable outside that experiment.
- No Arrow Flight server for RNTuple data anywhere I could find.

This project is my attempt to build both.

---

## What it does today

- Reads RNTuple files using the official ROOT C++ API (6.36+).
- Converts columns into `arrow::RecordBatch` / `arrow::Table` in memory.
- Serves the data over the network as an Arrow Flight gRPC stream — any Arrow-aware client (Python, C++, Go, Java) can connect and read without ROOT installed. **This is the part I'm most happy with so far.**
- Exposes the same engine through a `pybind11` module that returns a `pyarrow.Table` directly. It's correct but slower than `uproot.arrays(library="ak") + ak.to_arrow()` on what I've tested — closing that gap is the next thing on my list.

Types covered: `int32`, `int64`, `float`, `double`, `bool`, and single-level `std::vector<T>` of those. Nested-of-nested (`vector<vector<T>>`) is on the roadmap.

---

## Architecture

```
┌──────────────────────────┐
│  file.root (RNTuple)     │
└────────────┬─────────────┘
             ▼
┌────────────────────────────────────────────────┐
│  C++ engine (librag)                           │
│  RNTupleReader → schema mapper → BatchBuilder  │
└────────────┬───────────────────────────────────┘
      ┌──────┴───────┐
      ▼              ▼
┌───────────┐  ┌──────────────────────────┐
│ pybind11  │  │ Arrow Flight gRPC server │
└───────────┘  └──────────────────────────┘
```

The engine is the only reusable piece; the two sinks are thin wrappers over the same C++ API, which means the same bytes flow through all three paths.

---

## Benchmarks

I ran these on a WSL2 host (12th Gen Intel i7-12700H, 7.6 GB RAM cap), so I'd treat them as indicative rather than definitive — a bare-metal re-run is on the list before I claim anything stronger. Full results: [`benchmark/results/`](benchmark/results/).

What I found:

- **C++ path.** My `ReadAll` is **1.45× / 1.52× / 1.84× slower** than a raw `RNTupleReader` loop at 100 MB / 500 MB / 1 GB. I set myself a `< 2×` target; this clears it.
- **Arrow Flight (localhost).** **~4% overhead** vs in-process at 100 MB; within measurement noise at 500 MB. Flight looks like a perfectly fine transport for this kind of data.
- **Python path.** `pybind11` route is **2.7× / 3.2× slower** than `uproot + ak.to_arrow()` at 100 MB / 500 MB. That's the negative result I want to be upfront about — uproot's hot path is heavily optimised (compiled C++/Cython via Awkward + AwkwardForth), and I haven't profiled my code yet to find where the 2.7× actually goes.

All paths are verified column-for-column against uproot for correctness.

---

## Building

### With Docker (recommended)

```bash
docker build -f docker/dev.Dockerfile -t rag-env:dev .

docker run --rm -v "$(pwd)":/workspace -w /workspace rag-env:dev bash -lc "
  cmake --preset release &&
  cmake --build --preset release --parallel 2 &&
  ctest --test-dir build/release --output-on-failure
"
```

### Without Docker

You'll need: CMake ≥ 3.22, ROOT 6.36+, Apache Arrow C++ 19.0.0 with Flight, pybind11, Python 3 + `pyarrow` + `pytest`.

```bash
cmake --preset release
cmake --build --preset release --parallel 2
ctest --test-dir build/release --output-on-failure
```

Running the Flight server against a built tree:

```bash
./build/release/flight_server/rag-flight-server \
    --file path/to/data.root --ntuple events --port 9090
```

---

## What's next

Rough priority order :

- Profile the Python path and try to close the gap with `uproot`.
- Multi-stream Flight (`GetFlightInfo` returning N endpoints for parallel reads).
- Validate against a real CMS / ATLAS RNTuple sample.
- Support nested-of-nested types.
- Re-run benchmarks on a bare-metal Linux host with more RAM.

---

## Prior art

The closest existing work I found is **`RNTuplePlugin.cxx`** in [`AliceO2Group/AliceO2`](https://github.com/AliceO2Group/AliceO2) — it implements RNTuple as an `arrow::dataset::FileFormat` inside ALICE's framework. That code already shows the RNTuple → Arrow translation is sound; what I'm trying to do is extract and generalise the same idea into a standalone library plus a Flight server, so that it's usable without depending on O2.

---

## References

- ROOT RNTuple on-disk format — https://root.cern/blog/rntuple-binary-format/
- ROOT 6.36 release notes — https://root.cern/doc/v636/release-notes.html
- ATLAS RNTuple adoption (CHEP 2025) — https://www.epj-conferences.org/articles/epjconf/pdf/2025/22/epjconf_chep2025_01056.pdf
- Apache Arrow C++ — https://arrow.apache.org/docs/cpp/
- Arrow Flight RPC — https://arrow.apache.org/docs/format/Flight.html
- ALICE O2 analysis framework — https://aliceo2group.github.io/analysis-framework/docs/
- Uproot — https://uproot.readthedocs.io/

---

## Disclaimer

This is an independent, third-party project. It is not affiliated with, endorsed by, or produced in collaboration with CERN, the ROOT team, the Apache Arrow project, or any LHC experiment (ATLAS, CMS, LHCb, ALICE).

---

## License

Apache License, Version 2.0. See [LICENSE](LICENSE).
