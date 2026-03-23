# rntuple-arrow-gateway

A C++ library and server that reads CERN's ROOT **RNTuple** physics data files and exposes the data as **Apache Arrow** — both in-process to Python and over the network via an **Arrow Flight** (gRPC) streaming server.

---

## Background and research

High-energy physics experiments at organisations like CERN (ATLAS, CMS, LHCb, ALICE) produce enormous volumes of detector data — currently on the order of petabytes per year, projected to grow significantly with the High-Luminosity LHC starting around 2030.

For ~25 years this data has been stored in ROOT's `TTree` format. In 2024–2025, CERN released **RNTuple** as TTree's strictly columnar successor: the on-disk binary format was finalized in ROOT 6.34 (November 2024) and the production API shipped in ROOT 6.36 (May 2025).

**The gap aimed to fill:** there seems to be no standalone, experiment-neutral C++ library that reads RNTuple data and hands it directly to the [Apache Arrow](https://arrow.apache.org/) ecosystem — the standard columnar in-memory format used by pandas, Spark, DuckDB, and most modern data tooling. Specifically:

- No C++-native RNTuple → Arrow converter exists outside ALICE's internal O2 framework.
- No Arrow Flight server for RNTuple data exists anywhere in the open-source HEP ecosystem.

This project is an attempt to build both.

---

## What it does

- Reads ROOT RNTuple files using the official C++ ROOT API (ROOT 6.36+).
- Converts RNTuple columns to `arrow::RecordBatch` / `arrow::Table` in memory.
- Exposes data to Python as a `pyarrow.Table` via a `pybind11` module — no Awkward Array layer required.
- Serves RNTuple data over the network as an **Arrow Flight gRPC stream** — any Arrow-aware client (Python, C++, Go, Java) can connect and read physics data without ROOT installed.

---

## Architecture

```
┌──────────────────────────┐
│  file.root (RNTuple)     │
└────────────┬─────────────┘
             │
             ▼
┌────────────────────────────────────────────────┐
│  RAG C++ engine (librag)                       │
│                                                │
│  RNTupleReader → column pages                  │
│       ↓                                        │
│  Schema mapper  (RNTuple field → Arrow type)   │
│       ↓                                        │
│  BatchBuilder   (pages → arrow::RecordBatch)   │
└────────────┬───────────────────────────────────┘
             │
      ┌──────┴───────┐
      ▼              ▼
┌───────────┐  ┌──────────────────────────┐
│ pybind11  │  │ Arrow Flight gRPC server │
│ → pyarrow │  │ → any network client     │
└───────────┘  └──────────────────────────┘
```

The engine is the only reusable piece. Both sinks (Python binding and Flight server) are thin wrappers over the same C++ API.

---

## Building

### With Docker (recommended)

```bash
# Build the dev image (~10–20 min first time; cached after that)
docker build -f docker/dev.Dockerfile -t rntuple-arrow-gateway:dev .

# Configure, build, and test
docker run --rm -v "$(pwd)":/workspace rntuple-arrow-gateway:dev bash -c "
  cmake --preset release &&
  cmake --build --preset release --parallel 2 &&
  ctest --test-dir build/release --output-on-failure
"
```

### Without Docker

Requires: CMake ≥ 3.22, ROOT 6.36+, Apache Arrow C++ 19.0.0 with Flight, pybind11.

```bash
cmake --preset release
cmake --build --preset release --parallel 2
ctest --test-dir build/release --output-on-failure
```

---

## Benchmarks

Work in progress — will be added soon.

---

## References

- ROOT RNTuple on-disk format release — https://root.cern/blog/rntuple-binary-format/
- ROOT 6.36 release notes — https://root.cern/doc/v636/release-notes.html
- ATLAS RNTuple adoption (CHEP 2025) — https://www.epj-conferences.org/articles/epjconf/pdf/2025/22/epjconf_chep2025_01056.pdf
- Apache Arrow C++ documentation — https://arrow.apache.org/docs/cpp/
- Arrow Flight RPC specification — https://arrow.apache.org/docs/format/Flight.html
- ALICE O2 analysis framework — https://aliceo2group.github.io/analysis-framework/docs/
- Uproot documentation — https://uproot.readthedocs.io/
- rootproject/root Docker images — https://hub.docker.com/r/rootproject/root

---

## Disclaimer

This is an independent, third-party project. It is not affiliated with, endorsed by, or produced in collaboration with CERN, the ROOT team, the Apache Arrow project, or any LHC experiment (ATLAS, CMS, LHCb, ALICE).

---

## License

Apache License, Version 2.0. See [LICENSE](LICENSE).
