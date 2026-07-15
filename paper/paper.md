---
title: 'RNArrow: A standalone C++ library and Arrow Flight server for ROOT RNTuple data'
tags:
  - C++
  - Python
  - high energy physics
  - Apache Arrow
  - ROOT
  - RNTuple
  - columnar data
  - gRPC
authors:
  - name: Karan Singh
    orcid: 0009-0000-0920-2379
    affiliation: 1
affiliations:
  - name: Independent Researcher, India
    index: 1
date: 15 July 2026
bibliography: paper.bib
---

# Summary

Experiments at CERN store their data with the ROOT framework [@Brun:1997].
ROOT has a new columnar file format called RNTuple. It replaces the older
TTree format and is designed for the large data volumes expected at the
High-Luminosity LHC [@LopezGomez:2022]. Outside physics, Apache Arrow
[@Arrow] is the common format for columnar data in memory. Tools such as
pandas, Polars, DuckDB, and Spark all use it.

`RNArrow` connects these two worlds. It is a C++ library that reads RNTuple
files with ROOT's official C++ API and converts them into Arrow `RecordBatch`
and `Table` objects. The same engine has two outputs. The first is a
`pybind11` module that returns a `pyarrow.Table` through Arrow's C Data
Interface. The second is an Arrow Flight [@Flight] gRPC server that streams
record batches over the network. Only the server needs ROOT. Clients written
in Python, C++, Java, Go, or Rust can read the data without installing ROOT.

The library supports `int32`, `int64`, `float`, `double`, `bool`, and
single-level `std::vector<T>` of these types. The output is checked column by
column against `uproot` [@Pivarski:uproot]. The repository also includes a
benchmark suite. It compares the C++ engine, the Python binding, and the
Flight transport against a plain `RNTupleReader` loop.

# Statement of need

There are two gaps in the current tools.

First, no standalone C++ library converts RNTuple data to Arrow. The ALICE O2
framework contains code that does this [@AliceO2], but it is tied to the O2
framework and cannot be reused in other projects. A client written in C++,
Go, or Rust has no ready option.

Second, no Arrow Flight server for RNTuple data exists in ROOT or in any
experiment framework, as far as the author knows. Arrow Flight is used in
production systems: Dremio and InfluxDB v3 both use it as their client
protocol. However, it has no way to read RNTuple. `RNArrow` adds that path.
One server with ROOT installed can serve physics data as Arrow to many
clients that do not have ROOT.

`RNArrow` is for people who work between HEP data and the wider data
ecosystem. It is also for anyone who wants to test whether Arrow Flight is a
good transport for physics data. For Python-only analysis, `uproot` already
reads RNTuple and remains the better choice. `RNArrow` covers the C++ and
network cases that `uproot` does not.

The project is a working prototype. It is correct and benchmarked. However,
it was tested on synthetic files rather than real detector data, and it does
not support nested collections yet.

# Implementation

When a file is opened, the engine reads the RNTuple schema once and maps each
field to an Arrow type. If a type is not supported, it returns a clear error
instead of a silent fallback. Data is copied into Arrow buffers that are
allocated in advance. Batches are produced one at a time, so the Flight server
and the Python module can stream data instead of loading a whole file into
memory.

The Flight server implements `DoGet`, `GetFlightInfo`, and `ListFlights`. A
`RecordBatchReader` wraps the engine's batch API. This matches the way Flight
pulls data, so no extra threads are needed.

# Acknowledgements

I thank Jakob Blomer for his advice on RNTuple's bulk-read API on the ROOT
forum, and members of the HEP software community for their feedback on the
design and scope of this project. This work uses ROOT, Apache Arrow, and
`uproot`.

# References
