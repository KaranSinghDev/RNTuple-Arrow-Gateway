# RAG_Gateway — RNTuple-to-Arrow Gateway

A C++ gateway that reads ROOT `RNTuple` files and exposes the data as Apache Arrow — both in-process to Python via `pybind11` → `pyarrow`, and over the network via an Arrow Flight (gRPC) server.

**Status:** early development. See [`CLAUDE.md`](CLAUDE.md) for the full project plan, verified research context, architecture, and milestone roadmap.

**Affiliation:** third-party, independent project. Not affiliated with CERN or the ROOT project.

Licensed under the Apache License, Version 2.0. See [`LICENSE`](LICENSE).
