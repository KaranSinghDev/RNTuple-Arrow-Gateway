# CreateBulk experiment — results

Branch: `feat/createbulk-experiment` (commit `0c1348e`).
Same WSL2 host as v0.1.0 baseline: 12th Gen Intel i7-12700H, 11.7 GB RAM,
`rag-env:v0.0.2` docker image.

Implementation: per-cluster iteration; primitive columns
(`int32`, `int64`, `float32`, `float64`) bulk-read via
`RNTupleView<T>::CreateBulk()` + `RBulkValues::AdoptBuffer()` + `ReadBulk()`
into pre-allocated Arrow buffers. **`bool` and list columns still use the
per-entry path** because (a) bool needs byte→bit packing conversion and
(b) list columns need clarification on the offsets representation
(question pending to Jakob on the ROOT forum).

## Wall-time results (3 reps, MinTime 5 s, median)

| Benchmark | 100 MB | 500 MB | 1 GB |
|---|---|---|---|
| `BM_RawRNTuple` (this run baseline) | 385.1 ms | 1765.4 ms | 3414.7 ms |
| `BM_RAGReadAll` (per-entry, current master) | 551.6 ms | 2589.6 ms | 5766.0 ms |
| `BM_RAGStreaming` (per-entry, current master) | 560.7 ms | 2597.2 ms | 4932.0 ms |
| **`BM_RAGReadAllBulk` (this experiment)** | **467.7 ms** | **2248.5 ms** | **4281.1 ms** |

## Overhead ratios (vs `BM_RawRNTuple` from the same run)

| Path | 100 MB | 500 MB | 1 GB |
|---|---|---|---|
| `BM_RAGReadAll` | 1.43× | 1.47× | 1.69× |
| `BM_RAGStreaming` | 1.46× | 1.47× | 1.44× |
| **`BM_RAGReadAllBulk`** | **1.21×** | **1.27×** | **1.25×** |

## Bulk vs per-entry (`ReadAll`) wall-time delta

| Size | Per-entry ReadAll | Bulk ReadAll | Delta |
|---|---|---|---|
| 100 MB | 551.6 ms | 467.7 ms | **−15.2%** |
| 500 MB | 2589.6 ms | 2248.5 ms | **−13.2%** |
| 1 GB | 5766.0 ms | 4281.1 ms | **−25.7%** |

## Correctness

Verified independently: `ReadAll()` and `ReadAllBulk()` produce byte-identical
Tables for the `bench_small.root` fixture across all 7 columns (2 M rows,
including the list columns `vi32` and `vf32` which fall back to the per-entry
path inside the bulk implementation).

## Interpretation

The bulk path is a clear, measurable improvement on every fixture size, with
the gain growing at larger sizes (where per-entry overhead compounds more).
The 1 GB case is the most striking: 26% wall-time reduction.

The overhead vs the raw RNTupleReader loop drops from 1.43–1.69× down to
1.21–1.27×. This validates the approach suggested on the ROOT forum thread.

**What's still on the table (further potential wins):**
- Bool columns: bulk-read into a byte buffer, then bit-pack into Arrow's
  `BooleanArray` in one pass. Likely small absolute gain since bool is 1
  byte per entry.
- List columns: this is the larger remaining win, since `vi32` + `vf32`
  together carry significant byte traffic. Blocked on the offsets-
  representation question (waiting for follow-up on the forum thread).

## Regression check — Python and Flight paths

The bulk implementation is purely additive: `BuildAllBulk` /  `ReadAllBulk`
are new symbols. The existing `Build`, `Create`, `NextBatch`, `ReadAll`,
`StreamBatches`, the pybind11 module, and the Flight server are all
unchanged. To confirm there is no inadvertent regression, the Python-path
and Flight benchmarks were re-run on this branch and compared to the
v0.1.0 baseline.

### Python — `rag_pybind11` (uses `ReadAll`, unchanged path)

| Size | Baseline MB/s | Rerun MB/s | Δ |
|---|---|---|---|
| 100MB | 223.34 | 223.74 | +0.2% (identical) |
| 500MB | 232.23 | 229.86 | −1.0% (within noise) |

### Flight — `rag_inprocess` + `rag_flight_localhost` (uses `NextBatch`, unchanged)

| Method | Size | Baseline MB/s | Rerun MB/s | Δ |
|---|---|---|---|---|
| in-process | 100MB | 221.25 | 230.77 | +4.3% |
| Flight localhost | 100MB | 211.04 | 227.98 | +8.0% |
| in-process | 500MB | 225.90 | 233.84 | +3.5% |
| Flight localhost | 500MB | 234.51 | 237.21 | +1.2% |

All deltas are within run-to-run noise. No regression.

### Note on uproot_ak

`uproot_ak` showed lower MB/s in the rerun (599 → 491 at 100 MB; 525 → 445
at 500 MB) with higher stdev (0.084 → 0.152 at 500 MB). uproot is a
third-party library that is not modified by this branch; the variance is
environmental (WSL2 RAM/cache pressure, background load). Not a regression
caused by this work.

### Test suite

All 36 ctests pass on a clean rebuild of the branch (engine + Python sink
+ Flight roundtrip). Correctness of `ReadAllBulk` versus `ReadAll` verified
on the 2 M-row `bench_small.root` fixture, all 7 columns byte-match.

## Merge decision

Recommend keeping this branch open until the list-column path is also
resolved, so the v0.2.0 release can ship both improvements together.
Master stays at v0.1.0 in the meantime.

---

## Step 2 update — list-column bulk path implemented

Following Jakob Blomer's clarification on the ROOT forum (two bulk read
calls + Arrow offset fixup, using `RNTupleCardinality` for per-entry sizes
and the inner `_0` subfield view for the flat values buffer), `vi32` and
`vf32` columns now also flow through `CreateBulk()` + `AdoptBuffer()`.

### Implementation

`engine/src/batch_builder.cpp` adds `BulkReadListCluster<CppType>`:

1. `RNTupleView<ROOT::RNTupleCardinality<std::uint64_t>>` on the
   collection field → `CreateBulk()` + `AdoptBuffer(sizes.data(), n_entries)`
   → `ReadBulk(LocalIndex(cluster_id, 0), nullptr, n_entries)`.
2. Cumulative sum of sizes → Arrow `int32` offsets buffer
   (`n_entries + 1` entries).
3. `RNTupleView<CppType>("vi32._0")` (inner subfield) → `CreateBulk()` +
   `AdoptBuffer(values_buf, total_values)` → `ReadBulk(LocalIndex(cluster_id, 0), nullptr, total_values)`.
4. `arrow::ListArray(arrow::list(inner_type), n_entries, offsets, values)`.

`ReadColumnInCluster` now dispatches the `Type::LIST` case to
`BulkReadListCluster<inner>` for primitive inner types (int32 / int64 /
float / double); bool list inner type falls through to the per-entry path.

### Results (3 reps × ≥ 5 s, median)

| Benchmark | 100 MB | 500 MB | 1 GB |
|---|---|---|---|
| `BM_RawRNTuple` (this run) | 385.9 ms | 1788.4 ms | 3465.3 ms |
| `BM_RAGReadAll` (per-entry) | 536.8 ms | 2582.0 ms | 5414.6 ms |
| **`BM_RAGReadAllBulk` (bulk + lists)** | **252.8 ms** | **1232.9 ms** | **2428.8 ms** |

### Overhead ratios vs raw RNTuple loop

| Path | 100 MB | 500 MB | 1 GB |
|---|---|---|---|
| `BM_RAGReadAll` (per-entry) | 1.39× | 1.44× | 1.56× |
| **`BM_RAGReadAllBulk` (bulk + lists)** | **0.66×** | **0.69×** | **0.70×** |

### Wall-time reduction vs main / v0.1.0 ReadAll

| Size | v0.1.0 ReadAll | Bulk + lists | Δ |
|---|---|---|---|
| 100 MB | 555.7 ms | 252.8 ms | **−54.5%** |
| 500 MB | 2617.5 ms | 1232.9 ms | **−52.9%** |
| 1 GB | 5822.1 ms | 2428.8 ms | **−58.3%** |

### Why the bulk path beats the raw loop

The raw loop uses per-entry `view(i)` access on `RNTupleView<std::vector<T>>`,
which returns a fresh `std::vector<T>` per entry (allocation + copy). The
bulk path uses `CreateBulk()` + `AdoptBuffer()` — ROOT's intended fast path
for high-throughput consumers — and writes directly into Arrow's pre-
allocated buffers with no per-entry container construction. Closing the
list-column gap removes the dominant source of per-entry overhead.

### Correctness

`ReadAll()` and `ReadAllBulk()` produce byte-identical Tables on the
`bench_small.root` fixture across all 7 columns (2 M rows), now including
the list columns `vi32` and `vf32`. All 36 ctests still pass on a clean
rebuild of the branch.
