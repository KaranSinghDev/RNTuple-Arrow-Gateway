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

## Merge decision

Recommend keeping this branch open until the list-column path is also
resolved, so the v0.2.0 release can ship both improvements together.
Master stays at v0.1.0 in the meantime.
