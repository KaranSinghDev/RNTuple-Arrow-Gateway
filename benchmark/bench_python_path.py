"""
Python path benchmark: uproot naive vs uproot fast (ak→arrow) vs RAG pybind11.

Env vars required:
    FIXTURE_SMALL, FIXTURE_MEDIUM, FIXTURE_LARGE  — paths to .root files
    RESULTS_DIR                                    — output directory for CSV

Methodology:
    - 1 warmup run (discarded), then N_REPS timed runs per (method, fixture).
    - Wall time measured with time.perf_counter().
    - Throughput = Arrow table uncompressed buffer bytes / median wall time (MB/s).
    - Correctness: RAG output is byte-checked against uproot fast-path output.
"""

import csv
import os
import statistics
import sys
import time

import awkward as ak
import numpy as np
import pyarrow as pa
import uproot
import rag_gateway

N_REPS   = 5
N_WARMUP = 1
NTUPLE   = "bench"

FIXTURES = {
    "100MB":  os.environ["FIXTURE_SMALL"],
    "500MB":  os.environ["FIXTURE_MEDIUM"],
    "1GB":    os.environ["FIXTURE_LARGE"],
}
RESULTS_DIR = os.environ["RESULTS_DIR"]


def table_bytes(table: pa.Table) -> int:
    return sum(
        buf.size
        for col in table.columns
        for chunk in col.chunks
        for buf in chunk.buffers()
        if buf is not None
    )


# ── Benchmark functions ───────────────────────────────────────────────────────

def run_uproot_naive(path: str) -> pa.Table:
    with uproot.open(path) as f:
        arrays = f[NTUPLE].arrays()
    # Convert awkward arrays to pyarrow for a fair bytes comparison
    return ak.to_arrow_table(arrays)


def run_uproot_fast(path: str) -> pa.Table:
    with uproot.open(path) as f:
        arr = f[NTUPLE].arrays(library="ak")
    return ak.to_arrow_table(arr)


def run_rag(path: str) -> pa.Table:
    return rag_gateway.open(path, NTUPLE)


METHODS = {
    "uproot_naive": run_uproot_naive,
    "uproot_fast":  run_uproot_fast,
    "rag_pybind11": run_rag,
}


def bench(fn, path: str):
    times = []
    table = None
    for i in range(N_WARMUP + N_REPS):
        t0 = time.perf_counter()
        table = fn(path)
        t1 = time.perf_counter()
        if i >= N_WARMUP:
            times.append(t1 - t0)
    return times, table


# ── Correctness check ─────────────────────────────────────────────────────────

def check_correctness(ref: pa.Table, got: pa.Table, label: str):
    assert ref.num_rows == got.num_rows, \
        f"[{label}] row count mismatch: {ref.num_rows} vs {got.num_rows}"
    for col in ["i32", "i64", "f32", "f64", "b"]:
        if col not in ref.schema.names or col not in got.schema.names:
            continue
        r = ref.column(col).to_pylist()
        g = got.column(col).to_pylist()
        assert r == g, f"[{label}] column '{col}' mismatch at first diff index " \
                       f"{next(i for i,(a,b) in enumerate(zip(r,g)) if a!=b)}"
    print(f"  correctness OK ({label})")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    rows = []
    print(f"Python path benchmark  ({N_WARMUP} warmup + {N_REPS} reps)\n")

    for size_label, path in FIXTURES.items():
        print(f"Fixture: {size_label}  ({path})")

        results = {}
        tables  = {}
        for method, fn in METHODS.items():
            print(f"  {method} ...", end="", flush=True)
            times, table = bench(fn, path)
            results[method] = times
            tables[method]  = table
            med = statistics.median(times)
            std = statistics.stdev(times) if len(times) > 1 else 0.0
            nbytes = table_bytes(table)
            tput = nbytes / med / 1e6
            print(f"  median={med*1000:.1f} ms  stdev={std*1000:.1f} ms  "
                  f"{tput:.1f} MB/s  ({nbytes/1e6:.1f} MB uncompressed)")
            rows.append({
                "size":       size_label,
                "method":     method,
                "median_s":   round(med, 6),
                "stdev_s":    round(std, 6),
                "min_s":      round(min(times), 6),
                "max_s":      round(max(times), 6),
                "bytes_uncompressed": nbytes,
                "throughput_MBps": round(nbytes / med / 1e6, 2),
                "reps":       N_REPS,
            })

        # Correctness: RAG vs uproot_fast (reference)
        ref = tables["uproot_fast"]
        check_correctness(ref, tables["rag_pybind11"], f"{size_label}/rag_pybind11")
        print()

    # Write CSV
    os.makedirs(RESULTS_DIR, exist_ok=True)
    csv_path = os.path.join(RESULTS_DIR, "python_path.csv")
    fieldnames = ["size","method","median_s","stdev_s","min_s","max_s",
                  "bytes_uncompressed","throughput_MBps","reps"]
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"CSV written: {csv_path}")


if __name__ == "__main__":
    main()
