"""
Generates 3 publication-quality benchmark plots from CSV results.

Plot 1 — Python path throughput comparison (uproot naive / uproot fast / RAG)
Plot 2 — C++ path overhead (raw RNTupleReader vs RAG ReadAll vs RAG streaming)
Plot 3 — Arrow Flight gRPC overhead (in-process vs localhost Flight)

Env vars:
    RESULTS_DIR — directory containing *.csv and where *.png will be written
"""

import json
import os
import csv
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

RESULTS_DIR = os.environ["RESULTS_DIR"]

SIZE_ORDER  = ["100MB", "500MB", "1GB"]
FIG_DPI     = 150
BAR_WIDTH   = 0.22
COLORS = {
    "uproot_naive":           "#e07b39",
    "uproot_fast":            "#4c97c9",
    "rag_pybind11":           "#2ca02c",
    "BM_RawRNTuple_mean":     "#e07b39",
    "BM_RAGReadAll_mean":     "#2ca02c",
    "BM_RAGStreaming_mean":   "#9467bd",
    "rag_inprocess":          "#2ca02c",
    "rag_flight_localhost":   "#d62728",
}
LABELS = {
    "uproot_naive":           "uproot (naive)",
    "uproot_fast":            "uproot fast (ak→arrow)",
    "rag_pybind11":           "RAG pybind11",
    "BM_RawRNTuple_mean":     "Raw RNTupleReader",
    "BM_RAGReadAll_mean":     "RAG ReadAll",
    "BM_RAGStreaming_mean":   "RAG Streaming",
    "rag_inprocess":          "RAG in-process",
    "rag_flight_localhost":   "RAG Flight (localhost)",
}


def read_csv(filename):
    path = os.path.join(RESULTS_DIR, filename)
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return list(csv.DictReader(f))


def read_cpp_json(filename="cpp_path.json"):
    path = os.path.join(RESULTS_DIR, filename)
    if not os.path.exists(path):
        return []
    with open(path) as f:
        data = json.load(f)
    return data.get("benchmarks", [])


def save(fig, name):
    out = os.path.join(RESULTS_DIR, name)
    fig.savefig(out, dpi=FIG_DPI, bbox_inches="tight")
    plt.close(fig)
    print(f"  saved: {out}")


# ── Plot 1: Python path throughput ────────────────────────────────────────────

def plot_python_throughput():
    rows = read_csv("python_path.csv")
    if not rows:
        print("  [skip] python_path.csv not found")
        return

    methods = ["uproot_naive", "uproot_fast", "rag_pybind11"]
    data = defaultdict(dict)
    errs = defaultdict(dict)
    for r in rows:
        data[r["method"]][r["size"]] = float(r["throughput_MBps"])
        errs[r["method"]][r["size"]] = float(r["stdev_s"]) * float(r["throughput_MBps"]) \
                                        / max(float(r["median_s"]), 1e-9)

    x = np.arange(len(SIZE_ORDER))
    offsets = np.linspace(-(len(methods)-1)/2, (len(methods)-1)/2, len(methods)) * BAR_WIDTH

    fig, ax = plt.subplots(figsize=(9, 5))
    for i, method in enumerate(methods):
        vals = [data[method].get(s, 0) for s in SIZE_ORDER]
        erv  = [errs[method].get(s, 0) for s in SIZE_ORDER]
        ax.bar(x + offsets[i], vals, BAR_WIDTH,
               label=LABELS.get(method, method),
               color=COLORS.get(method, "#888"),
               yerr=erv, capsize=4, error_kw={"linewidth": 1.2})

    ax.set_xlabel("Fixture size", fontsize=12)
    ax.set_ylabel("Throughput (MB/s, uncompressed)", fontsize=12)
    ax.set_title("RNTuple Read Throughput — Python Path\n"
                 "(uproot naive vs uproot fast vs RAG pybind11)", fontsize=13)
    ax.set_xticks(x)
    ax.set_xticklabels(SIZE_ORDER)
    ax.legend(fontsize=10)
    ax.yaxis.grid(True, linestyle="--", alpha=0.6)
    ax.set_axisbelow(True)
    fig.tight_layout()
    save(fig, "plot_python_throughput.png")


# ── Plot 2: C++ path overhead ─────────────────────────────────────────────────

def plot_cpp_overhead():
    benchmarks = read_cpp_json()
    if not benchmarks:
        print("  [skip] cpp_path.json not found")
        return

    label_map = {"0": "100MB", "1": "500MB", "2": "1GB"}
    bm_names  = ["BM_RawRNTuple_mean", "BM_RAGReadAll_mean", "BM_RAGStreaming_mean"]
    data = defaultdict(dict)
    errs = defaultdict(dict)

    for bm in benchmarks:
        name = bm.get("name", "")
        # name format: BM_RawRNTuple/0/min_time:5.000_mean
        for bm_key in bm_names:
            base = bm_key.replace("_mean", "")
            if name.startswith(base + "/") and name.endswith("_mean"):
                # extract arg: first segment after base/
                rest = name[len(base)+1:]          # e.g. "0/min_time:5.000_mean"
                arg = rest.split("/")[0]           # e.g. "0"
                size = label_map.get(arg, arg)
                bps = float(bm.get("bytes_per_second", 0))
                data[bm_key][size] = bps / 1e6
                errs[bm_key][size] = 0.0

    # If bytes_per_second unavailable, use wall time
    if not any(data.values()):
        for bm in benchmarks:
            name = bm.get("name", "")
            for bm_key in bm_names:
                base = bm_key.replace("_mean", "")
                if name.startswith(base + "/") and name.endswith("_mean"):
                    rest = name[len(base)+1:]
                    arg  = rest.split("/")[0]
                    size = label_map.get(arg, arg)
                    ms = float(bm.get("real_time", 0))
                    data[bm_key][size] = ms
                    errs[bm_key][size] = float(bm.get("real_time_cv", 0)) * ms / 100.0

        y_label = "Wall time (ms)"
        title_suffix = "wall time — lower is better"
    else:
        y_label = "Throughput (MB/s, uncompressed)"
        title_suffix = "throughput — higher is better"

    sizes_present = [s for s in SIZE_ORDER if any(s in data[k] for k in bm_names)]
    if not sizes_present:
        print("  [skip] no C++ benchmark data parsed")
        return

    x = np.arange(len(sizes_present))
    offsets = np.linspace(-(len(bm_names)-1)/2, (len(bm_names)-1)/2, len(bm_names)) * BAR_WIDTH

    fig, ax = plt.subplots(figsize=(9, 5))
    for i, bm_key in enumerate(bm_names):
        vals = [data[bm_key].get(s, 0) for s in sizes_present]
        erv  = [errs[bm_key].get(s, 0) for s in sizes_present]
        ax.bar(x + offsets[i], vals, BAR_WIDTH,
               label=LABELS.get(bm_key, bm_key),
               color=COLORS.get(bm_key, "#888"),
               yerr=erv, capsize=4, error_kw={"linewidth": 1.2})

    ax.set_xlabel("Fixture size", fontsize=12)
    ax.set_ylabel(y_label, fontsize=12)
    ax.set_title(f"RNTuple Read — C++ Path Overhead\n"
                 f"(raw RNTupleReader vs RAG arrow::Table, {title_suffix})", fontsize=13)
    ax.set_xticks(x)
    ax.set_xticklabels(sizes_present)
    ax.legend(fontsize=10)
    ax.yaxis.grid(True, linestyle="--", alpha=0.6)
    ax.set_axisbelow(True)
    fig.tight_layout()
    save(fig, "plot_cpp_overhead.png")


# ── Plot 3: Flight gRPC overhead ──────────────────────────────────────────────

def plot_flight_overhead():
    rows = read_csv("flight.csv")
    if not rows:
        print("  [skip] flight.csv not found")
        return

    methods = ["rag_inprocess", "rag_flight_localhost"]
    data = defaultdict(dict)
    errs = defaultdict(dict)
    for r in rows:
        data[r["method"]][r["size"]] = float(r["throughput_MBps"])
        errs[r["method"]][r["size"]] = float(r["stdev_s"]) * float(r["throughput_MBps"]) \
                                        / max(float(r["median_s"]), 1e-9)

    x = np.arange(len(SIZE_ORDER))
    offsets = np.linspace(-(len(methods)-1)/2, (len(methods)-1)/2, len(methods)) * BAR_WIDTH

    fig, ax = plt.subplots(figsize=(9, 5))
    for i, method in enumerate(methods):
        vals = [data[method].get(s, 0) for s in SIZE_ORDER]
        erv  = [errs[method].get(s, 0) for s in SIZE_ORDER]
        ax.bar(x + offsets[i], vals, BAR_WIDTH,
               label=LABELS.get(method, method),
               color=COLORS.get(method, "#888"),
               yerr=erv, capsize=4, error_kw={"linewidth": 1.2})

    ax.set_xlabel("Fixture size", fontsize=12)
    ax.set_ylabel("Throughput (MB/s, uncompressed)", fontsize=12)
    ax.set_title("Arrow Flight gRPC Overhead\n"
                 "(RAG in-process vs RAG Flight localhost)", fontsize=13)
    ax.set_xticks(x)
    ax.set_xticklabels(SIZE_ORDER)
    ax.legend(fontsize=10)
    ax.yaxis.grid(True, linestyle="--", alpha=0.6)
    ax.set_axisbelow(True)
    fig.tight_layout()
    save(fig, "plot_flight_overhead.png")


if __name__ == "__main__":
    print("Generating plots...")
    plot_python_throughput()
    plot_cpp_overhead()
    plot_flight_overhead()
    print("Done.")
