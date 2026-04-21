"""
Arrow Flight localhost benchmark: RAG in-process vs RAG Flight gRPC.

Starts rag-flight-server as a subprocess on a random free port, measures
round-trip throughput (server read + gRPC serialisation + client deserialisation).

Env vars required:
    FIXTURE_SMALL, FIXTURE_MEDIUM, FIXTURE_LARGE
    FLIGHT_SERVER   — path to rag-flight-server binary
    RESULTS_DIR
"""

import csv
import os
import socket
import statistics
import subprocess
import time

import pyarrow as pa
import pyarrow.flight as pf
import rag_gateway

N_REPS   = 5
N_WARMUP = 1
NTUPLE   = "bench"

FIXTURES = {
    "100MB": os.environ["FIXTURE_SMALL"],
    "500MB": os.environ["FIXTURE_MEDIUM"],
    "1GB":   os.environ["FIXTURE_LARGE"],
}
FLIGHT_SERVER = os.environ["FLIGHT_SERVER"]
RESULTS_DIR   = os.environ["RESULTS_DIR"]


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def table_bytes(table: pa.Table) -> int:
    return sum(
        buf.size
        for col in table.columns
        for chunk in col.chunks
        for buf in chunk.buffers()
        if buf is not None
    )


def start_server(path: str, port: int) -> subprocess.Popen:
    proc = subprocess.Popen(
        [FLIGHT_SERVER, "--file", path, "--ntuple", NTUPLE, "--port", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.monotonic() + 15.0
    while time.monotonic() < deadline:
        try:
            conn = pf.connect(f"grpc://localhost:{port}")
            list(conn.list_flights())
            conn.close()
            return proc
        except Exception:
            time.sleep(0.05)
    proc.terminate()
    raise RuntimeError(f"rag-flight-server did not start on port {port}")


def run_inprocess(path: str) -> pa.Table:
    return rag_gateway.open(path, NTUPLE)


def run_flight(path: str, port: int) -> pa.Table:
    conn = pf.connect(f"grpc://localhost:{port}")
    reader = conn.do_get(pf.Ticket(NTUPLE.encode()))
    table = reader.read_all()
    conn.close()
    return table


def bench_inprocess(path: str):
    times = []
    table = None
    for i in range(N_WARMUP + N_REPS):
        t0 = time.perf_counter()
        table = run_inprocess(path)
        t1 = time.perf_counter()
        if i >= N_WARMUP:
            times.append(t1 - t0)
    return times, table


def bench_flight_server(path: str):
    port = free_port()
    proc = start_server(path, port)
    try:
        times = []
        table = None
        for i in range(N_WARMUP + N_REPS):
            t0 = time.perf_counter()
            table = run_flight(path, port)
            t1 = time.perf_counter()
            if i >= N_WARMUP:
                times.append(t1 - t0)
        return times, table
    finally:
        proc.terminate()
        proc.wait()


def main():
    rows = []
    print(f"Flight benchmark  ({N_WARMUP} warmup + {N_REPS} reps)\n")

    for size_label, path in FIXTURES.items():
        print(f"Fixture: {size_label}  ({path})")

        for method, fn in [("rag_inprocess", bench_inprocess),
                           ("rag_flight_localhost", bench_flight_server)]:
            print(f"  {method} ...", end="", flush=True)
            times, table = fn(path)
            med = statistics.median(times)
            std = statistics.stdev(times) if len(times) > 1 else 0.0
            nbytes = table_bytes(table)
            tput = nbytes / med / 1e6
            print(f"  median={med*1000:.1f} ms  stdev={std*1000:.1f} ms  "
                  f"{tput:.1f} MB/s")
            rows.append({
                "size":              size_label,
                "method":            method,
                "median_s":          round(med, 6),
                "stdev_s":           round(std, 6),
                "min_s":             round(min(times), 6),
                "max_s":             round(max(times), 6),
                "bytes_uncompressed": nbytes,
                "throughput_MBps":   round(tput, 2),
                "reps":              N_REPS,
            })
        print()

    os.makedirs(RESULTS_DIR, exist_ok=True)
    csv_path = os.path.join(RESULTS_DIR, "flight.csv")
    fieldnames = ["size","method","median_s","stdev_s","min_s","max_s",
                  "bytes_uncompressed","throughput_MBps","reps"]
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"CSV written: {csv_path}")


if __name__ == "__main__":
    main()
