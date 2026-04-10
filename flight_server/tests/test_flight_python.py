import os
import subprocess
import time

import pyarrow as pa
import pyarrow.flight as pf
import pytest

FLIGHT_SERVER_PATH = os.environ["FLIGHT_SERVER_PATH"]
FIXTURE_PATH = os.environ["FIXTURE_PATH"]
NTUPLE_NAME = "fixture"
PORT = 39091
FIXTURE_ROWS = int(os.environ.get("FIXTURE_ROWS", "1000"))


@pytest.fixture(scope="module")
def client():
    proc = subprocess.Popen(
        [
            FLIGHT_SERVER_PATH,
            "--file", FIXTURE_PATH,
            "--ntuple", NTUPLE_NAME,
            "--port", str(PORT),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    # Retry until the server accepts connections (up to 10 s).
    conn = None
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        try:
            conn = pf.connect(f"grpc://localhost:{PORT}")
            list(conn.list_flights())  # probe: raises if server not ready
            break
        except Exception:
            conn = None
            time.sleep(0.1)
    if conn is None:
        proc.terminate()
        pytest.fail("rag-flight-server did not start within 10 seconds")
    yield conn
    conn.close()
    proc.terminate()
    proc.wait()


def _table(client):
    reader = client.do_get(pf.Ticket(NTUPLE_NAME.encode()))
    return reader.read_all()


def test_schema(client):
    s = _table(client).schema
    assert s.field("i32").type == pa.int32()
    assert s.field("i64").type == pa.int64()
    assert s.field("f32").type == pa.float32()
    assert s.field("f64").type == pa.float64()
    assert s.field("b").type == pa.bool_()


def test_row_count(client):
    assert _table(client).num_rows == FIXTURE_ROWS


def test_values_i32(client):
    assert _table(client).column("i32").to_pylist() == list(range(FIXTURE_ROWS))


def test_values_i64(client):
    assert _table(client).column("i64").to_pylist() == [i * 1000 for i in range(FIXTURE_ROWS)]


def test_values_f32(client):
    col = _table(client).column("f32").to_pylist()
    for i, v in enumerate(col):
        assert abs(v - i * 0.5) < 1e-6, f"row {i}: {v} != {i * 0.5}"


def test_values_f64(client):
    col = _table(client).column("f64").to_pylist()
    for i, v in enumerate(col):
        assert abs(v - i * 0.1) < 1e-12, f"row {i}: {v} != {i * 0.1}"


def test_values_bool(client):
    assert _table(client).column("b").to_pylist() == [(i % 2 == 0) for i in range(FIXTURE_ROWS)]


def test_get_flight_info(client):
    descriptor = pf.FlightDescriptor.for_path(FIXTURE_PATH)
    info = client.get_flight_info(descriptor)
    assert info.total_records == FIXTURE_ROWS
    assert len(info.endpoints) == 1
    assert info.endpoints[0].ticket.ticket == NTUPLE_NAME.encode()
