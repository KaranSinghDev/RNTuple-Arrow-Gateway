import os
import subprocess
import time

import pyarrow as pa
import pyarrow.flight as pf
import pytest

FLIGHT_SERVER_PATH = os.environ["FLIGHT_SERVER_PATH"]
FIXTURE_LISTS_PATH = os.environ["FIXTURE_LISTS_PATH"]
NTUPLE_NAME = "fixture_lists"
PORT = 39092  # distinct from test_flight_python.py (39091)
ROWS = 5


@pytest.fixture(scope="module")
def client():
    proc = subprocess.Popen(
        [
            FLIGHT_SERVER_PATH,
            "--file", FIXTURE_LISTS_PATH,
            "--ntuple", NTUPLE_NAME,
            "--port", str(PORT),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    conn = None
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        try:
            conn = pf.connect(f"grpc://localhost:{PORT}")
            list(conn.list_flights())
            break
        except Exception:
            conn = None
            time.sleep(0.1)
    if conn is None:
        proc.terminate()
        pytest.fail("rag-flight-server (lists) did not start within 10 seconds")
    yield conn
    conn.close()
    proc.terminate()
    proc.wait()


def _table(client):
    reader = client.do_get(pf.Ticket(NTUPLE_NAME.encode()))
    return reader.read_all()


def test_schema(client):
    s = _table(client).schema
    assert s.field("vi32").type == pa.list_(pa.int32())
    assert s.field("vf32").type == pa.list_(pa.float32())


def test_row_count(client):
    assert _table(client).num_rows == ROWS


def test_vi32_values(client):
    col = _table(client).column("vi32").to_pylist()
    assert col[0] == []
    assert col[1] == [1]
    assert col[2] == [2, 3]
    assert col[3] == [4, 5, 6]
    assert col[4] == [7, 8, 9, 10]


def test_vf32_values(client):
    col = _table(client).column("vf32").to_pylist()
    assert col[2] == []
    for v, expected in zip(col[1], [0.5, 1.0]):
        assert abs(v - expected) < 1e-6
    for v, expected in zip(col[4], [2.0, 2.5]):
        assert abs(v - expected) < 1e-6
