import os
import pyarrow as pa
import rag_gateway

FIXTURE_LISTS_PATH = os.environ["FIXTURE_LISTS_PATH"]
ROWS = 5


def _table():
    return rag_gateway.open(FIXTURE_LISTS_PATH, "fixture_lists")


def test_schema_types():
    s = _table().schema
    assert s.field("vi32").type == pa.list_(pa.int32())
    assert s.field("vf32").type == pa.list_(pa.float32())


def test_row_count():
    assert _table().num_rows == ROWS


# vi32: row 0=[], row 1=[1], row 2=[2,3], row 3=[4,5,6], row 4=[7,8,9,10]

def test_vi32_as_pylist():
    col = _table().column("vi32").to_pylist()
    assert col[0] == []
    assert col[1] == [1]
    assert col[2] == [2, 3]
    assert col[3] == [4, 5, 6]
    assert col[4] == [7, 8, 9, 10]


# vf32: row 0=[0.0], row 1=[0.5,1.0], row 2=[], row 3=[1.5], row 4=[2.0,2.5]

def test_vf32_as_pylist():
    col = _table().column("vf32").to_pylist()
    assert col[2] == []
    for v, expected in zip(col[1], [0.5, 1.0]):
        assert abs(v - expected) < 1e-6
    for v, expected in zip(col[4], [2.0, 2.5]):
        assert abs(v - expected) < 1e-6


def test_empty_list_roundtrip():
    col = _table().column("vi32").to_pylist()
    assert col[0] == []
    col2 = _table().column("vf32").to_pylist()
    assert col2[2] == []
