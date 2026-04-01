import os
import pyarrow as pa
import rag_gateway

FIXTURE_PATH = os.environ["FIXTURE_PATH"]
FIXTURE_ROWS = int(os.environ.get("FIXTURE_ROWS", "1000"))


def test_returns_table():
    table = rag_gateway.open(FIXTURE_PATH, "fixture")
    assert isinstance(table, pa.Table)


def test_row_count():
    table = rag_gateway.open(FIXTURE_PATH, "fixture")
    assert table.num_rows == FIXTURE_ROWS


def test_column_count():
    table = rag_gateway.open(FIXTURE_PATH, "fixture")
    assert table.num_columns == 5


def test_schema_types():
    table = rag_gateway.open(FIXTURE_PATH, "fixture")
    assert table.schema.field("i32").type == pa.int32()
    assert table.schema.field("i64").type == pa.int64()
    assert table.schema.field("f32").type == pa.float32()
    assert table.schema.field("f64").type == pa.float64()
    assert table.schema.field("b").type == pa.bool_()


def test_values_i32():
    col = rag_gateway.open(FIXTURE_PATH, "fixture").column("i32").to_pylist()
    assert col == list(range(FIXTURE_ROWS))


def test_values_i64():
    col = rag_gateway.open(FIXTURE_PATH, "fixture").column("i64").to_pylist()
    assert col == [i * 1000 for i in range(FIXTURE_ROWS)]


def test_values_f32():
    col = rag_gateway.open(FIXTURE_PATH, "fixture").column("f32").to_pylist()
    for i, v in enumerate(col):
        assert abs(v - i * 0.5) < 1e-6, f"row {i}: {v} != {i * 0.5}"


def test_values_f64():
    col = rag_gateway.open(FIXTURE_PATH, "fixture").column("f64").to_pylist()
    for i, v in enumerate(col):
        assert abs(v - i * 0.1) < 1e-12, f"row {i}: {v} != {i * 0.1}"


def test_values_bool():
    col = rag_gateway.open(FIXTURE_PATH, "fixture").column("b").to_pylist()
    assert col == [(i % 2 == 0) for i in range(FIXTURE_ROWS)]


def test_matches_c_engine():
    """Values via Python binding must byte-match C++ test fixture expectations."""
    table = rag_gateway.open(FIXTURE_PATH, "fixture")
    i32 = table.column("i32")[42].as_py()
    i64 = table.column("i64")[42].as_py()
    b   = table.column("b")[42].as_py()
    assert i32 == 42
    assert i64 == 42 * 1000
    assert b is True  # 42 % 2 == 0
