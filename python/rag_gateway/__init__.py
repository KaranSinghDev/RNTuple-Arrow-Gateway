from rag_gateway._native import open_table as _open_table
import pyarrow as pa


def open(path: str, ntuple_name: str = "ntuple") -> pa.Table:
    """Open an RNTuple file and return a pyarrow.Table."""
    return _open_table(path, ntuple_name)
