# API reference

RNArrow has three interfaces: a Python module, a C++ library, and a Flight
server. All three read the same RNTuple data and return the same Arrow output.

---

## Python

### `rag_gateway.open(path, ntuple_name="ntuple") -> pyarrow.Table`

Opens an RNTuple file and returns all rows as a `pyarrow.Table`.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` | — | Path to the `.root` file |
| `ntuple_name` | `str` | `"ntuple"` | Name of the RNTuple inside the file |

Raises a `RuntimeError` if the file cannot be opened, the RNTuple is not
found, or a column has a type that is not supported.

```python
import rag_gateway
table = rag_gateway.open("data.root", "events")
```

---

## C++

Header: `#include <rag/reader.hpp>`. Namespace: `rag`.

### `struct ReaderOptions`

| Field | Type | Default | Description |
|---|---|---|---|
| `batch_size` | `std::size_t` | `65536` | Rows per `RecordBatch` |
| `columns` | `std::vector<std::string>` | empty | Columns to read. Empty means all top-level fields. |

### `class RNTupleFile`

| Method | Returns | Description |
|---|---|---|
| `Open(path, ntuple_name, opts = {})` | `Result<std::unique_ptr<RNTupleFile>>` | Static factory. Opens the file and maps the schema. |
| `arrow_schema()` | `const std::shared_ptr<arrow::Schema>&` | The Arrow schema of the RNTuple. |
| `num_rows()` | `std::int64_t` | Number of entries in the RNTuple. |
| `NextBatch()` | `Result<std::shared_ptr<arrow::RecordBatch>>` | The next batch, or `nullptr` when all rows are read. |
| `ReadAll()` | `Result<std::shared_ptr<arrow::Table>>` | All rows as one `arrow::Table`. |
| `StreamBatches(on_batch)` | `Status` | Resets the cursor and calls `on_batch` for every batch. |

`Result<T>` and `Status` are aliases for `arrow::Result<T>` and
`arrow::Status`. Check them before use, or call `.ValueOrDie()` if you want
errors to abort.

```cpp
#include <rag/reader.hpp>

rag::ReaderOptions opts;
opts.batch_size = 8192;
opts.columns    = {"pt", "eta"};   // read two columns only

auto file  = rag::RNTupleFile::Open("data.root", "events", opts).ValueOrDie();
auto table = file->ReadAll().ValueOrDie();
```

---

## Flight server

### `rag-flight-server`

Serves one RNTuple over Arrow Flight (gRPC). Clients do not need ROOT.

| Flag | Short | Default | Description |
|---|---|---|---|
| `--file` | `-f` | — | Path to the `.root` file (required) |
| `--ntuple` | `-n` | `"ntuple"` | Name of the RNTuple inside the file |
| `--port` | `-p` | `9090` | Port to listen on |

```bash
rag-flight-server --file data.root --ntuple events --port 9090
```

Supported Flight calls:

| Call | Description |
|---|---|
| `ListFlights` | Lists the RNTuple served by this instance. |
| `GetFlightInfo` | Returns the row count and an endpoint ticket. |
| `DoGet` | Streams the data as Arrow record batches. |

The ticket is the RNTuple name as bytes, for example `b"events"`.

---

## Supported types

| RNTuple type | Arrow type |
|---|---|
| `std::int32_t` | `int32` |
| `std::int64_t` | `int64` |
| `float` | `float32` |
| `double` | `float64` |
| `bool` | `bool` |
| `std::vector<T>` of the above | `list<T>` |

Any other type causes `Open` to fail with a clear error. Nested collections
such as `std::vector<std::vector<T>>` are not supported yet.
