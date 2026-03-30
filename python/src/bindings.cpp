#include <pybind11/pybind11.h>
#include <arrow/c/bridge.h>
#include <arrow/table.h>
#include <rag/reader.hpp>

#include <memory>
#include <stdexcept>
#include <string>

namespace py = pybind11;

// Read an RNTuple file and return a pyarrow.Table.
// Data crosses the language boundary via the Arrow C Stream Interface —
// no extra copy beyond what the engine already performs.
py::object open_table(const std::string& path, const std::string& ntuple_name) {
    auto file_result = rag::RNTupleFile::Open(path, ntuple_name);
    if (!file_result.ok()) {
        throw std::runtime_error(file_result.status().ToString());
    }
    auto file = std::move(file_result).ValueOrDie();

    auto table_result = file->ReadAll();
    if (!table_result.ok()) {
        throw std::runtime_error(table_result.status().ToString());
    }
    // shared_ptr keeps the table alive through the reader.
    std::shared_ptr<arrow::Table> table = std::move(table_result).ValueOrDie();

    // Wrap as a RecordBatchReader and export via Arrow C Stream Interface.
    auto batch_reader = std::make_shared<arrow::TableBatchReader>(table);
    ArrowArrayStream stream{};
    auto status = arrow::ExportRecordBatchReader(batch_reader, &stream);
    if (!status.ok()) {
        throw std::runtime_error(status.ToString());
    }

    // _import_from_c takes ownership: it zeros our struct (sets release=NULL)
    // and reads all batches synchronously, so lifetime is safe.
    auto pyarrow = py::module_::import("pyarrow");
    auto py_reader = pyarrow.attr("RecordBatchReader").attr("_import_from_c")(
        reinterpret_cast<uintptr_t>(&stream));
    return py_reader.attr("read_all")();
}

PYBIND11_MODULE(_native, m) {
    m.doc() = "rntuple-arrow-gateway: C++ RNTuple → Arrow bridge";
    m.def("open_table", &open_table,
          py::arg("path"), py::arg("ntuple_name"),
          "Open an RNTuple file and return a pyarrow.Table");
}
