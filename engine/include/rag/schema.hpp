#pragma once
#include <rag/options.hpp>
#include <rag/status.hpp>

#include <arrow/type.h>
#include <ROOT/RNTupleReader.hxx>

#include <memory>
#include <string>

namespace rag {

// Maps the top-level primitive fields of an RNTuple to an arrow::Schema.
// Unsupported types (nested, variable-length, user-defined) produce an error.
Result<std::shared_ptr<arrow::Schema>> MapSchema(
    ROOT::RNTupleReader& reader,
    const ReaderOptions& opts = {});

// Map a single RNTuple type name string to an Arrow DataType.
// Returns an error for unsupported types.
Result<std::shared_ptr<arrow::DataType>> MapFieldType(
    const std::string& rntuple_type_name);

} // namespace rag
