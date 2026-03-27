#pragma once
#include <arrow/array.h>
#include <arrow/status.h>
#include <ROOT/RNTupleUtil.hxx>

#include <memory>

namespace rag::detail {

struct IColumnAppender {
    virtual ~IColumnAppender() = default;
    virtual arrow::Status Append(ROOT::NTupleSize_t entry_id) = 0;
    virtual arrow::Result<std::shared_ptr<arrow::Array>> Finish() = 0;
};

} // namespace rag::detail
