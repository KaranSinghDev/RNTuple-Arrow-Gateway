#pragma once
#include <arrow/result.h>
#include <arrow/status.h>

namespace rag {
using Status = arrow::Status;
template <typename T>
using Result = arrow::Result<T>;
} // namespace rag
