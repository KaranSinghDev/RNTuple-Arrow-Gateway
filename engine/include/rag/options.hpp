#pragma once
#include <cstddef>
#include <string>
#include <vector>

namespace rag {

struct ReaderOptions {
    std::size_t batch_size = 64 * 1024;      // rows per RecordBatch
    std::vector<std::string> columns;         // empty = all top-level fields
};

} // namespace rag
