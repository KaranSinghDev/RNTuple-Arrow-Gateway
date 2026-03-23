#pragma once
#include <rag/status.hpp>

#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <ROOT/RNTupleReader.hxx>
#include <ROOT/RNTupleUtil.hxx>

#include <memory>
#include <vector>

namespace rag {

// Builds a single arrow::RecordBatch from a contiguous range of RNTuple entries.
// One instance is created per RNTupleFile and reused across batches.
class BatchBuilder {
public:
    static Result<std::unique_ptr<BatchBuilder>> Create(
        ROOT::RNTupleReader& reader,
        const std::shared_ptr<arrow::Schema>& schema);

    Result<std::shared_ptr<arrow::RecordBatch>> Build(
        ROOT::NTupleSize_t start, ROOT::NTupleSize_t count);

private:
    struct IColumnAppender {
        virtual ~IColumnAppender() = default;
        virtual arrow::Status Append(ROOT::NTupleSize_t entry_id) = 0;
        virtual arrow::Result<std::shared_ptr<arrow::Array>> Finish() = 0;
    };

    explicit BatchBuilder(std::shared_ptr<arrow::Schema> schema);

    std::shared_ptr<arrow::Schema> schema_;
    std::vector<std::unique_ptr<IColumnAppender>> appenders_;
};

} // namespace rag
