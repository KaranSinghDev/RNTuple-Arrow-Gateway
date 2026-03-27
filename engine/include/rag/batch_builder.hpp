#pragma once
#include <rag/status.hpp>

#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <ROOT/RNTupleReader.hxx>
#include <ROOT/RNTupleUtil.hxx>

#include <memory>
#include <vector>

namespace rag::detail { struct IColumnAppender; }

namespace rag {

class BatchBuilder {
public:
    static Result<std::unique_ptr<BatchBuilder>> Create(
        ROOT::RNTupleReader& reader,
        const std::shared_ptr<arrow::Schema>& schema);

    ~BatchBuilder();

    Result<std::shared_ptr<arrow::RecordBatch>> Build(
        ROOT::NTupleSize_t start, ROOT::NTupleSize_t count);

private:
    explicit BatchBuilder(std::shared_ptr<arrow::Schema> schema);

    std::shared_ptr<arrow::Schema> schema_;
    std::vector<std::unique_ptr<detail::IColumnAppender>> appenders_;
};

} // namespace rag
