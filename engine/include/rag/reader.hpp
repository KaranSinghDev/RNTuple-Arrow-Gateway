#pragma once
#include <rag/batch_builder.hpp>
#include <rag/options.hpp>
#include <rag/status.hpp>

#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <ROOT/RNTupleReader.hxx>

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace rag {

using BatchCallback = std::function<Status(std::shared_ptr<arrow::RecordBatch>)>;

class RNTupleFile {
public:
    static Result<std::unique_ptr<RNTupleFile>> Open(
        const std::string& path,
        const std::string& ntuple_name,
        ReaderOptions opts = {});

    const std::shared_ptr<arrow::Schema>& arrow_schema() const { return schema_; }
    std::int64_t num_rows() const;

    // Returns the next batch, or nullptr when all entries are consumed.
    Result<std::shared_ptr<arrow::RecordBatch>> NextBatch();

    // Materialise all entries into a single Table.
    Result<std::shared_ptr<arrow::Table>> ReadAll();

    // Push-based: resets cursor and invokes on_batch for every batch.
    Status StreamBatches(BatchCallback on_batch);

private:
    RNTupleFile() = default;

    std::unique_ptr<ROOT::RNTupleReader> reader_;
    std::shared_ptr<arrow::Schema> schema_;
    std::unique_ptr<BatchBuilder> batch_builder_;
    ReaderOptions opts_;
    ROOT::NTupleSize_t next_entry_ = 0;
};

} // namespace rag
