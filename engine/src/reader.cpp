#include <rag/reader.hpp>
#include <rag/schema.hpp>

#include <arrow/table.h>
#include <ROOT/RNTupleReader.hxx>

#include <algorithm>
#include <vector>

namespace rag {

Result<std::unique_ptr<RNTupleFile>> RNTupleFile::Open(
    const std::string& path,
    const std::string& ntuple_name,
    ReaderOptions opts)
{
    auto file = std::unique_ptr<RNTupleFile>(new RNTupleFile());
    file->opts_ = opts;

    file->reader_ = ROOT::RNTupleReader::Open(ntuple_name, path);
    if (!file->reader_) {
        return arrow::Status::IOError(
            "Failed to open RNTuple \"", ntuple_name, "\" in \"", path, "\"");
    }

    ARROW_ASSIGN_OR_RAISE(file->schema_, MapSchema(*file->reader_, opts));

    ARROW_ASSIGN_OR_RAISE(
        file->batch_builder_,
        BatchBuilder::Create(*file->reader_, file->schema_));

    return file;
}

std::int64_t RNTupleFile::num_rows() const {
    return static_cast<std::int64_t>(reader_->GetNEntries());
}

Result<std::shared_ptr<arrow::RecordBatch>> RNTupleFile::NextBatch() {
    const auto total = static_cast<ROOT::NTupleSize_t>(num_rows());
    if (next_entry_ >= total) return nullptr;

    const auto count = std::min(
        static_cast<ROOT::NTupleSize_t>(opts_.batch_size),
        total - next_entry_);

    ARROW_ASSIGN_OR_RAISE(auto batch, batch_builder_->Build(next_entry_, count));
    next_entry_ += count;
    return batch;
}

Result<std::shared_ptr<arrow::Table>> RNTupleFile::ReadAll() {
    next_entry_ = 0;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

    while (true) {
        ARROW_ASSIGN_OR_RAISE(auto batch, NextBatch());
        if (!batch) break;
        batches.push_back(std::move(batch));
    }

    return arrow::Table::FromRecordBatches(schema_, batches);
}

} // namespace rag
