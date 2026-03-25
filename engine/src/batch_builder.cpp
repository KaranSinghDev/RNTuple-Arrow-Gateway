#include <rag/batch_builder.hpp>

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>
#include <ROOT/RNTupleReader.hxx>
#include <ROOT/RNTupleView.hxx>

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <vector>

namespace rag {

namespace {

// Typed appender for fixed-size numeric types (int32, int64, float32, float64).
template <typename CppType, typename ArrowBuilder>
struct NumericAppender : BatchBuilder::IColumnAppender {
    ROOT::RNTupleView<CppType> view;
    ArrowBuilder builder;

    explicit NumericAppender(ROOT::RNTupleView<CppType> v) : view(std::move(v)) {}

    arrow::Status Append(ROOT::NTupleSize_t entry_id) override {
        return builder.Append(static_cast<typename ArrowBuilder::value_type>(view(entry_id)));
    }

    arrow::Result<std::shared_ptr<arrow::Array>> Finish() override {
        std::shared_ptr<arrow::Array> arr;
        ARROW_RETURN_NOT_OK(builder.Finish(&arr));
        return arr;
    }
};

// Bool appender: RNTuple returns bool (byte), Arrow BooleanBuilder bit-packs.
struct BoolAppender : BatchBuilder::IColumnAppender {
    ROOT::RNTupleView<bool> view;
    arrow::BooleanBuilder builder;

    explicit BoolAppender(ROOT::RNTupleView<bool> v) : view(std::move(v)) {}

    arrow::Status Append(ROOT::NTupleSize_t entry_id) override {
        return builder.Append(view(entry_id));
    }

    arrow::Result<std::shared_ptr<arrow::Array>> Finish() override {
        std::shared_ptr<arrow::Array> arr;
        ARROW_RETURN_NOT_OK(builder.Finish(&arr));
        return arr;
    }
};

arrow::Result<std::unique_ptr<BatchBuilder::IColumnAppender>> MakeAppender(
    ROOT::RNTupleReader& reader,
    const std::string& field_name,
    const std::shared_ptr<arrow::DataType>& arrow_type)
{
    using arrow::Type;
    switch (arrow_type->id()) {
    case Type::INT32:
        return std::make_unique<NumericAppender<std::int32_t, arrow::Int32Builder>>(
            reader.GetView<std::int32_t>(field_name));
    case Type::INT64:
        return std::make_unique<NumericAppender<std::int64_t, arrow::Int64Builder>>(
            reader.GetView<std::int64_t>(field_name));
    case Type::UINT32:
        return std::make_unique<NumericAppender<std::uint32_t, arrow::UInt32Builder>>(
            reader.GetView<std::uint32_t>(field_name));
    case Type::UINT64:
        return std::make_unique<NumericAppender<std::uint64_t, arrow::UInt64Builder>>(
            reader.GetView<std::uint64_t>(field_name));
    case Type::FLOAT:
        return std::make_unique<NumericAppender<float, arrow::FloatBuilder>>(
            reader.GetView<float>(field_name));
    case Type::DOUBLE:
        return std::make_unique<NumericAppender<double, arrow::DoubleBuilder>>(
            reader.GetView<double>(field_name));
    case Type::BOOL:
        return std::make_unique<BoolAppender>(reader.GetView<bool>(field_name));
    default:
        return arrow::Status::NotImplemented(
            "No appender for Arrow type: ", arrow_type->ToString());
    }
}

} // namespace

BatchBuilder::BatchBuilder(std::shared_ptr<arrow::Schema> schema)
    : schema_(std::move(schema)) {}

Result<std::unique_ptr<BatchBuilder>> BatchBuilder::Create(
    ROOT::RNTupleReader& reader,
    const std::shared_ptr<arrow::Schema>& schema)
{
    auto builder = std::unique_ptr<BatchBuilder>(new BatchBuilder(schema));
    builder->appenders_.reserve(schema->num_fields());

    for (int i = 0; i < schema->num_fields(); ++i) {
        const auto& f = schema->field(i);
        ARROW_ASSIGN_OR_RAISE(auto appender, MakeAppender(reader, f->name(), f->type()));
        builder->appenders_.push_back(std::move(appender));
    }
    return builder;
}

Result<std::shared_ptr<arrow::RecordBatch>> BatchBuilder::Build(
    ROOT::NTupleSize_t start, ROOT::NTupleSize_t count)
{
    // Reserve capacity in each builder.
    for (auto& app : appenders_) {
        if (auto* num = dynamic_cast<IColumnAppender*>(app.get())) {
            (void)num; // reserve happens in individual builders below
        }
    }

    for (ROOT::NTupleSize_t i = start; i < start + count; ++i) {
        for (auto& app : appenders_) {
            ARROW_RETURN_NOT_OK(app->Append(i));
        }
    }

    arrow::ArrayVector arrays;
    arrays.reserve(appenders_.size());
    for (auto& app : appenders_) {
        ARROW_ASSIGN_OR_RAISE(auto arr, app->Finish());
        arrays.push_back(std::move(arr));
    }

    return arrow::RecordBatch::Make(schema_, static_cast<std::int64_t>(count), arrays);
}

} // namespace rag
