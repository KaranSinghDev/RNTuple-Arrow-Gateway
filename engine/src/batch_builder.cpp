#include <rag/batch_builder.hpp>
#include "internal/column_appender.hpp"

#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/type.h>
#include <ROOT/RNTupleReader.hxx>
#include <ROOT/RNTupleView.hxx>

#include <cstdint>
#include <memory>
#include <vector>

namespace rag {

namespace {

template <typename CppType, typename ArrowBuilder>
struct NumericAppender : detail::IColumnAppender {
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

template <typename CppType, typename InnerBuilder>
struct ListAppender : detail::IColumnAppender {
    ROOT::RNTupleView<std::vector<CppType>> view;
    arrow::ListBuilder list_builder;
    InnerBuilder* value_builder;  // non-owning; lifetime tied to list_builder

    explicit ListAppender(ROOT::RNTupleView<std::vector<CppType>> v)
        : view(std::move(v))
        , list_builder(arrow::default_memory_pool(),
                       std::make_shared<InnerBuilder>())
        , value_builder(static_cast<InnerBuilder*>(list_builder.value_builder()))
    {}

    arrow::Status Append(ROOT::NTupleSize_t entry_id) override {
        ARROW_RETURN_NOT_OK(list_builder.Append());
        for (const auto& elem : view(entry_id)) {
            ARROW_RETURN_NOT_OK(value_builder->Append(
                static_cast<typename InnerBuilder::value_type>(elem)));
        }
        return arrow::Status::OK();
    }

    arrow::Result<std::shared_ptr<arrow::Array>> Finish() override {
        std::shared_ptr<arrow::Array> arr;
        ARROW_RETURN_NOT_OK(list_builder.Finish(&arr));
        return arr;
    }
};

struct BoolListAppender : detail::IColumnAppender {
    ROOT::RNTupleView<std::vector<bool>> view;
    arrow::ListBuilder list_builder;
    arrow::BooleanBuilder* value_builder;

    explicit BoolListAppender(ROOT::RNTupleView<std::vector<bool>> v)
        : view(std::move(v))
        , list_builder(arrow::default_memory_pool(),
                       std::make_shared<arrow::BooleanBuilder>())
        , value_builder(
              static_cast<arrow::BooleanBuilder*>(list_builder.value_builder()))
    {}

    arrow::Status Append(ROOT::NTupleSize_t entry_id) override {
        ARROW_RETURN_NOT_OK(list_builder.Append());
        const auto& vec = view(entry_id);
        // vector<bool> uses proxy elements; index explicitly to force bool cast.
        for (std::size_t i = 0; i < vec.size(); ++i) {
            ARROW_RETURN_NOT_OK(value_builder->Append(static_cast<bool>(vec[i])));
        }
        return arrow::Status::OK();
    }

    arrow::Result<std::shared_ptr<arrow::Array>> Finish() override {
        std::shared_ptr<arrow::Array> arr;
        ARROW_RETURN_NOT_OK(list_builder.Finish(&arr));
        return arr;
    }
};

struct BoolAppender : detail::IColumnAppender {
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

arrow::Result<std::unique_ptr<detail::IColumnAppender>> MakeAppender(
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
    case Type::LIST: {
        auto inner = std::static_pointer_cast<arrow::ListType>(arrow_type)->value_type();
        switch (inner->id()) {
        case Type::INT32:
            return std::make_unique<ListAppender<std::int32_t, arrow::Int32Builder>>(
                reader.GetView<std::vector<std::int32_t>>(field_name));
        case Type::INT64:
            return std::make_unique<ListAppender<std::int64_t, arrow::Int64Builder>>(
                reader.GetView<std::vector<std::int64_t>>(field_name));
        case Type::FLOAT:
            return std::make_unique<ListAppender<float, arrow::FloatBuilder>>(
                reader.GetView<std::vector<float>>(field_name));
        case Type::DOUBLE:
            return std::make_unique<ListAppender<double, arrow::DoubleBuilder>>(
                reader.GetView<std::vector<double>>(field_name));
        case Type::BOOL:
            return std::make_unique<BoolListAppender>(
                reader.GetView<std::vector<bool>>(field_name));
        default:
            return arrow::Status::NotImplemented(
                "No list appender for inner Arrow type: ", inner->ToString());
        }
    }
    default:
        return arrow::Status::NotImplemented(
            "No appender for Arrow type: ", arrow_type->ToString());
    }
}

} // namespace

BatchBuilder::BatchBuilder(std::shared_ptr<arrow::Schema> schema)
    : schema_(std::move(schema)) {}

BatchBuilder::~BatchBuilder() = default;

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
