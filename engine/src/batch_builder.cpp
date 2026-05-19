#include <rag/batch_builder.hpp>
#include "internal/column_appender.hpp"

#include <arrow/array/array_primitive.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/buffer.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <ROOT/RFieldBase.hxx>
#include <ROOT/RNTupleDescriptor.hxx>
#include <ROOT/RNTupleReader.hxx>
#include <ROOT/RNTupleUtil.hxx>
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

// Bulk-read one cluster's worth of a primitive column directly into an Arrow buffer.
template <typename CppType>
arrow::Result<std::shared_ptr<arrow::Array>> BulkReadCluster(
    ROOT::RNTupleReader& reader,
    const std::string& field_name,
    ROOT::DescriptorId_t cluster_id,
    ROOT::NTupleSize_t n_entries,
    const std::shared_ptr<arrow::DataType>& arrow_type)
{
    auto view = reader.GetView<CppType>(field_name);
    auto bulk = view.CreateBulk();

    ARROW_ASSIGN_OR_RAISE(
        auto buf,
        arrow::AllocateBuffer(
            static_cast<std::int64_t>(n_entries * sizeof(CppType))));

    bulk.AdoptBuffer(buf->mutable_data(), n_entries);
    bulk.ReadBulk(ROOT::RNTupleLocalIndex(cluster_id, 0), nullptr, n_entries);

    return std::make_shared<arrow::PrimitiveArray>(
        arrow_type,
        static_cast<std::int64_t>(n_entries),
        std::shared_ptr<arrow::Buffer>(std::move(buf)));
}

// Bulk-read one cluster's worth of a std::vector<CppType> column.
// Two bulk reads + Arrow offsets fixup, per Jakob Blomer's note on the ROOT forum:
//   (1) RNTupleCardinality view -> per-entry collection sizes
//   (2) inner subfield "._0" view -> flat values buffer
//   (3) convert sizes -> Arrow cumulative offsets
template <typename CppType>
arrow::Result<std::shared_ptr<arrow::Array>> BulkReadListCluster(
    ROOT::RNTupleReader& reader,
    const std::string& field_name,
    ROOT::DescriptorId_t cluster_id,
    ROOT::NTupleSize_t n_entries,
    const std::shared_ptr<arrow::DataType>& arrow_inner_type)
{
    // (1) Bulk-read cardinality (per-entry sizes).
    auto card_view =
        reader.GetView<ROOT::RNTupleCardinality<std::uint64_t>>(field_name);
    auto card_bulk = card_view.CreateBulk();

    std::vector<ROOT::RNTupleCardinality<std::uint64_t>> sizes(n_entries);
    card_bulk.AdoptBuffer(sizes.data(), n_entries);
    card_bulk.ReadBulk(ROOT::RNTupleLocalIndex(cluster_id, 0), nullptr, n_entries);

    // (2) Build Arrow's cumulative offsets buffer (int32 offsets).
    ARROW_ASSIGN_OR_RAISE(
        auto offsets_buf,
        arrow::AllocateBuffer(
            static_cast<std::int64_t>((n_entries + 1) * sizeof(std::int32_t))));
    auto* offsets = reinterpret_cast<std::int32_t*>(offsets_buf->mutable_data());
    offsets[0] = 0;
    std::int64_t running = 0;
    for (ROOT::NTupleSize_t i = 0; i < n_entries; ++i) {
        running += static_cast<std::int64_t>(sizes[i].fValue);
        offsets[i + 1] = static_cast<std::int32_t>(running);
    }
    const std::int64_t total_values = running;

    // (3) Bulk-read flat values via the inner subfield.
    auto val_view = reader.GetView<CppType>(field_name + "._0");
    auto val_bulk = val_view.CreateBulk();

    ARROW_ASSIGN_OR_RAISE(
        auto values_buf,
        arrow::AllocateBuffer(
            static_cast<std::int64_t>(total_values * sizeof(CppType))));

    if (total_values > 0) {
        val_bulk.AdoptBuffer(values_buf->mutable_data(),
                             static_cast<std::size_t>(total_values));
        val_bulk.ReadBulk(ROOT::RNTupleLocalIndex(cluster_id, 0), nullptr,
                          static_cast<std::size_t>(total_values));
    }

    auto values_array = std::make_shared<arrow::PrimitiveArray>(
        arrow_inner_type,
        total_values,
        std::shared_ptr<arrow::Buffer>(std::move(values_buf)));

    return std::make_shared<arrow::ListArray>(
        arrow::list(arrow_inner_type),
        static_cast<std::int64_t>(n_entries),
        std::shared_ptr<arrow::Buffer>(std::move(offsets_buf)),
        values_array);
}

// Per-cluster column read: bulk path for primitives, per-entry fallback for bool/lists.
arrow::Result<std::shared_ptr<arrow::Array>> ReadColumnInCluster(
    ROOT::RNTupleReader& reader,
    const std::string& field_name,
    const std::shared_ptr<arrow::DataType>& arrow_type,
    ROOT::DescriptorId_t cluster_id,
    ROOT::NTupleSize_t first_entry,
    ROOT::NTupleSize_t n_entries)
{
    using arrow::Type;
    switch (arrow_type->id()) {
    case Type::INT32:
        return BulkReadCluster<std::int32_t>(
            reader, field_name, cluster_id, n_entries, arrow_type);
    case Type::INT64:
        return BulkReadCluster<std::int64_t>(
            reader, field_name, cluster_id, n_entries, arrow_type);
    case Type::FLOAT:
        return BulkReadCluster<float>(
            reader, field_name, cluster_id, n_entries, arrow_type);
    case Type::DOUBLE:
        return BulkReadCluster<double>(
            reader, field_name, cluster_id, n_entries, arrow_type);
    case Type::LIST: {
        auto inner =
            std::static_pointer_cast<arrow::ListType>(arrow_type)->value_type();
        switch (inner->id()) {
        case Type::INT32:
            return BulkReadListCluster<std::int32_t>(
                reader, field_name, cluster_id, n_entries, inner);
        case Type::INT64:
            return BulkReadListCluster<std::int64_t>(
                reader, field_name, cluster_id, n_entries, inner);
        case Type::FLOAT:
            return BulkReadListCluster<float>(
                reader, field_name, cluster_id, n_entries, inner);
        case Type::DOUBLE:
            return BulkReadListCluster<double>(
                reader, field_name, cluster_id, n_entries, inner);
        default:
            break;  // fall through to per-entry path below
        }
        [[fallthrough]];
    }
    default: {
        // bool inner (bit-pack) and any unsupported list inner type fall back
        // to the per-entry appender used by the original BatchBuilder::Build.
        ARROW_ASSIGN_OR_RAISE(
            auto appender, MakeAppender(reader, field_name, arrow_type));
        for (auto i = first_entry; i < first_entry + n_entries; ++i) {
            ARROW_RETURN_NOT_OK(appender->Append(i));
        }
        return appender->Finish();
    }
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

Result<std::shared_ptr<arrow::Table>> BatchBuilder::BuildAllBulk(
    ROOT::RNTupleReader& reader,
    const std::shared_ptr<arrow::Schema>& schema)
{
    const auto& desc = reader.GetDescriptor();

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

    for (const auto& cluster : desc.GetClusterIterable()) {
        const auto cluster_id  = cluster.GetId();
        const auto first_entry = cluster.GetFirstEntryIndex();
        const auto n_entries   = cluster.GetNEntries();

        arrow::ArrayVector arrays;
        arrays.reserve(schema->num_fields());

        for (int i = 0; i < schema->num_fields(); ++i) {
            const auto& f = schema->field(i);
            ARROW_ASSIGN_OR_RAISE(
                auto arr,
                ReadColumnInCluster(reader, f->name(), f->type(),
                                    cluster_id, first_entry, n_entries));
            arrays.push_back(std::move(arr));
        }

        batches.push_back(arrow::RecordBatch::Make(
            schema, static_cast<std::int64_t>(n_entries), arrays));
    }

    return arrow::Table::FromRecordBatches(schema, batches);
}

} // namespace rag
