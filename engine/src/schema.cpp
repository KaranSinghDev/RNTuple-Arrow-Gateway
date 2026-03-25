#include <rag/schema.hpp>

#include <arrow/type.h>
#include <ROOT/RNTupleDescriptor.hxx>
#include <ROOT/RNTupleReader.hxx>

#include <string>
#include <unordered_map>

namespace rag {

namespace {

const std::unordered_map<std::string, std::shared_ptr<arrow::DataType>> kTypeMap = {
    {"float",          arrow::float32()},
    {"double",         arrow::float64()},
    {"bool",           arrow::boolean()},
    {"std::int32_t",   arrow::int32()},
    {"std::int64_t",   arrow::int64()},
    {"std::uint32_t",  arrow::uint32()},
    {"std::uint64_t",  arrow::uint64()},
};

} // namespace

Result<std::shared_ptr<arrow::DataType>> MapFieldType(
    const std::string& rntuple_type_name)
{
    auto it = kTypeMap.find(rntuple_type_name);
    if (it == kTypeMap.end()) {
        return arrow::Status::NotImplemented(
            "RNTuple type not supported in M1: \"", rntuple_type_name, "\"");
    }
    return it->second;
}

Result<std::shared_ptr<arrow::Schema>> MapSchema(
    const ROOT::RNTupleReader& reader,
    const ReaderOptions& opts)
{
    const auto& desc = reader.GetDescriptor();
    const auto& want = opts.columns;

    arrow::FieldVector fields;
    for (const auto& field_desc : desc.GetTopLevelFields()) {
        const std::string& name = field_desc.GetFieldName();

        if (!want.empty()) {
            bool requested = false;
            for (const auto& col : want) {
                if (col == name) { requested = true; break; }
            }
            if (!requested) continue;
        }

        ARROW_ASSIGN_OR_RAISE(auto arrow_type, MapFieldType(field_desc.GetTypeName()));
        fields.push_back(arrow::field(name, arrow_type));
    }

    if (fields.empty()) {
        return arrow::Status::Invalid("No supported fields found in RNTuple");
    }

    return arrow::schema(fields);
}

} // namespace rag
