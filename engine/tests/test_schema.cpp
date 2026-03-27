#include <gtest/gtest.h>
#include <rag/schema.hpp>
#include <ROOT/RNTupleReader.hxx>
#include <arrow/type.h>

static const char* kFixturePath = FIXTURE_PATH;

TEST(SchemaMapper, MapFieldType_primitives) {
    EXPECT_EQ(*rag::MapFieldType("float").ValueOrDie(),        *arrow::float32());
    EXPECT_EQ(*rag::MapFieldType("double").ValueOrDie(),       *arrow::float64());
    EXPECT_EQ(*rag::MapFieldType("bool").ValueOrDie(),         *arrow::boolean());
    EXPECT_EQ(*rag::MapFieldType("std::int32_t").ValueOrDie(), *arrow::int32());
    EXPECT_EQ(*rag::MapFieldType("std::int64_t").ValueOrDie(), *arrow::int64());
}

TEST(SchemaMapper, MapFieldType_unsupported_returns_error) {
    auto result = rag::MapFieldType("std::vector<float>");
    EXPECT_FALSE(result.ok());
}

TEST(SchemaMapper, MapSchema_fixture) {
    auto reader = ROOT::RNTupleReader::Open("fixture", kFixturePath);
    ASSERT_NE(reader, nullptr);

    auto schema_result = rag::MapSchema(*reader);
    ASSERT_TRUE(schema_result.ok()) << schema_result.status().ToString();

    auto schema = schema_result.ValueOrDie();
    EXPECT_EQ(schema->num_fields(), 5);

    EXPECT_EQ(schema->field(0)->name(), "i32");
    EXPECT_EQ(*schema->field(0)->type(), *arrow::int32());

    EXPECT_EQ(schema->field(1)->name(), "i64");
    EXPECT_EQ(*schema->field(1)->type(), *arrow::int64());

    EXPECT_EQ(schema->field(2)->name(), "f32");
    EXPECT_EQ(*schema->field(2)->type(), *arrow::float32());

    EXPECT_EQ(schema->field(3)->name(), "f64");
    EXPECT_EQ(*schema->field(3)->type(), *arrow::float64());

    EXPECT_EQ(schema->field(4)->name(), "b");
    EXPECT_EQ(*schema->field(4)->type(), *arrow::boolean());
}
