#include <gtest/gtest.h>
#include <rag/reader.hpp>

#include <arrow/array.h>
#include <arrow/table.h>
#include <arrow/type.h>

#include <cstdint>

static const char* kFixturePath = FIXTURE_PATH;
static const std::int64_t kFixtureRows = FIXTURE_ROWS;

class RNTupleFileTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto result = rag::RNTupleFile::Open(kFixturePath, "fixture");
        ASSERT_TRUE(result.ok()) << result.status().ToString();
        file_ = std::move(result.ValueOrDie());
    }
    std::unique_ptr<rag::RNTupleFile> file_;
};

TEST_F(RNTupleFileTest, NumRows) {
    EXPECT_EQ(file_->num_rows(), kFixtureRows);
}

TEST_F(RNTupleFileTest, SchemaMatchesExpected) {
    const auto& schema = file_->arrow_schema();
    ASSERT_EQ(schema->num_fields(), 5);
    EXPECT_EQ(*schema->field(0)->type(), *arrow::int32());
    EXPECT_EQ(*schema->field(1)->type(), *arrow::int64());
    EXPECT_EQ(*schema->field(2)->type(), *arrow::float32());
    EXPECT_EQ(*schema->field(3)->type(), *arrow::float64());
    EXPECT_EQ(*schema->field(4)->type(), *arrow::boolean());
}

TEST_F(RNTupleFileTest, ReadAllRowCount) {
    auto table_result = file_->ReadAll();
    ASSERT_TRUE(table_result.ok()) << table_result.status().ToString();
    EXPECT_EQ(table_result.ValueOrDie()->num_rows(), kFixtureRows);
}

TEST_F(RNTupleFileTest, ReadAllColumnCount) {
    auto table_result = file_->ReadAll();
    ASSERT_TRUE(table_result.ok());
    EXPECT_EQ(table_result.ValueOrDie()->num_columns(), 5);
}

TEST_F(RNTupleFileTest, ValuesCorrect_i32) {
    auto table = file_->ReadAll().ValueOrDie();
    auto col = std::static_pointer_cast<arrow::Int32Array>(
        table->GetColumnByName("i32")->chunk(0));
    for (std::int64_t i = 0; i < kFixtureRows; ++i) {
        ASSERT_EQ(col->Value(i), static_cast<std::int32_t>(i)) << "row " << i;
    }
}

TEST_F(RNTupleFileTest, ValuesCorrect_i64) {
    auto table = file_->ReadAll().ValueOrDie();
    auto col = std::static_pointer_cast<arrow::Int64Array>(
        table->GetColumnByName("i64")->chunk(0));
    for (std::int64_t i = 0; i < kFixtureRows; ++i) {
        ASSERT_EQ(col->Value(i), i * 1000LL) << "row " << i;
    }
}

TEST_F(RNTupleFileTest, ValuesCorrect_f32) {
    auto table = file_->ReadAll().ValueOrDie();
    auto col = std::static_pointer_cast<arrow::FloatArray>(
        table->GetColumnByName("f32")->chunk(0));
    for (std::int64_t i = 0; i < kFixtureRows; ++i) {
        ASSERT_FLOAT_EQ(col->Value(i), static_cast<float>(i) * 0.5f) << "row " << i;
    }
}

TEST_F(RNTupleFileTest, ValuesCorrect_bool) {
    auto table = file_->ReadAll().ValueOrDie();
    auto col = std::static_pointer_cast<arrow::BooleanArray>(
        table->GetColumnByName("b")->chunk(0));
    for (std::int64_t i = 0; i < kFixtureRows; ++i) {
        ASSERT_EQ(col->Value(i), (i % 2 == 0)) << "row " << i;
    }
}

TEST_F(RNTupleFileTest, NextBatch_returnsNullWhenDone) {
    // Exhaust all batches
    while (true) {
        auto batch = file_->NextBatch().ValueOrDie();
        if (!batch) break;
    }
    // Next call must return nullptr
    auto batch = file_->NextBatch().ValueOrDie();
    EXPECT_EQ(batch, nullptr);
}
