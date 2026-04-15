#include <rag/reader.hpp>

#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>

#ifndef FIXTURE_LISTS_PATH
#error "FIXTURE_LISTS_PATH must be defined at compile time"
#endif

static constexpr int kRows = 5;

class ListColumnsTest : public ::testing::Test {
protected:
    std::shared_ptr<arrow::Table> table_;

    void SetUp() override {
        auto file = rag::RNTupleFile::Open(
            FIXTURE_LISTS_PATH, "fixture_lists").ValueOrDie();
        table_ = file->ReadAll().ValueOrDie();
    }
};

// ── Schema ──────────────────────────────────────────────────────────────────

TEST_F(ListColumnsTest, Schema_FieldCount) {
    EXPECT_EQ(table_->schema()->num_fields(), 2);
}

TEST_F(ListColumnsTest, Schema_Types) {
    EXPECT_TRUE(table_->schema()->field(0)->type()->Equals(arrow::list(arrow::int32())));
    EXPECT_TRUE(table_->schema()->field(1)->type()->Equals(arrow::list(arrow::float32())));
}

TEST_F(ListColumnsTest, Schema_Names) {
    EXPECT_EQ(table_->schema()->field(0)->name(), "vi32");
    EXPECT_EQ(table_->schema()->field(1)->name(), "vf32");
}

// ── Row count ───────────────────────────────────────────────────────────────

TEST_F(ListColumnsTest, RowCount) {
    EXPECT_EQ(table_->num_rows(), kRows);
}

// ── vi32 list values ─────────────────────────────────────────────────────────
// row 0=[], row 1=[1], row 2=[2,3], row 3=[4,5,6], row 4=[7,8,9,10]

TEST_F(ListColumnsTest, vi32_ListLengths) {
    auto col = std::static_pointer_cast<arrow::ListArray>(
        table_->column(0)->chunk(0));
    EXPECT_EQ(col->value_length(0), 0);
    EXPECT_EQ(col->value_length(1), 1);
    EXPECT_EQ(col->value_length(2), 2);
    EXPECT_EQ(col->value_length(3), 3);
    EXPECT_EQ(col->value_length(4), 4);
}

TEST_F(ListColumnsTest, vi32_Values) {
    auto col = std::static_pointer_cast<arrow::ListArray>(
        table_->column(0)->chunk(0));
    auto vals = std::static_pointer_cast<arrow::Int32Array>(col->values());

    // row 1 → [1]
    EXPECT_EQ(vals->Value(col->value_offset(1)), 1);

    // row 2 → [2, 3]
    EXPECT_EQ(vals->Value(col->value_offset(2)),     2);
    EXPECT_EQ(vals->Value(col->value_offset(2) + 1), 3);

    // row 4 → [7, 8, 9, 10]
    EXPECT_EQ(vals->Value(col->value_offset(4)),     7);
    EXPECT_EQ(vals->Value(col->value_offset(4) + 3), 10);
}

TEST_F(ListColumnsTest, vi32_EmptyRow) {
    auto col = std::static_pointer_cast<arrow::ListArray>(
        table_->column(0)->chunk(0));
    EXPECT_EQ(col->value_length(0), 0);
    // Offsets for row 0 and row 1 are the same (no elements consumed).
    EXPECT_EQ(col->value_offset(0), col->value_offset(1));
}

// ── vf32 list values ─────────────────────────────────────────────────────────
// row 0=[0.0], row 1=[0.5,1.0], row 2=[], row 3=[1.5], row 4=[2.0,2.5]

TEST_F(ListColumnsTest, vf32_ListLengths) {
    auto col = std::static_pointer_cast<arrow::ListArray>(
        table_->column(1)->chunk(0));
    EXPECT_EQ(col->value_length(0), 1);
    EXPECT_EQ(col->value_length(1), 2);
    EXPECT_EQ(col->value_length(2), 0);
    EXPECT_EQ(col->value_length(3), 1);
    EXPECT_EQ(col->value_length(4), 2);
}

TEST_F(ListColumnsTest, vf32_Values) {
    auto col = std::static_pointer_cast<arrow::ListArray>(
        table_->column(1)->chunk(0));
    auto vals = std::static_pointer_cast<arrow::FloatArray>(col->values());

    EXPECT_NEAR(vals->Value(col->value_offset(0)), 0.0f, 1e-6f);
    EXPECT_NEAR(vals->Value(col->value_offset(1)),     0.5f, 1e-6f);
    EXPECT_NEAR(vals->Value(col->value_offset(1) + 1), 1.0f, 1e-6f);
    EXPECT_NEAR(vals->Value(col->value_offset(3)), 1.5f, 1e-6f);
    EXPECT_NEAR(vals->Value(col->value_offset(4)),     2.0f, 1e-6f);
    EXPECT_NEAR(vals->Value(col->value_offset(4) + 1), 2.5f, 1e-6f);
}
