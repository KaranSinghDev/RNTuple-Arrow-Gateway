#include "../service.hpp"

#include <arrow/array/array_base.h>
#include <arrow/array/array_primitive.h>
#include <arrow/flight/client.h>
#include <arrow/flight/server.h>
#include <arrow/table.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#ifndef FIXTURE_PATH
#error "FIXTURE_PATH must be defined at compile time"
#endif

// Fixture is written with ntuple name "fixture" by gen_fixture.
static constexpr int kFixtureRows = 1000;
static constexpr const char* kNtupleName = "fixture";

class FlightRoundTripTest : public ::testing::Test {
protected:
    std::unique_ptr<rag::RNTupleFlightServer> server_;
    std::unique_ptr<arrow::flight::FlightClient> client_;

    void SetUp() override {
        // Port 0 lets the OS pick a free port.
        auto loc = arrow::flight::Location::ForGrpcTcp("localhost", 0).ValueOrDie();
        server_ = std::make_unique<rag::RNTupleFlightServer>(
            std::string(FIXTURE_PATH), kNtupleName);
        ASSERT_TRUE(server_->Init(arrow::flight::FlightServerOptions(loc)).ok());

        auto client_loc = arrow::flight::Location::ForGrpcTcp(
            "localhost", server_->port()).ValueOrDie();
        client_ = arrow::flight::FlightClient::Connect(client_loc).ValueOrDie();
    }

    void TearDown() override {
        client_.reset();
        ASSERT_TRUE(server_->Shutdown().ok());
    }

    // Helper: pull all data via DoGet into a Table.
    std::shared_ptr<arrow::Table> GetTable() {
        arrow::flight::Ticket ticket{kNtupleName};
        auto reader = client_->DoGet(ticket).ValueOrDie();
        return reader->ToTable().ValueOrDie();
    }
};

TEST_F(FlightRoundTripTest, DoGet_ReturnsCorrectSchema) {
    auto table = GetTable();
    auto schema = table->schema();
    ASSERT_EQ(schema->num_fields(), 5);
    EXPECT_EQ(schema->field(0)->name(), "i32");
    EXPECT_EQ(schema->field(1)->name(), "i64");
    EXPECT_EQ(schema->field(2)->name(), "f32");
    EXPECT_EQ(schema->field(3)->name(), "f64");
    EXPECT_EQ(schema->field(4)->name(), "b");
}

TEST_F(FlightRoundTripTest, DoGet_ReturnsCorrectRowCount) {
    EXPECT_EQ(GetTable()->num_rows(), kFixtureRows);
}

TEST_F(FlightRoundTripTest, DoGet_ValuesCorrect_i32) {
    auto col = std::static_pointer_cast<arrow::Int32Array>(
        GetTable()->column(0)->chunk(0));
    EXPECT_EQ(col->Value(0),   0);
    EXPECT_EQ(col->Value(42),  42);
    EXPECT_EQ(col->Value(999), 999);
}

TEST_F(FlightRoundTripTest, DoGet_ValuesCorrect_i64) {
    auto col = std::static_pointer_cast<arrow::Int64Array>(
        GetTable()->column(1)->chunk(0));
    EXPECT_EQ(col->Value(42), 42 * 1000LL);
}

TEST_F(FlightRoundTripTest, DoGet_ValuesCorrect_bool) {
    auto col = std::static_pointer_cast<arrow::BooleanArray>(
        GetTable()->column(4)->chunk(0));
    EXPECT_TRUE(col->Value(42));   // 42 % 2 == 0
    EXPECT_FALSE(col->Value(43));  // 43 % 2 != 0
}

TEST_F(FlightRoundTripTest, GetFlightInfo_ReturnsRowCountAndEndpoint) {
    auto descriptor = arrow::flight::FlightDescriptor::Path({FIXTURE_PATH});
    auto info = client_->GetFlightInfo(descriptor).ValueOrDie();
    EXPECT_EQ(info->total_records(), kFixtureRows);
    // One endpoint pointing back at this server.
    ASSERT_EQ(info->endpoints().size(), 1u);
    EXPECT_EQ(info->endpoints()[0].ticket.ticket, kNtupleName);
}

TEST_F(FlightRoundTripTest, ListFlights_ReturnsOneFlight) {
    auto listing = client_->ListFlights().ValueOrDie();

    auto fi = listing->Next().ValueOrDie();
    ASSERT_NE(fi, nullptr);
    EXPECT_EQ(fi->total_records(), kFixtureRows);

    // Only one flight listed.
    auto fi2 = listing->Next().ValueOrDie();
    EXPECT_EQ(fi2, nullptr);
}
