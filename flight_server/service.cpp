#include "service.hpp"

#include <rag/reader.hpp>

#include <arrow/flight/server.h>
#include <arrow/flight/types.h>
#include <arrow/table.h>

#include <memory>
#include <vector>

namespace rag {

RNTupleFlightServer::RNTupleFlightServer(std::string file_path, std::string ntuple_name)
    : file_path_(std::move(file_path)), ntuple_name_(std::move(ntuple_name)) {}

arrow::Status RNTupleFlightServer::GetFlightInfo(
    const arrow::flight::ServerCallContext&,
    const arrow::flight::FlightDescriptor& descriptor,
    std::unique_ptr<arrow::flight::FlightInfo>* info)
{
    ARROW_ASSIGN_OR_RAISE(auto file, rag::RNTupleFile::Open(file_path_, ntuple_name_));
    auto schema = file->arrow_schema();

    arrow::flight::FlightEndpoint endpoint{
        arrow::flight::Ticket{ntuple_name_},
        /*locations=*/{},
        /*expiration_time=*/std::nullopt,
        /*app_metadata=*/""
    };

    ARROW_ASSIGN_OR_RAISE(auto fi,
        arrow::flight::FlightInfo::Make(
            *schema, descriptor, {endpoint},
            file->num_rows(), /*total_bytes=*/-1));
    *info = std::make_unique<arrow::flight::FlightInfo>(std::move(fi));
    return arrow::Status::OK();
}

arrow::Status RNTupleFlightServer::ListFlights(
    const arrow::flight::ServerCallContext&,
    const arrow::flight::Criteria*,
    std::unique_ptr<arrow::flight::FlightListing>* listings)
{
    ARROW_ASSIGN_OR_RAISE(auto file, rag::RNTupleFile::Open(file_path_, ntuple_name_));
    auto schema = file->arrow_schema();

    auto descriptor = arrow::flight::FlightDescriptor::Path({file_path_});
    arrow::flight::FlightEndpoint endpoint{
        arrow::flight::Ticket{ntuple_name_},
        /*locations=*/{},
        /*expiration_time=*/std::nullopt,
        /*app_metadata=*/""
    };

    ARROW_ASSIGN_OR_RAISE(auto fi,
        arrow::flight::FlightInfo::Make(
            *schema, descriptor, {endpoint},
            file->num_rows(), /*total_bytes=*/-1));

    *listings = std::make_unique<arrow::flight::SimpleFlightListing>(
        std::vector<arrow::flight::FlightInfo>{std::move(fi)});
    return arrow::Status::OK();
}

arrow::Status RNTupleFlightServer::DoGet(
    const arrow::flight::ServerCallContext&,
    const arrow::flight::Ticket& request,
    std::unique_ptr<arrow::flight::FlightDataStream>* stream)
{
    ARROW_ASSIGN_OR_RAISE(auto file,
        rag::RNTupleFile::Open(file_path_, request.ticket));
    ARROW_ASSIGN_OR_RAISE(auto table, file->ReadAll());

    auto reader = std::make_shared<arrow::TableBatchReader>(table);
    *stream = std::make_unique<arrow::flight::RecordBatchStream>(reader);
    return arrow::Status::OK();
}

} // namespace rag
