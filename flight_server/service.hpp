#pragma once
#include <arrow/flight/server.h>
#include <string>

namespace rag {

class RNTupleFlightServer : public arrow::flight::FlightServerBase {
public:
    RNTupleFlightServer(std::string file_path, std::string ntuple_name);

    arrow::Status ListFlights(
        const arrow::flight::ServerCallContext& context,
        const arrow::flight::Criteria* criteria,
        std::unique_ptr<arrow::flight::FlightListing>* listings) override;

    arrow::Status GetFlightInfo(
        const arrow::flight::ServerCallContext& context,
        const arrow::flight::FlightDescriptor& descriptor,
        std::unique_ptr<arrow::flight::FlightInfo>* info) override;

    arrow::Status DoGet(
        const arrow::flight::ServerCallContext& context,
        const arrow::flight::Ticket& request,
        std::unique_ptr<arrow::flight::FlightDataStream>* stream) override;

private:
    std::string file_path_;
    std::string ntuple_name_;
};

} // namespace rag
