#include "service.hpp"

#include <arrow/flight/server.h>

#include <csignal>
#include <iostream>
#include <string>

int main(int argc, char* argv[]) {
    std::string file_path;
    std::string ntuple_name = "ntuple";
    int port = 9090;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if ((arg == "--file" || arg == "-f") && i + 1 < argc) {
            file_path = argv[++i];
        } else if ((arg == "--ntuple" || arg == "-n") && i + 1 < argc) {
            ntuple_name = argv[++i];
        } else if ((arg == "--port" || arg == "-p") && i + 1 < argc) {
            port = std::stoi(argv[++i]);
        }
    }

    if (file_path.empty()) {
        std::cerr << "Usage: rag-flight-server --file <path.root> "
                     "[--ntuple <name>] [--port <port>]\n";
        return 1;
    }

    auto location_result = arrow::flight::Location::ForGrpcTcp("0.0.0.0", port);
    if (!location_result.ok()) {
        std::cerr << "Bad location: " << location_result.status().ToString() << "\n";
        return 1;
    }

    arrow::flight::FlightServerOptions options(*location_result);
    auto server = std::make_unique<rag::RNTupleFlightServer>(file_path, ntuple_name);

    auto status = server->Init(options);
    if (!status.ok()) {
        std::cerr << "Failed to start server: " << status.ToString() << "\n";
        return 1;
    }

    std::cout << "rag-flight-server listening on port " << server->port()
              << "  (file=" << file_path << "  ntuple=" << ntuple_name << ")\n";

    (void)server->SetShutdownOnSignals({SIGTERM, SIGINT});
    (void)server->Wait();
    return 0;
}
