// Generates a benchmark RNTuple fixture at a specified path and row count.
// Schema: 5 primitives (i32, i64, f32, f64, b) + 2 list columns (vi32, vf32).
// vi32: row i has (i%8 + 1) int32 elements  → avg 4.5 elements/row
// vf32: row i has (i%6 + 1) float elements  → avg 3.5 elements/row
//
// Usage: gen_bench_fixture <output_path> <num_rows>

#include <ROOT/RNTupleModel.hxx>
#include <ROOT/RNTupleWriter.hxx>

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <output_path> <num_rows>\n";
        return 1;
    }
    const std::string path    = argv[1];
    const std::int64_t nrows  = std::stoll(argv[2]);

    auto model = ROOT::RNTupleModel::Create();

    auto fi32  = model->MakeField<std::int32_t>("i32");
    auto fi64  = model->MakeField<std::int64_t>("i64");
    auto ff32  = model->MakeField<float>("f32");
    auto ff64  = model->MakeField<double>("f64");
    auto fb    = model->MakeField<bool>("b");
    auto fvi32 = model->MakeField<std::vector<std::int32_t>>("vi32");
    auto fvf32 = model->MakeField<std::vector<float>>("vf32");

    auto writer = ROOT::RNTupleWriter::Recreate(std::move(model), "bench", path);

    for (std::int64_t i = 0; i < nrows; ++i) {
        *fi32 = static_cast<std::int32_t>(i % INT32_MAX);
        *fi64 = i * 1000LL;
        *ff32 = static_cast<float>(i) * 0.5f;
        *ff64 = static_cast<double>(i) * 0.1;
        *fb   = (i % 2 == 0);

        int n32 = static_cast<int>(i % 8) + 1;
        fvi32->resize(n32);
        for (int k = 0; k < n32; ++k)
            (*fvi32)[k] = static_cast<std::int32_t>((i + k) % INT32_MAX);

        int nf32 = static_cast<int>(i % 6) + 1;
        fvf32->resize(nf32);
        for (int k = 0; k < nf32; ++k)
            (*fvf32)[k] = static_cast<float>(i + k) * 0.25f;

        writer->Fill();
    }

    std::cout << "Written " << nrows << " rows to " << path << "\n";
    return 0;
}
