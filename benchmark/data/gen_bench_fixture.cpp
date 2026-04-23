// Generates a benchmark RNTuple fixture at a specified path and row count.
// Schema: 5 primitives (i32, i64, f32, f64, b) + 2 list columns (vi32, vf32).
// vi32: row i has (i%8 + 1) int32 elements  → avg 4.5 elements/row
// vf32: row i has (i%6 + 1) float elements  → avg 3.5 elements/row
//
// Usage: gen_bench_fixture <output_path> <num_rows>

// Uses seeded mt19937 so fixtures are reproducible across runs.
// Values are pseudo-random (not sequential) — gives realistic compression
// ratios comparable to real HEP data.

#include <ROOT/RNTupleModel.hxx>
#include <ROOT/RNTupleWriter.hxx>

#include <cstdint>
#include <iostream>
#include <random>
#include <string>
#include <vector>

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <output_path> <num_rows>\n";
        return 1;
    }
    const std::string  path  = argv[1];
    const std::int64_t nrows = std::stoll(argv[2]);

    std::mt19937 rng(42);
    std::uniform_int_distribution<std::int32_t> di32(0, 1'000'000);
    std::uniform_int_distribution<std::int64_t> di64(0, 1'000'000'000'000LL);
    std::uniform_real_distribution<float>       df32(0.f, 1000.f);
    std::uniform_real_distribution<double>      df64(0.0, 10000.0);
    std::bernoulli_distribution                 dbool(0.5);
    std::uniform_int_distribution<int>          dlen32(1, 8);
    std::uniform_int_distribution<int>          dlenf32(1, 6);

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
        *fi32 = di32(rng);
        *fi64 = di64(rng);
        *ff32 = df32(rng);
        *ff64 = df64(rng);
        *fb   = dbool(rng);

        int n32 = dlen32(rng);
        fvi32->resize(n32);
        for (int k = 0; k < n32; ++k) (*fvi32)[k] = di32(rng);

        int nf = dlenf32(rng);
        fvf32->resize(nf);
        for (int k = 0; k < nf; ++k) (*fvf32)[k] = df32(rng);

        writer->Fill();
    }

    std::cout << "Written " << nrows << " rows to " << path << "\n";
    return 0;
}
