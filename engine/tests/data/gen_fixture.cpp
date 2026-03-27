#include <ROOT/RNTupleModel.hxx>
#include <ROOT/RNTupleWriter.hxx>

#include <cmath>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>

// Writes a small RNTuple fixture with known values for unit tests.
// Usage: gen_fixture <output_path> <nrows>
//
// Schema: i32 (int32), i64 (int64), f32 (float), f64 (double), b (bool)
// Values at row i:
//   i32 = i
//   i64 = i * 1000
//   f32 = i * 0.5f
//   f64 = i * 0.1
//   b   = (i % 2 == 0)

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "usage: gen_fixture <output_path> <nrows>\n";
        return 1;
    }
    const std::string path = argv[1];
    const std::int64_t nrows = std::stoll(argv[2]);

    auto model = ROOT::RNTupleModel::Create();
    auto f_i32 = model->MakeField<std::int32_t>("i32");
    auto f_i64 = model->MakeField<std::int64_t>("i64");
    auto f_f32 = model->MakeField<float>("f32");
    auto f_f64 = model->MakeField<double>("f64");
    auto f_b   = model->MakeField<bool>("b");

    auto writer = ROOT::RNTupleWriter::Recreate(std::move(model), "fixture", path);

    for (std::int64_t i = 0; i < nrows; ++i) {
        *f_i32 = static_cast<std::int32_t>(i);
        *f_i64 = i * 1000LL;
        *f_f32 = static_cast<float>(i) * 0.5f;
        *f_f64 = static_cast<double>(i) * 0.1;
        *f_b   = (i % 2 == 0);
        writer->Fill();
    }

    std::cout << "wrote " << nrows << " rows to " << path << "\n";
}
