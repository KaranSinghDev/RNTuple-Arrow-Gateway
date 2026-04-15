#include <ROOT/RNTupleModel.hxx>
#include <ROOT/RNTupleWriter.hxx>

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

// Generates fixture_lists.root with 5 rows and two list columns:
//   vi32: list<int32>  — row i has i elements [i*(i-1)/2 .. i*(i+1)/2 - 1]
//   vf32: list<float>  — row i has (2 - i%3) elements, value = i * 0.5f each
//
// Concrete values (easy to verify in tests):
//   row 0: vi32=[]           vf32=[0.0]
//   row 1: vi32=[1]          vf32=[0.5, 1.0]
//   row 2: vi32=[2, 3]       vf32=[]
//   row 3: vi32=[4, 5, 6]    vf32=[1.5]
//   row 4: vi32=[7, 8, 9,10] vf32=[2.0, 2.5]

static constexpr int kRows = 5;

int main(int argc, char* argv[]) {
    const std::string path = (argc > 1) ? argv[1] : "fixture_lists.root";

    auto model = ROOT::RNTupleModel::Create();
    model->MakeField<std::vector<std::int32_t>>("vi32");
    model->MakeField<std::vector<float>>("vf32");

    auto writer = ROOT::RNTupleWriter::Recreate(std::move(model), "fixture_lists", path);
    auto& entry = writer->GetModel().GetDefaultEntry();
    auto vi32 = entry.GetPtr<std::vector<std::int32_t>>("vi32");
    auto vf32 = entry.GetPtr<std::vector<float>>("vf32");

    // row 0
    *vi32 = {};
    *vf32 = {0.0f};
    writer->Fill();

    // row 1
    *vi32 = {1};
    *vf32 = {0.5f, 1.0f};
    writer->Fill();

    // row 2
    *vi32 = {2, 3};
    *vf32 = {};
    writer->Fill();

    // row 3
    *vi32 = {4, 5, 6};
    *vf32 = {1.5f};
    writer->Fill();

    // row 4
    *vi32 = {7, 8, 9, 10};
    *vf32 = {2.0f, 2.5f};
    writer->Fill();

    std::cout << "Generated " << path << " (" << kRows << " rows)\n";
    return 0;
}
