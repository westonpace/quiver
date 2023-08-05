#include "quiver/sort/sort.h"

#include <bits/iterator_concepts.h>

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <memory>
#include <numeric>
#include <stdexcept>
#include <string_view>
#include <utility>

#include "quiver/core/array.h"
#include "quiver/row/row_p.h"

namespace quiver::sort {

class AllColumnsBinarySorterImpl : public AllColumnsBinarySorter {
 public:
  AllColumnsBinarySorterImpl(Storage* storage) : storage_(storage) {}

  Status Sort(const ReadOnlyBatch& unsorted, Batch* sorted) override {
    std::unique_ptr<row::RowEncoder> encoder;
    QUIVER_RETURN_NOT_OK(row::RowEncoder::Create(unsorted.schema(), storage_, &encoder));
    int64_t throwaway;
    QUIVER_RETURN_NOT_OK(encoder->Append(unsorted, &throwaway));

    std::span<uint8_t> data = storage_->DebugBuffer();
    std::span<uint8_t> referenced_data =
        data.subspan(0, unsorted.length() * encoder->row_width());

    std::cout << "Before" << std::endl;
    buffer::PrintBuffer(referenced_data, 1, 0, 1000, std::cout);

    // TODO: It should be possible to use a custom iterator to do this without needing
    // a vector and the copy
    std::vector<std::string_view> row_views;
    row_views.reserve(unsorted.length());
    for (int i = 0; i < unsorted.length(); i++) {
      row_views.emplace_back(reinterpret_cast<char*>(referenced_data.data()) +
                                 (static_cast<size_t>(i * encoder->row_width())),
                             encoder->row_width());
    }

    std::stable_sort(row_views.begin(), row_views.end());

    // Continuation of above TODO, this is very inefficient
    std::vector<uint8_t> referenced_data_copy(referenced_data.size());
    auto* copy_itr = referenced_data_copy.data();
    for (const auto& row_view : row_views) {
      std::cout << "Copying offset="
                << (row_view.data() -
                    reinterpret_cast<const char*>(referenced_data.data()))
                << " bytes: " << row_view.size()
                << " into offset=" << (copy_itr - referenced_data_copy.data())
                << std::endl;
      std::memcpy(copy_itr, row_view.data(), row_view.size());
      copy_itr += row_view.size();
    }

    std::memcpy(referenced_data.data(), referenced_data_copy.data(),
                referenced_data_copy.size());

    std::cout << "After" << std::endl;
    buffer::PrintBuffer(referenced_data, 1, 0, 1000, std::cout);

    std::unique_ptr<row::RowDecoder> decoder;
    QUIVER_RETURN_NOT_OK(row::RowDecoder::Create(unsorted.schema(), storage_, &decoder));

    std::vector<int64_t> all_indices(unsorted.length());
    std::iota(all_indices.begin(), all_indices.end(), 0);
    return decoder->Load(all_indices, sorted);
  }

 private:
  Storage* storage_;
};

Status AllColumnsBinarySorter::Create(Storage* storage,
                                      std::unique_ptr<AllColumnsBinarySorter>* out) {
  if (!storage->in_memory()) {
    return Status::NotImplemented("Disk based sorting not implemented");
  }
  *out = std::make_unique<AllColumnsBinarySorterImpl>(storage);
  return Status::OK();
}
}  // namespace quiver::sort
