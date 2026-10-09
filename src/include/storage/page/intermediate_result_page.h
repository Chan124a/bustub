#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>

#include "storage/table/tuple.h"

namespace bustub {

/**
 * Page to hold the intermediate data for external merge sort and hash join.
 * Supports variable-length tuples.
 */
class IntermediateResultPage {
 public:
  void Init() {
    used_ = 0;
    count_ = 0;
  }

  auto Append(const Tuple &tuple) -> bool {
    const auto length = tuple.GetLength();
    if (length + sizeof(length) > BUSTUB_PAGE_SIZE - sizeof(IntermediateResultPage) - used_) {
      return false;
    }
    std::memcpy(data_ + used_, &length, sizeof(length));
    std::memcpy(data_ + used_ + sizeof(length), tuple.GetData(), length);
    used_ += sizeof(length) + length;
    count_++;
    return true;
  }

  auto ReadTuple(uint32_t *offset) const -> Tuple {
    uint32_t length;
    std::memcpy(&length, data_ + *offset, sizeof(length));
    auto tuple = Tuple(RID{}, data_ + *offset + sizeof(length), length);
    *offset += sizeof(length) + length;
    return tuple;
  }

  auto GetUsed() const -> uint32_t { return used_; }
  auto GetCount() const -> uint32_t { return count_; }

 private:
  uint32_t used_{0};
  uint32_t count_{0};
  char data_[0];
};

static_assert(sizeof(IntermediateResultPage) <= BUSTUB_PAGE_SIZE);

}  // namespace bustub
