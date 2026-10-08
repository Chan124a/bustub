//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.cpp
//
// Identification: src/storage/index/index_iterator.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/**
 * @note you can change the destructor/constructor method here
 * set your own input parameters
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(std::shared_ptr<TracedBufferPoolManager> bpm, ReadPageGuard guard, int index,
                                  const KeyComparator &comparator)
    : bpm_(std::move(bpm)),
      guard_(std::make_shared<ReadPageGuard>(std::move(guard))),
      comparator_(comparator),
      index_(index) {
  SeekToLiveEntry();
}

FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

FULL_INDEX_TEMPLATE_ARGUMENTS
void INDEXITERATOR_TYPE::SeekToLiveEntry() {
  while (guard_ != nullptr) {
    const auto *leaf = guard_->template As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator, NumTombs>>();
    auto tombstones = leaf->GetTombstones();
    while (index_ < leaf->GetSize()) {
      auto key = leaf->KeyAt(index_);
      bool deleted = false;
      for (const auto &tombstone : tombstones) {
        if ((*comparator_)(key, tombstone) == 0) {
          deleted = true;
          break;
        }
      }
      if (!deleted) {
        key_ = key;
        value_ = leaf->ValueAt(index_);
        return;
      }
      ++index_;
    }
    auto next = leaf->GetNextPageId();
    guard_.reset();
    if (next == INVALID_PAGE_ID) {
      return;
    }
    guard_ = std::make_shared<ReadPageGuard>(bpm_->ReadPage(next));
    index_ = 0;
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() const -> bool { return guard_ == nullptr; }

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() const -> reference {
  BUSTUB_ASSERT(!IsEnd(), "cannot dereference the end iterator");
  return {key_, value_};
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  BUSTUB_ASSERT(!IsEnd(), "cannot increment the end iterator");
  ++index_;
  SeekToLiveEntry();
  return *this;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const INDEXITERATOR_TYPE &itr) const -> bool {
  if (guard_ == nullptr || itr.guard_ == nullptr) {
    return guard_ == nullptr && itr.guard_ == nullptr;
  }
  return guard_->GetPageId() == itr.guard_->GetPageId() && index_ == itr.index_;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const INDEXITERATOR_TYPE &itr) const -> bool { return !(*this == itr); }

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
