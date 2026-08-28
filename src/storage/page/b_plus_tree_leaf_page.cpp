//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_leaf_page.cpp
//
// Identification: src/storage/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * @brief Init method after creating a new leaf page
 *
 * After creating a new leaf page from buffer pool, must call initialize method to set default values,
 * including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size.
 *
 * @param max_size Max size of the leaf node
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetMaxSize(max_size);
  SetSize(0);
  SetNextPageId(INVALID_PAGE_ID);
  num_tombstones_ = 0;
}

/**
 * @brief Helper function for fetching tombstones of a page.
 * @return The last `NumTombs` keys with pending deletes in this page in order of recency (oldest at front).
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetTombstones() const -> std::vector<KeyType> {
  std::vector<KeyType> tombstones;
  tombstones.reserve(num_tombstones_);
  for (size_t i = 0; i < num_tombstones_; i++) {
    BUSTUB_ASSERT(tombstones_[i] < static_cast<size_t>(GetSize()), "invalid tombstone index");
    tombstones.push_back(key_array_[tombstones_[i]]);
  }
  return tombstones;
}

/**
 * Helper methods to set/get next page id
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }
/*
 * Helper method to find and return the key associated with input "index" (a.k.a
 * array offset)
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "invalid key index");
  return key_array_[index];
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "invalid value index");
  return rid_array_[index];
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertTombstone(int index) {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "invalid tombstone index");
  BUSTUB_ASSERT(num_tombstones_ < static_cast<size_t>(LEAF_PAGE_TOMB_CNT), "tombstone buffer is full");
  for (size_t i = 0; i < num_tombstones_; i++) {
    BUSTUB_ASSERT(tombstones_[i] != static_cast<size_t>(index), "entry already tombstoned");
  }
  tombstones_[num_tombstones_] = static_cast<size_t>(index);
  num_tombstones_++;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveTombstoneAt(size_t tombstone_index) {
  BUSTUB_ASSERT(tombstone_index < num_tombstones_, "invalid tombstone position");
  for (size_t i = tombstone_index; i + 1 < num_tombstones_; i++) {
    tombstones_[i] = tombstones_[i + 1];
  }
  num_tombstones_--;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetTombstoneCount() const -> size_t { return num_tombstones_; }

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertAt(int index, const KeyType &key, const ValueType &value) {
  BUSTUB_ASSERT(index >= 0 && index <= GetSize(), "invalid insert index");
  BUSTUB_ASSERT(GetSize() < GetMaxSize(), "leaf page is full");
  for (int i = GetSize(); i > index; --i) {
    key_array_[i] = key_array_[i - 1];
    rid_array_[i] = rid_array_[i - 1];
  }
  for (size_t i = 0; i < num_tombstones_; i++) {
    if (tombstones_[i] >= static_cast<size_t>(index)) {
      tombstones_[i]++;
    }
  }
  key_array_[index] = key;
  rid_array_[index] = value;
  IncreaseSize(1);
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAt(int index) {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "invalid remove index");
  for (size_t i = 0; i < num_tombstones_; i++) {
    BUSTUB_ASSERT(tombstones_[i] != static_cast<size_t>(index), "remove tombstone before physical deletion");
  }
  for (int i = index; i < GetSize() - 1; i++) {
    key_array_[i] = key_array_[i + 1];
    rid_array_[i] = rid_array_[i + 1];
  }
  for (size_t i = 0; i < num_tombstones_; i++) {
    if (tombstones_[i] > static_cast<size_t>(index)) {
      tombstones_[i]--;
    }
  }
  DecreaseSize(1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
