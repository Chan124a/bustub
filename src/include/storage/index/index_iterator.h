//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // 禁用拷贝是因为成员对象ReadPageGuard page_guard_ 已经禁用拷贝了
  DISALLOW_COPY(IndexIterator);

  IndexIterator(BufferPoolManager *bpm, page_id_t page_id, int index);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool;

  auto operator!=(const IndexIterator &itr) const -> bool;

 private:
  void AdvanceToLive();

  // add your own private member variables here
  BufferPoolManager *bpm_{nullptr};
  page_id_t page_id_{INVALID_PAGE_ID};
  ReadPageGuard page_guard_{};
  const B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page_{nullptr};
  int index_{0};
  MappingType key_value_pair_{};
};

}  // namespace bustub
