//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.h
//
// Identification: src/include/storage/index/index_iterator.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include <cstddef>
#include <iterator>
#include <memory>
#include <optional>
#include <utility>
#include "buffer/traced_buffer_pool_manager.h"
#include "common/config.h"
#include "common/macros.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator, NumTombs>
#define SHORT_INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

FULL_INDEX_TEMPLATE_ARGUMENTS_DEFN
class IndexIterator {
 public:
  using value_type = std::pair<KeyType, ValueType>;
  using difference_type = std::ptrdiff_t;
  using reference = std::pair<const KeyType &, const ValueType &>;
  using pointer = void;
  using iterator_category = std::input_iterator_tag;

  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(std::shared_ptr<TracedBufferPoolManager> bpm, ReadPageGuard guard, int index,
                const KeyComparator &comparator);
  ~IndexIterator();  // NOLINT

  IndexIterator(IndexIterator &&) = default;
  auto operator=(IndexIterator &&) -> IndexIterator & = default;
  IndexIterator(const IndexIterator &) = default;
  auto operator=(const IndexIterator &) -> IndexIterator & = default;

  auto IsEnd() const -> bool;

  auto operator*() const -> reference;

  auto operator++() -> IndexIterator &;

  auto operator++(int) -> IndexIterator {
    auto previous = *this;
    ++*this;
    return previous;
  }

  auto operator==(const IndexIterator &itr) const -> bool;

  auto operator!=(const IndexIterator &itr) const -> bool;

 private:
  void SeekToLiveEntry();

  std::shared_ptr<TracedBufferPoolManager> bpm_;
  std::shared_ptr<ReadPageGuard> guard_;
  std::optional<KeyComparator> comparator_;
  int index_{0};
  KeyType key_{};
  ValueType value_{};
};

}  // namespace bustub
