//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// count_min_sketch.cpp
//
// Identification: src/primer/count_min_sketch.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/count_min_sketch.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace bustub {

/**
 * Constructor for the count-min sketch.
 *
 * @param width The width of the sketch matrix.
 * @param depth The depth of the sketch matrix.
 * @throws std::invalid_argument if width or depth are zero.
 */
template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(uint32_t width, uint32_t depth) : width_(width), depth_(depth) {
  /** @TODO(student) Implement this function! */
  if (width == 0 || depth == 0) {
    throw std::invalid_argument("invalid argument");
  }

  /** @spring2026 PLEASE DO NOT MODIFY THE FOLLOWING */
  // Initialize seeded hash functions
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }

  const size_t count_size = static_cast<size_t>(width_) * static_cast<size_t>(depth_);
  counts_ = std::make_unique<std::atomic<uint32_t>[]>(count_size);
  for (size_t i = 0; i < count_size; i++) {
    counts_[i].store(0, std::memory_order_relaxed);
  }
}

template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(CountMinSketch &&other) noexcept : width_(other.width_), depth_(other.depth_) {
  /** @TODO(student) Implement this function! */
  hash_functions_.clear();
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }

  counts_ = std::move(other.counts_);

  other.width_ = 0;
  other.depth_ = 0;
  other.hash_functions_.clear();
}

template <typename KeyType>
auto CountMinSketch<KeyType>::operator=(CountMinSketch &&other) noexcept -> CountMinSketch & {
  /** @TODO(student) Implement this function! */
  if (this == &other) {
    return *this;
  }

  width_ = other.width_;
  depth_ = other.depth_;
  hash_functions_.clear();
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }

  counts_ = std::move(other.counts_);

  other.width_ = 0;
  other.depth_ = 0;
  other.hash_functions_.clear();

  return *this;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Insert(const KeyType &item) {
  /** @TODO(student) Implement this function! */
  for (size_t i = 0; i < depth_; i++) {
    size_t hash_value = hash_functions_[i](item);
    counts_[i * width_ + hash_value].fetch_add(1, std::memory_order_relaxed);
  }
}

template <typename KeyType>
void CountMinSketch<KeyType>::Merge(const CountMinSketch<KeyType> &other) {
  if (width_ != other.width_ || depth_ != other.depth_) {
    throw std::invalid_argument("Incompatible CountMinSketch dimensions for merge.");
  }
  /** @TODO(student) Implement this function! */
  for (size_t i = 0; i < depth_; i++) {
    for (size_t j = 0; j < width_; j++) {
      counts_[i * width_ + j].fetch_add(other.counts_[i * width_ + j].load(std::memory_order_relaxed),
                                        std::memory_order_relaxed);
    }
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::Count(const KeyType &item) const -> uint32_t {
  uint32_t result = std::numeric_limits<uint32_t>::max();
  for (size_t i = 0; i < depth_; i++) {
    size_t hash_value = hash_functions_[i](item);
    result = std::min(counts_[i * width_ + hash_value].load(std::memory_order_relaxed), result);
  }
  return result;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Clear() {
  /** @TODO(student) Implement this function! */
  const size_t count_size = static_cast<size_t>(width_) * static_cast<size_t>(depth_);
  for (size_t i = 0; i < count_size; i++) {
    counts_[i].store(0, std::memory_order_relaxed);
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::TopK(uint16_t k, const std::vector<KeyType> &candidates)
    -> std::vector<std::pair<KeyType, uint32_t>> {
  /** @TODO(student) Implement this function! */
  std::vector<std::pair<KeyType, uint32_t>> result;
  for (size_t i = 0; i < candidates.size(); i++) {
    result.push_back({candidates[i], Count(candidates[i])});
  }
  std::sort(result.begin(), result.end(),
            [](std::pair<KeyType, uint32_t> &a, std::pair<KeyType, uint32_t> &b) { return a.second > b.second; });
  if (result.size() > k) {
    result.erase(result.begin() + k, result.end());
  }
  return result;
}

// Explicit instantiations for all types used in tests
template class CountMinSketch<std::string>;
template class CountMinSketch<int64_t>;  // For int64_t tests
template class CountMinSketch<int>;      // This covers both int and int32_t
}  // namespace bustub
