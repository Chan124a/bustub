//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cstddef>
#include <mutex>
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

void LRUKNode::UpdateAccessTime(size_t timestamp) {
  history_.push_back(timestamp);
  while (history_.size() > k_) {
    history_.pop_front();
  }
}

void LRUKNode::GetKTimestamp(size_t &kTimestamp, size_t &count) {
  kTimestamp = history_.front();
  count = history_.size();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  size_t distance_equal_k = 0;
  size_t frame_id_equal_k = 0;
  bool find_frame_id_equal_k = false;
  size_t distance_less_k = 0;
  size_t frame_id_less_k = 0;
  bool find_frame_id_less_k = false;
  for (auto it = node_store_.begin(); it != node_store_.end(); ++it) {
    if (!it->second.IsEvictable()) {
      continue;
    }
    size_t kTimestamp = 0;
    size_t count = 0;
    it->second.GetKTimestamp(kTimestamp, count);
    size_t distance = current_timestamp_ - kTimestamp;
    if (count == k_) {
      if (!find_frame_id_equal_k || distance > distance_equal_k) {
        distance_equal_k = distance;
        frame_id_equal_k = it->first;
        find_frame_id_equal_k = true;
      }
    } else {
      if (!find_frame_id_less_k || distance > distance_less_k) {
        distance_less_k = distance;
        frame_id_less_k = it->first;
        find_frame_id_less_k = true;
      }
    }
  }
  if (!find_frame_id_equal_k && !find_frame_id_less_k) {
    return false;
  }
  if (find_frame_id_less_k) {
    *frame_id = frame_id_less_k;
  } else {
    *frame_id = frame_id_equal_k;
  }
  node_store_.erase(*frame_id);
  curr_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  BUSTUB_ASSERT((size_t)frame_id <= replacer_size_, "the frame_id is larger than replacer_size");
  std::lock_guard<std::mutex> guard(latch_);
  current_timestamp_++;
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    LRUKNode newNode(k_, frame_id);
    newNode.UpdateAccessTime(current_timestamp_);
    node_store_.emplace(frame_id, newNode);
  } else {
    it->second.UpdateAccessTime(current_timestamp_);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> guard(latch_);
  auto it = node_store_.find(frame_id);
  BUSTUB_ASSERT(it != node_store_.end(), "the frame_id is invalid.");
  if (!it->second.IsEvictable() && set_evictable) {
    curr_size_++;
  } else if (it->second.IsEvictable() && !set_evictable) {
    curr_size_--;
  }
  it->second.SetEvictable(set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }
  BUSTUB_ASSERT(it->second.IsEvictable(), "the frame is non-evictable.");
  node_store_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
