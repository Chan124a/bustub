//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

void LRUKNode::UpdateAccessTime(size_t timestamp) {
  history_.push_back(timestamp);
  while (history_.size() > k_) {
    history_.pop_front();
  }
}

void LRUKNode::GetKTimestamp(size_t &k_timestamp, size_t &count) const {
  k_timestamp = history_.front();
  count = history_.size();
}

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  BUSTUB_ENSURE(k > 0, "k must be greater than zero");
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return the frame ID if a frame is successfully evicted, or `std::nullopt` if no frames can be evicted.
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  std::optional<frame_id_t> frame_id = std::nullopt;
  std::lock_guard<std::mutex> guard(latch_);
  size_t distance_equal_k = 0;
  frame_id_t frame_id_equal_k = 0;
  bool find_frame_id_equal_k = false;
  size_t distance_less_k = 0;
  frame_id_t frame_id_less_k = 0;
  bool find_frame_id_less_k = false;
  for (auto &[candidate_frame_id, node] : node_store_) {
    if (!node.IsEvictable()) {
      continue;
    }
    size_t k_timestamp = 0;
    size_t count = 0;
    node.GetKTimestamp(k_timestamp, count);
    size_t distance = current_timestamp_ - k_timestamp;
    if (count == k_) {
      if (!find_frame_id_equal_k || distance > distance_equal_k) {
        distance_equal_k = distance;
        frame_id_equal_k = candidate_frame_id;
        find_frame_id_equal_k = true;
      }
    } else {
      if (!find_frame_id_less_k || distance > distance_less_k) {
        distance_less_k = distance;
        frame_id_less_k = candidate_frame_id;
        find_frame_id_less_k = true;
      }
    }
  }
  if (!find_frame_id_equal_k && !find_frame_id_less_k) {
    return frame_id;
  }
  if (find_frame_id_less_k) {
    frame_id = frame_id_less_k;
  } else {
    frame_id = frame_id_equal_k;
  }
  node_store_.erase(*frame_id);
  curr_size_--;
  return frame_id;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  BUSTUB_ENSURE(frame_id >= 0, "frame_id must not be negative");
  BUSTUB_ENSURE(static_cast<size_t>(frame_id) < replacer_size_, "frame_id must be smaller than replacer_size");
  std::lock_guard<std::mutex> guard(latch_);
  current_timestamp_++;
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    LRUKNode new_node(k_, frame_id);
    new_node.UpdateAccessTime(current_timestamp_);
    node_store_.emplace(frame_id, new_node);
  } else {
    it->second.UpdateAccessTime(current_timestamp_);
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ENSURE(frame_id >= 0, "frame_id must not be negative");
  BUSTUB_ENSURE(static_cast<size_t>(frame_id) < replacer_size_, "frame_id must be smaller than replacer_size");
  std::lock_guard<std::mutex> guard(latch_);
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }
  if (!it->second.IsEvictable() && set_evictable) {
    curr_size_++;
  } else if (it->second.IsEvictable() && !set_evictable) {
    curr_size_--;
  }
  it->second.SetEvictable(set_evictable);
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }
  BUSTUB_ENSURE(it->second.IsEvictable(), "the frame is non-evictable.");
  node_store_.erase(frame_id);
  curr_size_--;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> guard(latch_);
  return curr_size_;
}

}  // namespace bustub
