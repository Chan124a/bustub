// :bustub-keep-private:
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer.cpp
//
// Identification: src/buffer/arc_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/arc_replacer.h"
#include <algorithm>
#include <cstddef>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include "common/config.h"

namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new ArcReplacer, with lists initialized to be empty and target size to 0
 * @param num_frames the maximum number of frames the ArcReplacer will be required to cache
 */
ArcReplacer::ArcReplacer(size_t num_frames) : replacer_size_(num_frames) {}

/**
 * TODO(P1): Add implementation
 *
 * @brief Performs the Replace operation as described by the writeup
 * that evicts from either mfu_ or mru_ into its corresponding ghost list
 * according to balancing policy.
 *
 * If you wish to refer to the original ARC paper, please note that there are
 * two changes in our implementation:
 * 1. When the size of mru_ equals the target size, we don't check
 * the last access as the paper did when deciding which list to evict from.
 * This is fine since the original decision is stated to be arbitrary.
 * 2. Entries that are not evictable are skipped. If all entries from the desired side
 * (mru_ / mfu_) are pinned, we instead try victimize the other side (mfu_ / mru_),
 * and move it to its corresponding ghost list (mfu_ghost_ / mru_ghost_).
 *
 * @return frame id of the evicted frame, or std::nullopt if cannot evict
 */
auto ArcReplacer::Evict() -> std::optional<frame_id_t> {
  std::optional<frame_id_t> frame_id = std::nullopt;
  std::unique_lock<std::mutex> latch_guard(latch_);
  if (mru_.size() >= mru_target_size_) {
    frame_id = TryEvictList(mru_, mru_ghost_, ArcStatus::MRU_GHOST);
    if (frame_id != std::nullopt) {
      return frame_id;
    } else {
      return TryEvictList(mfu_, mfu_ghost_, ArcStatus::MFU_GHOST);
    }
  } else {
    frame_id = TryEvictList(mfu_, mfu_ghost_, ArcStatus::MFU_GHOST);
    if (frame_id != std::nullopt) {
      return frame_id;
    } else {
      return TryEvictList(mru_, mru_ghost_, ArcStatus::MRU_GHOST);
    }
  }
}

auto ArcReplacer::TryEvictList(std::list<frame_id_t> &list, std::list<page_id_t> &ghost_list, ArcStatus ghost_state)
    -> std::optional<frame_id_t> {
  std::optional<frame_id_t> frame_id = std::nullopt;
  auto it = list.end();
  while (it != list.begin()) {
    --it;
    auto frame_status = alive_map_[*it];
    if (!frame_status->evictable_) {
      continue;
    }
    ghost_list.push_front(frame_status->page_id_);
    ghost_map_[frame_status->page_id_] = std::make_shared<FrameStatus>(frame_status->page_id_, frame_status->frame_id_,
                                                                       frame_status->evictable_, ghost_state);
    ghost_map_[frame_status->page_id_]->ghost_iterator = ghost_list.begin();
    frame_id = frame_status->frame_id_;
    alive_map_.erase(*it);
    list.erase(it);
    curr_size_--;
    break;
  }
  return frame_id;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record access to a frame, adjusting ARC bookkeeping accordingly
 * by bring the accessed page to the front of mfu_ if it exists in any of the lists
 * or the front of mru_ if it does not.
 *
 * Performs the operations EXCEPT REPLACE described in original paper, which is
 * handled by `Evict()`.
 *
 * Consider the following four cases, handle accordingly:
 * 1. Access hits mru_ or mfu_
 * 2/3. Access hits mru_ghost_ / mfu_ghost_
 * 4. Access misses all the lists
 *
 * This routine performs all changes to the four lists as preperation
 * for `Evict()` to simply find and evict a victim into ghost lists.
 *
 * Note that frame_id is used as identifier for alive pages and
 * page_id is used as identifier for the ghost pages, since page_id is
 * the unique identifier to the page after it's dead.
 * Using page_id for alive pages should be the same since it's one to one mapping,
 * but using frame_id is slightly more intuitive.
 *
 * @param frame_id id of frame that received a new access.
 * @param page_id id of page that is mapped to the frame.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void ArcReplacer::RecordAccess(frame_id_t frame_id, page_id_t page_id, [[maybe_unused]] AccessType access_type) {
  BUSTUB_ENSURE(frame_id >= 0, "frame_id must not be negative");
  BUSTUB_ENSURE(static_cast<size_t>(frame_id) < replacer_size_, "frame_id must be smaller than replacer_size");
  std::unique_lock<std::mutex> latch_guard(latch_);
  auto framt_state_iter = alive_map_.find(frame_id);
  if (framt_state_iter != alive_map_.end()) {
    if (framt_state_iter->second->arc_status_ == ArcStatus::MRU) {
      mfu_.splice(mfu_.begin(), mru_, framt_state_iter->second->list_iterator);
    } else {
      mfu_.splice(mfu_.begin(), mfu_, framt_state_iter->second->list_iterator);
    }
    alive_map_[frame_id]->arc_status_ = ArcStatus::MFU;
    alive_map_[frame_id]->list_iterator = mfu_.begin();
    return;
  }

  framt_state_iter = ghost_map_.find(page_id);
  if (framt_state_iter != ghost_map_.end()) {
    if (framt_state_iter->second->arc_status_ == ArcStatus::MRU_GHOST) {
      if (mru_ghost_.size() >= mfu_ghost_.size()) {
        mru_target_size_ += 1;
      } else {
        mru_target_size_ += (mfu_ghost_.size() / mru_ghost_.size());
      }
      if (mru_target_size_ > replacer_size_) {
        mru_target_size_ = replacer_size_;
      }
      mfu_.push_front(frame_id);
      mru_ghost_.erase(framt_state_iter->second->ghost_iterator);
      ghost_map_.erase(page_id);
      alive_map_[frame_id] = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MFU);
      alive_map_[frame_id]->list_iterator = mfu_.begin();
    } else if (framt_state_iter->second->arc_status_ == ArcStatus::MFU_GHOST) {
      size_t decrease = 1;
      if (mfu_ghost_.size() < mru_ghost_.size()) {
        decrease = mru_ghost_.size() / mfu_ghost_.size();
      }
      mru_target_size_ = mru_target_size_ >= decrease ? mru_target_size_ - decrease : 0;
      mfu_.push_front(frame_id);
      mfu_ghost_.erase(framt_state_iter->second->ghost_iterator);
      ghost_map_.erase(page_id);
      alive_map_[frame_id] = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MFU);
      alive_map_[frame_id]->list_iterator = mfu_.begin();
    }
    return;
  }

  // 由于入参frame_id的范围为[0,replacer_size_-1],所以mru中的frame个数不可能>replacer_size_
  // 如果mru_.siz+ mru_ghost_.size() == replacer_size_，那么mru_ghost的size一定不为0.
  if (mru_.size() + mru_ghost_.size() == replacer_size_) {
    ghost_map_.erase(mru_ghost_.back());
    mru_ghost_.pop_back();
  } else if (mru_.size() + mru_ghost_.size() + mfu_.size() + mfu_ghost_.size() == 2 * replacer_size_) {
    ghost_map_.erase(mfu_ghost_.back());
    mfu_ghost_.pop_back();
  }
  mru_.push_front(frame_id);
  alive_map_[frame_id] = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MRU);
  alive_map_[frame_id]->list_iterator = mru_.begin();
  return;
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
void ArcReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ENSURE(frame_id >= 0, "frame_id must not be negative");
  BUSTUB_ENSURE(static_cast<size_t>(frame_id) < replacer_size_, "frame_id must be smaller than replacer_size");
  std::unique_lock<std::mutex> guard(latch_);
  auto it = alive_map_.find(frame_id);
  if (it == alive_map_.end()) {
    return;
  }

  if (!it->second->evictable_ && set_evictable) {
    curr_size_++;
  } else if (it->second->evictable_ && !set_evictable) {
    curr_size_--;
  }
  it->second->evictable_ = set_evictable;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * decided by the ARC algorithm.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void ArcReplacer::Remove(frame_id_t frame_id) {
  std::unique_lock<std::mutex> guard(latch_);
  auto it = alive_map_.find(frame_id);
  if (it == alive_map_.end()) {
    return;
  }

  BUSTUB_ENSURE(it->second->evictable_, "the frame is non-evictable.");
  if (it->second->arc_status_ == ArcStatus::MRU) {
    mru_.erase(it->second->list_iterator);
  }
  if (it->second->arc_status_ == ArcStatus::MFU) {
    mfu_.erase(it->second->list_iterator);
  }
  alive_map_.erase(frame_id);
  curr_size_--;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto ArcReplacer::Size() -> size_t {
  std::unique_lock<std::mutex> guard(latch_);
  return curr_size_;
}

}  // namespace bustub
