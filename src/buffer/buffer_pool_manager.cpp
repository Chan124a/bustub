//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <cstddef>
#include <cstring>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  page_id_t victim_page_id = INVALID_PAGE_ID;
  std::vector<char> victim_data;
  frame_id_t new_frame_id = -1;
  Page *page = nullptr;

  {
    std::lock_guard<std::mutex> guard(latch_);
    if (free_list_.size() != 0) {
      new_frame_id = free_list_.front();
      free_list_.pop_front();
    } else {
      if (!replacer_->Evict(&new_frame_id)) {
        return nullptr;
      }
    }

    page = &pages_[new_frame_id];
    if (page->IsDirty() && page->GetPageId() != INVALID_PAGE_ID) {
      victim_page_id = page->GetPageId();
      victim_data.resize(BUSTUB_PAGE_SIZE);
      std::memcpy(victim_data.data(), page->GetData(), BUSTUB_PAGE_SIZE);
      loading_pages_.insert(victim_page_id);
    }
    if (page->GetPageId() != INVALID_PAGE_ID) {
      page_table_.erase(page->GetPageId());
    }
    page->ResetMemory();

    page_id_t newPageId = AllocatePage();
    page->page_id_ = newPageId;
    page->pin_count_ = 1;
    page->is_dirty_ = false;
    page_table_[newPageId] = new_frame_id;
    replacer_->RecordAccess(new_frame_id);
    replacer_->SetEvictable(new_frame_id, false);

    *page_id = newPageId;
  }

  if (victim_page_id != INVALID_PAGE_ID) {
    disk_manager_->WritePage(victim_page_id, victim_data.data());
    {
      std::lock_guard<std::mutex> guard(latch_);
      loading_pages_.erase(victim_page_id);
    }
    load_cv_.notify_all();
  }

  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  page_id_t victim_page_id = INVALID_PAGE_ID;
  std::vector<char> victim_data;
  std::vector<char> page_data(BUSTUB_PAGE_SIZE);
  frame_id_t new_frame_id = -1;
  Page *page = nullptr;

  {
    std::unique_lock<std::mutex> lock(latch_);
    while (true) {
      auto it = page_table_.find(page_id);
      if (it != page_table_.end()) {
        frame_id_t frame_id = it->second;
        page = &pages_[frame_id];
        page->pin_count_ += 1;
        replacer_->RecordAccess(frame_id);
        replacer_->SetEvictable(frame_id, false);
        return page;
      }

      if (loading_pages_.find(page_id) == loading_pages_.end()) {
        loading_pages_.insert(page_id);
        break;
      }

      load_cv_.wait(lock);
    }

    if (free_list_.size() != 0) {
      new_frame_id = free_list_.front();
      free_list_.pop_front();
    } else {
      if (!replacer_->Evict(&new_frame_id)) {
        loading_pages_.erase(page_id);
        load_cv_.notify_all();
        return nullptr;
      }
    }

    page = &pages_[new_frame_id];
    if (page->IsDirty() && page->GetPageId() != INVALID_PAGE_ID) {
      victim_page_id = page->GetPageId();
      victim_data.resize(BUSTUB_PAGE_SIZE);
      std::memcpy(victim_data.data(), page->GetData(), BUSTUB_PAGE_SIZE);
      loading_pages_.insert(victim_page_id);
    }
    if (page->GetPageId() != INVALID_PAGE_ID) {
      page_table_.erase(page->GetPageId());
    }
    page->ResetMemory();
    page->page_id_ = INVALID_PAGE_ID;
    page->pin_count_ = 1;
    page->is_dirty_ = false;
  }

  if (victim_page_id != INVALID_PAGE_ID) {
    disk_manager_->WritePage(victim_page_id, victim_data.data());
    {
      std::lock_guard<std::mutex> guard(latch_);
      loading_pages_.erase(victim_page_id);
    }
    load_cv_.notify_all();
  }
  disk_manager_->ReadPage(page_id, page_data.data());

  {
    std::lock_guard<std::mutex> guard(latch_);
    std::memcpy(page->data_, page_data.data(), BUSTUB_PAGE_SIZE);
    page->page_id_ = page_id;
    page->pin_count_ = 1;
    page->is_dirty_ = false;
    page_table_[page_id] = new_frame_id;
    replacer_->RecordAccess(new_frame_id);
    replacer_->SetEvictable(new_frame_id, false);
    loading_pages_.erase(page_id);
  }
  load_cv_.notify_all();
  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = it->second;
  Page *page = &pages_[frame_id];
  if (page->GetPageId() != page_id || page->pin_count_ == 0) {
    return false;
  }

  page->is_dirty_ |= is_dirty;
  if (--page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::vector<char> page_data(BUSTUB_PAGE_SIZE);
  frame_id_t frame_id = -1;

  {
    std::lock_guard<std::mutex> guard(latch_);
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
      return false;
    }
    frame_id = it->second;
    Page *page = &pages_[frame_id];
    if (page->GetPageId() != page_id) {
      return false;
    }
    if (page->pin_count_ == 0) {
      replacer_->SetEvictable(frame_id, false);
    }
    page->pin_count_++;
    std::memcpy(page_data.data(), page->data_, BUSTUB_PAGE_SIZE);
    page->is_dirty_ = false;
  }

  disk_manager_->WritePage(page_id, page_data.data());

  {
    std::lock_guard<std::mutex> guard(latch_);
    Page *page = &pages_[frame_id];
    if (page->GetPageId() == page_id && --page->pin_count_ == 0) {
      replacer_->SetEvictable(frame_id, true);
    }
  }
  return true;
}

void BufferPoolManager::FlushAllPages() {
  struct FlushSnapshot {
    page_id_t page_id_;
    frame_id_t frame_id_;
    std::vector<char> data_;
  };

  std::vector<FlushSnapshot> pages;
  {
    std::lock_guard<std::mutex> guard(latch_);
    pages.reserve(page_table_.size());
    for (const auto &[page_id, frame_id] : page_table_) {
      Page *page = &pages_[frame_id];
      if (page->GetPageId() != page_id) {
        continue;
      }
      if (page->pin_count_ == 0) {
        replacer_->SetEvictable(frame_id, false);
      }
      page->pin_count_++;
      page->is_dirty_ = false;
      FlushSnapshot snapshot{page_id, frame_id, std::vector<char>(BUSTUB_PAGE_SIZE)};
      std::memcpy(snapshot.data_.data(), page->data_, BUSTUB_PAGE_SIZE);
      pages.emplace_back(std::move(snapshot));
    }
  }

  for (const auto &page : pages) {
    disk_manager_->WritePage(page.page_id_, page.data_.data());
  }

  {
    std::lock_guard<std::mutex> guard(latch_);
    for (const auto &snapshot : pages) {
      Page *page = &pages_[snapshot.frame_id_];
      if (page->GetPageId() == snapshot.page_id_ && --page->pin_count_ == 0) {
        replacer_->SetEvictable(snapshot.frame_id_, true);
      }
    }
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = it->second;
  Page *page = &pages_[frame_id];
  if (page->GetPageId() != page_id) {
    return true;
  }
  if (page->pin_count_ > 0) {
    return false;
  }
  page_table_.erase(page_id);
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  Page *page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *page = NewPage(page_id);
  return {this, page};
}

}  // namespace bustub
