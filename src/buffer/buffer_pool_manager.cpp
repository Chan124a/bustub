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
  frame_id_t new_frame_id = -1;
  if (free_list_.size() != 0) {
    new_frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&new_frame_id)) {
      return nullptr;
    }
  }

  Page *page = &pages_[new_frame_id];
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
  }
  page->ResetMemory();

  latch_.lock();
  page_id_t newPageId = AllocatePage();
  latch_.unlock();
  page->page_id_ = newPageId;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  page_table_[newPageId] = new_frame_id;
  replacer_->RecordAccess(new_frame_id);
  replacer_->SetEvictable(new_frame_id, false);

  *page_id = newPageId;
  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  if (page->GetPageId() != page_id) {
    frame_id_t new_frame_id = -1;
    if (free_list_.size() != 0) {
      new_frame_id = free_list_.front();
      free_list_.pop_front();
    } else {
      if (!replacer_->Evict(&new_frame_id)) {
        return nullptr;
      }
    }

    page = &pages_[new_frame_id];
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
    }
    page->ResetMemory();

    disk_manager_->ReadPage(page_id, page->data_);
    page->page_id_ = page_id;
    page->pin_count_ = 1;
    page->is_dirty_ = false;
    page_table_[page_id] = new_frame_id;
    replacer_->RecordAccess(new_frame_id);
    replacer_->SetEvictable(new_frame_id, false);
  } else {
    page->pin_count_ += 1;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
  }
  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  if (page->GetPageId() != page_id || page->pin_count_ == 0) {
    return false;
  }
  page->is_dirty_ = is_dirty;
  if (--page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  if (page->GetPageId() != page_id) {
    return false;
  }
  disk_manager_->WritePage(page->page_id_, page->data_);
  page->is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (size_t i = 0; i < pool_size_; ++i) {
    Page *page = &pages_[i];
    disk_manager_->WritePage(page->page_id_, page->data_);
    page->is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  if (page->GetPageId() != page_id) {
    return true;
  }
  if (page->pin_count_ > 0) {
    return false;
  }
  page_table_.erase(page_id);
  page->ResetMemory();
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  latch_.lock();
  DeallocatePage(page_id);
  latch_.unlock();
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
