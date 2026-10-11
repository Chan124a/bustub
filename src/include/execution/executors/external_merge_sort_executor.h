//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.h
//
// Identification: src/include/execution/executors/external_merge_sort_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include "common/config.h"
#include "common/macros.h"
#include "execution/execution_common.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/sort_plan.h"
#include "storage/page/intermediate_result_page.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * A data structure that holds the sorted tuples as a run during external merge sort.
 * Tuples might be stored in multiple pages, and tuples are ordered both within one page
 * and across pages.
 */
class MergeSortRun {
 public:
  MergeSortRun() = default;
  MergeSortRun(std::vector<page_id_t> pages, BufferPoolManager *bpm) : pages_(std::move(pages)), bpm_(bpm) {}

  auto GetPageCount() const -> size_t { return pages_.size(); }

  void Append(const Tuple &tuple) {
    if (!pages_.empty()) {
      auto guard = bpm_->WritePage(pages_.back());
      if (guard.AsMut<IntermediateResultPage>()->Append(tuple)) {
        return;
      }
    }
    auto page_id = bpm_->NewPage();
    auto guard = bpm_->WritePage(page_id);
    auto *page = guard.AsMut<IntermediateResultPage>();
    page->Init();
    BUSTUB_ASSERT(page->Append(tuple), "Sort tuple exceeds an intermediate page");
    pages_.push_back(page_id);
  }

  void DeletePages() {
    for (auto page_id : pages_) {
      bpm_->DeletePage(page_id);
    }
    pages_.clear();
  }

  /** Iterator for iterating on the sorted tuples in one run. */
  class Iterator {
    friend class MergeSortRun;

   public:
    Iterator() = default;

    /**
     * Advance the iterator to the next tuple. If the current sort page is exhausted, move to the
     * next sort page.
     */
    auto operator++() -> Iterator & {
      if (page_idx_ >= run_->pages_.size()) {
        return *this;
      }
      auto guard = run_->bpm_->ReadPage(run_->pages_[page_idx_]);
      const auto *page = guard.As<IntermediateResultPage>();
      page->ReadTuple(&byte_offset_);
      if (byte_offset_ >= page->GetUsed()) {
        page_idx_++;
        byte_offset_ = 0;
      }
      return *this;
    }

    /**
     * Dereference the iterator to get the current tuple in the sorted run that the iterator is
     * pointing to.
     */
    auto operator*() -> Tuple {
      auto guard = run_->bpm_->ReadPage(run_->pages_[page_idx_]);
      uint32_t offset = byte_offset_;
      return guard.As<IntermediateResultPage>()->ReadTuple(&offset);
    }

    /**
     * Checks whether two iterators are pointing to the same tuple in the same sorted run.
     */
    auto operator==(const Iterator &other) const -> bool {
      return run_ == other.run_ && page_idx_ == other.page_idx_ && byte_offset_ == other.byte_offset_;
    }

    /**
     * Checks whether two iterators are pointing to different tuples in a sorted run or iterating
     * on different sorted runs.
     */
    auto operator!=(const Iterator &other) const -> bool { return !(*this == other); }

   private:
    explicit Iterator(const MergeSortRun *run) : run_(run) {}

    /** The sorted run that the iterator is iterating on. */
    const MergeSortRun *run_{nullptr};

    size_t page_idx_{0};
    uint32_t byte_offset_{0};
  };

  /**
   * Get an iterator pointing to the beginning of the sorted run, i.e. the first tuple.
   */
  auto Begin() const -> Iterator { return Iterator(this); }

  /**
   * Get an iterator pointing to the end of the sorted run, i.e. the position after the last tuple.
   */
  auto End() const -> Iterator {
    auto end = Iterator(this);
    end.page_idx_ = pages_.size();
    return end;
  }

 private:
  /** The page IDs of the sort pages that store the sorted tuples. */
  std::vector<page_id_t> pages_;
  /**
   * The buffer pool manager used to read sort pages. The buffer pool manager is responsible for
   * deleting the sort pages when they are no longer needed.
   */
  BufferPoolManager *bpm_{nullptr};
};

/**
 * ExternalMergeSortExecutor executes an external merge sort.
 *
 * In Spring 2025, only 2-way external merge sort is required.
 */
template <size_t K>
class ExternalMergeSortExecutor : public AbstractExecutor {
 public:
  ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                            std::unique_ptr<AbstractExecutor> &&child_executor);
  ~ExternalMergeSortExecutor() override;

  void Init() override;

  auto Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool override;

  /** @return The output schema for the external merge sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;

  /** Compares tuples based on the order-bys */
  TupleComparator cmp_;

  std::unique_ptr<AbstractExecutor> child_executor_;
  MergeSortRun final_run_;
  std::optional<MergeSortRun::Iterator> final_iterator_;
};

}  // namespace bustub
