//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.cpp
//
// Identification: src/execution/external_merge_sort_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/external_merge_sort_executor.h"
#include <algorithm>
#include <vector>
#include "execution/plans/sort_plan.h"

namespace bustub {

template <size_t K>
ExternalMergeSortExecutor<K>::ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                                                        std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      cmp_(plan->GetOrderBy()),
      child_executor_(std::move(child_executor)),
      final_run_({}, exec_ctx->GetBufferPoolManager()) {}

template <size_t K>
ExternalMergeSortExecutor<K>::~ExternalMergeSortExecutor() {
  final_iterator_.reset();
  final_run_.DeletePages();
}

/** Initialize the external merge sort */
template <size_t K>
void ExternalMergeSortExecutor<K>::Init() {
  final_iterator_.reset();
  final_run_.DeletePages();
  child_executor_->Init();
  auto *bpm = exec_ctx_->GetBufferPoolManager();
  const auto &schema = child_executor_->GetOutputSchema();
  const auto &order_bys = plan_->GetOrderBy();
  auto less = [&](const Tuple &a, const Tuple &b) {
    return cmp_({GenerateSortKey(a, order_bys, schema), a}, {GenerateSortKey(b, order_bys, schema), b});
  };
  std::vector<MergeSortRun> runs;
  std::vector<Tuple> page_tuples;
  size_t used = 0;
  auto flush = [&] {
    if (page_tuples.empty()) {
      return;
    }
    std::sort(page_tuples.begin(), page_tuples.end(), less);
    MergeSortRun run({}, bpm);
    for (const auto &tuple : page_tuples) {
      run.Append(tuple);
    }
    runs.push_back(std::move(run));
    page_tuples.clear();
    used = 0;
  };
  std::vector<Tuple> tuples;
  std::vector<RID> rids;
  const size_t capacity = BUSTUB_PAGE_SIZE - sizeof(IntermediateResultPage);
  while (child_executor_->Next(&tuples, &rids, BUSTUB_BATCH_SIZE)) {
    for (const auto &tuple : tuples) {
      auto bytes = sizeof(uint32_t) + tuple.GetLength();
      BUSTUB_ASSERT(bytes <= capacity, "Sort tuple exceeds an intermediate page");
      if (used + bytes > capacity) {
        flush();
      }
      page_tuples.push_back(tuple);
      used += bytes;
    }
  }
  flush();
  while (runs.size() > 1) {
    std::vector<MergeSortRun> merged;
    for (size_t i = 0; i < runs.size(); i += K) {
      if (i + 1 == runs.size()) {
        merged.push_back(std::move(runs[i]));
        continue;
      }
      MergeSortRun out({}, bpm);
      auto left = runs[i].Begin();
      auto right = runs[i + 1].Begin();
      const auto left_end = runs[i].End();
      const auto right_end = runs[i + 1].End();
      while (left != left_end && right != right_end) {
        auto left_tuple = *left;
        auto right_tuple = *right;
        if (less(right_tuple, left_tuple)) {
          out.Append(right_tuple);
          ++right;
        } else {
          out.Append(left_tuple);
          ++left;
        }
      }
      while (left != left_end) {
        out.Append(*left);
        ++left;
      }
      while (right != right_end) {
        out.Append(*right);
        ++right;
      }
      runs[i].DeletePages();
      runs[i + 1].DeletePages();
      merged.push_back(std::move(out));
    }
    runs = std::move(merged);
  }
  if (!runs.empty()) {
    final_run_ = std::move(runs.front());
  }
  final_iterator_ = final_run_.Begin();
}

/**
 * Yield the next tuple batch from the external merge sort.
 * @param[out] tuple_batch The next tuple batch produced by the external merge sort.
 * @param[out] rid_batch The next tuple RID batch produced by the external merge sort.
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
template <size_t K>
auto ExternalMergeSortExecutor<K>::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                        size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();
  while (tuple_batch->size() < batch_size && *final_iterator_ != final_run_.End()) {
    tuple_batch->push_back(**final_iterator_);
    rid_batch->emplace_back();
    ++*final_iterator_;
  }
  return !tuple_batch->empty();
}

template class ExternalMergeSortExecutor<2>;

}  // namespace bustub
