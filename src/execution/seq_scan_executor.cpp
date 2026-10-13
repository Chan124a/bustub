//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {

/**
 * Construct a new SeqScanExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The sequential scan plan to be executed
 */
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

/** Initialize the sequential scan */
void SeqScanExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_).get();
  iterator_.emplace(table_info_->table_->MakeIterator());
}

/**
 * Yield the next tuple batch from the seq scan.
 * @param[out] tuple_batch The next tuple batch produced by the scan
 * @param[out] rid_batch The next tuple RID batch produced by the scan
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto SeqScanExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                           size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();
  while (tuple_batch->size() < batch_size && !iterator_->IsEnd()) {
    auto rid = iterator_->GetRID();
    auto *txn_mgr = exec_ctx_->GetTransactionManager();
    auto [meta, base_tuple, link] = GetTupleAndUndoLink(txn_mgr, table_info_->table_.get(), rid);
    ++*iterator_;
    auto logs = CollectUndoLogs(rid, meta, base_tuple, link, exec_ctx_->GetTransaction(), txn_mgr);
    if (!logs.has_value()) {
      continue;
    }
    auto visible = ReconstructTuple(&table_info_->schema_, base_tuple, meta, *logs);
    if (!visible.has_value()) {
      continue;
    }
    auto tuple = std::move(*visible);
    if (plan_->filter_predicate_ != nullptr) {
      auto value = plan_->filter_predicate_->Evaluate(&tuple, GetOutputSchema());
      if (value.IsNull() || !value.GetAs<bool>()) {
        continue;
      }
    }
    tuple_batch->push_back(std::move(tuple));
    rid_batch->push_back(rid);
  }
  return !tuple_batch->empty();
}

}  // namespace bustub
