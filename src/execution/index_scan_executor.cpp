//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/index_scan_executor.h"
#include <set>
#include <utility>
#include <vector>

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "storage/index/b_plus_tree_index.h"
#include "type/type.h"

namespace bustub {
namespace {
template <size_t KeySize>
void CollectIndexRids(IndexInfo *index_info, const IndexScanPlanNode *plan, Transaction *txn,
                      std::vector<RID> *scan_rids) {
  using Tree = BPlusTreeIndex<GenericKey<KeySize>, RID, GenericComparator<KeySize>>;
  auto *tree = dynamic_cast<Tree *>(index_info->index_.get());
  BUSTUB_ASSERT(tree != nullptr, "Index key size does not match the B+ tree template");
  auto *schema = &index_info->key_schema_;
  if (plan->lookup_keys_.empty()) {
    for (auto it = tree->GetBeginIterator(); !it.IsEnd(); ++it) {
      scan_rids->push_back((*it).second);
    }
    return;
  }
  std::set<std::pair<page_id_t, uint32_t>> seen;
  for (const auto &lookup : plan->lookup_keys_) {
    std::vector<Value> values;
    values.reserve(schema->GetColumnCount());
    for (const auto &expr : lookup) {
      values.push_back(expr->Evaluate(nullptr, *schema));
    }
    if (lookup.size() == schema->GetColumnCount()) {
      std::vector<RID> matches;
      index_info->index_->ScanKey(Tuple(values, schema), &matches, txn);
      for (const auto &rid : matches) {
        if (seen.emplace(rid.GetPageId(), rid.GetSlotNum()).second) {
          scan_rids->push_back(rid);
        }
      }
      continue;
    }
    for (size_t i = values.size(); i < schema->GetColumnCount(); i++) {
      values.push_back(Type::GetMinValue(schema->GetColumn(i).GetType()));
    }
    GenericKey<KeySize> lower_bound;
    lower_bound.SetFromKey(Tuple(values, schema));
    for (auto it = tree->GetBeginIterator(lower_bound); !it.IsEnd(); ++it) {
      bool same_prefix = true;
      for (size_t i = 0; i < lookup.size(); i++) {
        if ((*it).first.ToValue(schema, i).CompareEquals(values[i]) != CmpBool::CmpTrue) {
          same_prefix = false;
          break;
        }
      }
      if (!same_prefix) {
        break;
      }
      const auto &rid = (*it).second;
      if (seen.emplace(rid.GetPageId(), rid.GetSlotNum()).second) {
        scan_rids->push_back(rid);
      }
    }
  }
}
}  // namespace

/**
 * Creates a new index scan executor.
 * @param exec_ctx the executor context
 * @param plan the index scan plan to be executed
 */
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  auto *txn = exec_ctx_->GetTransaction();
  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    txn->AppendScanPredicate(plan_->table_oid_, plan_->filter_predicate_);
  }
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_).get();
  scan_rids_.clear();
  next_rid_ = 0;
  size_t size = 0;
  for (const auto &column : index_info->key_schema_.GetColumns()) {
    size += column.GetStorageSize();
  }
  if (size <= 4) {
    CollectIndexRids<4>(index_info.get(), plan_, exec_ctx_->GetTransaction(), &scan_rids_);
  } else if (size <= 8) {
    CollectIndexRids<8>(index_info.get(), plan_, exec_ctx_->GetTransaction(), &scan_rids_);
  } else if (size <= 16) {
    CollectIndexRids<16>(index_info.get(), plan_, exec_ctx_->GetTransaction(), &scan_rids_);
  } else if (size <= 32) {
    CollectIndexRids<32>(index_info.get(), plan_, exec_ctx_->GetTransaction(), &scan_rids_);
  } else {
    CollectIndexRids<64>(index_info.get(), plan_, exec_ctx_->GetTransaction(), &scan_rids_);
  }
}

auto IndexScanExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                             size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();
  while (tuple_batch->size() < batch_size && next_rid_ < scan_rids_.size()) {
    auto rid = scan_rids_[next_rid_++];
    auto *txn_mgr = exec_ctx_->GetTransactionManager();
    auto [meta, base_tuple, link] = GetTupleAndUndoLink(txn_mgr, table_info_->table_.get(), rid);
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
