//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  // 正常查询执行中通常不会重复 Init() 同一个 executor，但从代码健壮性上看，Init
  // 语义本来就是“把 executor重置到开始状态”，所以 executor 实现应该支持重复 Init()
  rids_.clear();
  cursor_ = 0;

  auto index_info = exec_ctx_->GetCatalog()->GetIndex((plan_->GetIndexOid()));
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_);
  auto index = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get());
  for (auto iter = index->GetBeginIterator(); !iter.IsEnd(); ++iter) {
    rids_.push_back((*iter).second);
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (cursor_ < rids_.size()) {
    auto cur_rid = rids_[cursor_];
    auto cur_tuple = table_info_->table_->GetTuple(cur_rid);
    ++cursor_;
    if (!cur_tuple.first.is_deleted_) {
      *tuple = cur_tuple.second;
      *rid = cur_rid;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
