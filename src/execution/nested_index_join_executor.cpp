//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Creates a new nested index join executor.
 * @param exec_ctx the context that the nested index join should be performed in
 * @param plan the nested index join plan to be executed
 * @param child_executor the outer table
 */
NestedIndexJoinExecutor::NestedIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                                 std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedIndexJoinExecutor::Init() {
  child_executor_->Init();
  inner_table_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid()).get();
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid()).get();
  left_batch_.clear();
  left_offset_ = 0;
  current_left_.reset();
  matches_.clear();
  match_offset_ = 0;
  matched_ = false;
}

auto NestedIndexJoinExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                   size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();
  const auto &left_schema = child_executor_->GetOutputSchema();
  const auto &right_schema = plan_->InnerTableSchema();
  while (tuple_batch->size() < batch_size) {
    if (!current_left_.has_value()) {
      if (left_offset_ >= left_batch_.size()) {
        if (!child_executor_->Next(&left_batch_, &left_rids_, BUSTUB_BATCH_SIZE)) {
          break;
        }
        left_offset_ = 0;
      }
      current_left_ = left_batch_[left_offset_++];
      matches_.clear();
      match_offset_ = 0;
      matched_ = false;
      auto key = plan_->KeyPredicate()->Evaluate(&*current_left_, left_schema);
      if (!key.IsNull()) {
        index_info_->index_->ScanKey(Tuple({key}, &index_info_->key_schema_), &matches_, exec_ctx_->GetTransaction());
      }
    }
    while (match_offset_ < matches_.size() && tuple_batch->size() < batch_size) {
      auto [meta, right] = inner_table_->table_->GetTuple(matches_[match_offset_++]);
      if (meta.is_deleted_) {
        continue;
      }
      matched_ = true;
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
        values.push_back(current_left_->GetValue(&left_schema, i));
      }
      for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
        values.push_back(right.GetValue(&right_schema, i));
      }
      tuple_batch->emplace_back(values, &GetOutputSchema());
      rid_batch->emplace_back();
    }
    if (match_offset_ == matches_.size()) {
      if (!matched_ && plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> values;
        for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
          values.push_back(current_left_->GetValue(&left_schema, i));
        }
        for (const auto &column : right_schema.GetColumns()) {
          values.push_back(ValueFactory::GetNullValueByType(column.GetType()));
        }
        tuple_batch->emplace_back(values, &GetOutputSchema());
        rid_batch->emplace_back();
      }
      current_left_.reset();
    }
  }
  return !tuple_batch->empty();
}

}  // namespace bustub
