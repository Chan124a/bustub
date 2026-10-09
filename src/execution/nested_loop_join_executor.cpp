//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/macros.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new NestedLoopJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The nested loop join plan to be executed
 * @param left_executor The child executor that produces tuple for the left side of join
 * @param right_executor The child executor that produces tuple for the right side of join
 */
NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

/** Initialize the join */
void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_tuples_.clear();
  left_batch_.clear();
  left_offset_ = 0;
  current_left_.reset();
  right_offset_ = 0;
  matched_ = false;
  right_exhausted_ = false;
}

/**
 * Yield the next tuple batch from the join.
 * @param[out] tuple_batch The next tuple batch produced by the join
 * @param[out] rid_batch The next tuple RID batch produced by the join
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto NestedLoopJoinExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                  size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();
  const auto &left_schema = left_executor_->GetOutputSchema();
  const auto &right_schema = right_executor_->GetOutputSchema();
  while (tuple_batch->size() < batch_size) {
    if (!current_left_.has_value()) {
      if (left_offset_ >= left_batch_.size()) {
        if (!left_executor_->Next(&left_batch_, &left_rids_, BUSTUB_BATCH_SIZE)) {
          break;
        }
        left_offset_ = 0;
      }
      current_left_ = left_batch_[left_offset_++];
      right_executor_->Init();
      right_tuples_.clear();
      right_offset_ = 0;
      matched_ = false;
      right_exhausted_ = false;
    }
    if (right_offset_ >= right_tuples_.size() && !right_exhausted_) {
      right_exhausted_ = !right_executor_->Next(&right_tuples_, &right_rids_, BUSTUB_BATCH_SIZE);
      right_offset_ = 0;
    }
    if (right_exhausted_) {
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
      continue;
    }
    const auto &right = right_tuples_[right_offset_++];
    auto predicate = plan_->Predicate()->EvaluateJoin(&*current_left_, left_schema, &right, right_schema);
    if (predicate.IsNull() || !predicate.GetAs<bool>()) {
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
  return !tuple_batch->empty();
}

}  // namespace bustub
