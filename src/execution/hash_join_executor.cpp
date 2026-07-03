//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <cstddef>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "catalog/schema.h"
#include "common/util/hash_util.h"
#include "execution/expressions/abstract_expression.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  const auto &left_keys = plan_->LeftJoinKeyExpressions();
  const auto &right_keys = plan_->RightJoinKeyExpressions();

  if (left_keys.empty() || left_keys.size() != right_keys.size()) {
    throw ExecutionException("Invalid hash join plan: join keys must be non-empty and paired.");
  }
}

void HashJoinExecutor::Init() {
  ht_.clear();
  has_left_ = false;

  left_executor_->Init();
  right_executor_->Init();
  Tuple right_tuple{};
  RID right_rid{};
  while (right_executor_->Next(&right_tuple, &right_rid)) {
    HashJoinKey right_key;
    MakeHashJoinKey(&right_tuple, plan_->RightJoinKeyExpressions(), right_executor_->GetOutputSchema(), right_key);
    if (!right_key.is_null) {
      ht_[right_key].push_back(right_tuple);
    }
  }

  RID left_rid;
  has_left_ = left_executor_->Next(&left_tuple_, &left_rid);
  if (has_left_) {
    MakeHashJoinKey(&left_tuple_, plan_->LeftJoinKeyExpressions(), left_executor_->GetOutputSchema(), left_key_);
  }
  index_ = 0;
}

void HashJoinExecutor::MakeHashJoinKey(const Tuple *tuple, const std::vector<AbstractExpressionRef> &expressions,
                                       const Schema &schema, HashJoinKey &hash_join_key) {
  hash_join_key.value.clear();
  hash_join_key.is_null = false;
  hash_join_key.value.reserve(expressions.size());
  for (size_t i = 0; i < expressions.size(); i++) {
    auto expr = expressions[i];
    Value value = expr->Evaluate(tuple, schema);
    if (value.IsNull()) {
      hash_join_key.is_null = true;
    }
    hash_join_key.value.push_back(value);
  }
}

void HashJoinExecutor::AdvanceLeftTuple() {
  RID left_rid;
  has_left_ = left_executor_->Next(&left_tuple_, &left_rid);
  if (has_left_) {
    MakeHashJoinKey(&left_tuple_, plan_->LeftJoinKeyExpressions(), left_executor_->GetOutputSchema(), left_key_);
  }
  index_ = 0;
}

void HashJoinExecutor::MakeLeftJoinNullTuple(Tuple *tuple) {
  std::vector<Value> new_value;
  new_value.reserve(GetOutputSchema().GetColumnCount());
  for (size_t j = 0; j < left_executor_->GetOutputSchema().GetColumnCount(); j++) {
    new_value.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), j));
  }
  for (size_t j = 0; j < right_executor_->GetOutputSchema().GetColumnCount(); j++) {
    new_value.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(j).GetType()));
  }
  *tuple = {new_value, &GetOutputSchema()};
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (has_left_) {
    if (left_key_.is_null) {
      if (plan_->GetJoinType() == JoinType::LEFT) {
        // 插入初始值
        MakeLeftJoinNullTuple(tuple);
        // 切换下一个left
        AdvanceLeftTuple();
        return true;
      }
      // 切换下一个left
      AdvanceLeftTuple();
      continue;
    }
    const auto it = ht_.find(left_key_);
    if (it == ht_.end()) {
      if (plan_->GetJoinType() == JoinType::LEFT) {
        // 插入初始值
        MakeLeftJoinNullTuple(tuple);

        // 切换下一个left
        AdvanceLeftTuple();
        return true;
      }
      // 切换下一个left
      AdvanceLeftTuple();
      continue;
    }
    if (index_ < it->second.size()) {
      // 插入新tuple
      // 增加index
      Tuple right_tuple = it->second[index_];
      index_++;
      std::vector<Value> new_value;
      new_value.reserve(GetOutputSchema().GetColumnCount());
      for (size_t j = 0; j < left_executor_->GetOutputSchema().GetColumnCount(); j++) {
        new_value.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), j));
      }
      for (size_t j = 0; j < right_executor_->GetOutputSchema().GetColumnCount(); j++) {
        new_value.push_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), j));
      }
      *tuple = {new_value, &GetOutputSchema()};
      return true;
    }
    // 切换下一个left
    AdvanceLeftTuple();
  }
  return false;
}

}  // namespace bustub
