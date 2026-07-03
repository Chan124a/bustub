//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <cstddef>
#include <utility>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  has_left_ = false;
  left_tuple_ = Tuple{};
  match_ = false;

  left_executor_->Init();
  right_executor_->Init();
  RID left_rid;
  has_left_ = left_executor_->Next(&left_tuple_, &left_rid);
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (has_left_) {
    Tuple right_tuple{};
    RID right_rid{};
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      auto value = plan_->Predicate()->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                    right_executor_->GetOutputSchema());
      if (!value.IsNull() && value.GetAs<bool>()) {
        match_ = true;
        if (plan_->GetJoinType() == JoinType::INNER || plan_->GetJoinType() == JoinType::LEFT) {
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
      }
    }
    if (plan_->GetJoinType() == JoinType::LEFT && !match_) {
      std::vector<Value> new_value;
      new_value.reserve(GetOutputSchema().GetColumnCount());
      for (size_t j = 0; j < left_executor_->GetOutputSchema().GetColumnCount(); j++) {
        new_value.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), j));
      }
      for (size_t j = 0; j < right_executor_->GetOutputSchema().GetColumnCount(); j++) {
        new_value.push_back(
            ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(j).GetType()));
      }
      *tuple = {new_value, &GetOutputSchema()};

      right_executor_->Init();
      RID left_rid;
      has_left_ = left_executor_->Next(&left_tuple_, &left_rid);
      match_ = false;
      return true;
    }

    right_executor_->Init();
    RID left_rid;
    has_left_ = left_executor_->Next(&left_tuple_, &left_rid);
    match_ = false;
  }
  return false;
}

}  // namespace bustub
