//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nlj_as_hash_join.cpp
//
// Identification: src/optimizer/nlj_as_hash_join.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {
namespace {
auto ExtractKeys(const AbstractExpressionRef &expr, std::vector<AbstractExpressionRef> *left,
                 std::vector<AbstractExpressionRef> *right) -> bool {
  if (auto logic = dynamic_cast<const LogicExpression *>(expr.get()); logic != nullptr) {
    return logic->logic_type_ == LogicType::And && ExtractKeys(logic->GetChildAt(0), left, right) &&
           ExtractKeys(logic->GetChildAt(1), left, right);
  }
  auto comparison = dynamic_cast<const ComparisonExpression *>(expr.get());
  if (comparison == nullptr || comparison->comp_type_ != ComparisonType::Equal) {
    return false;
  }
  auto first = dynamic_cast<const ColumnValueExpression *>(comparison->GetChildAt(0).get());
  auto second = dynamic_cast<const ColumnValueExpression *>(comparison->GetChildAt(1).get());
  if (first == nullptr || second == nullptr || first->GetTupleIdx() == second->GetTupleIdx() ||
      first->GetTupleIdx() > 1 || second->GetTupleIdx() > 1) {
    return false;
  }
  auto left_col = first->GetTupleIdx() == 0 ? first : second;
  auto right_col = first->GetTupleIdx() == 1 ? first : second;
  left->push_back(std::make_shared<ColumnValueExpression>(0, left_col->GetColIdx(), left_col->GetReturnType()));
  right->push_back(std::make_shared<ColumnValueExpression>(0, right_col->GetColIdx(), right_col->GetReturnType()));
  return true;
}
}  // namespace

/**
 * @brief optimize nested loop join into hash join.
 * In the starter code, we will check NLJs with exactly one equal condition. You can further support optimizing joins
 * with multiple eq conditions.
 */
auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.push_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized = plan->CloneWithChildren(std::move(children));
  if (optimized->GetType() != PlanType::NestedLoopJoin) {
    return optimized;
  }
  const auto &join = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized);
  std::vector<AbstractExpressionRef> left_keys;
  std::vector<AbstractExpressionRef> right_keys;
  if (!ExtractKeys(join.Predicate(), &left_keys, &right_keys) || left_keys.empty()) {
    return optimized;
  }
  return std::make_shared<HashJoinPlanNode>(join.output_schema_, join.GetLeftPlan(), join.GetRightPlan(),
                                            std::move(left_keys), std::move(right_keys), join.GetJoinType());
}

}  // namespace bustub
