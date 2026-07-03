#include <algorithm>
#include <cstddef>
#include <memory>
#include <utility>
#include <vector>
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

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    BUSTUB_ENSURE(optimized_plan->GetChildren().size() == 2, "NLJ should have exactly 2 children.");
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    if (nlj_plan.GetJoinType() == JoinType::INNER || nlj_plan.GetJoinType() == JoinType::LEFT) {
      std::vector<AbstractExpressionRef> left_key_expressions;
      std::vector<AbstractExpressionRef> right_key_expressions;
      if (MakeHashJoinExpression(nlj_plan.Predicate(), left_key_expressions, right_key_expressions)) {
        return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                  nlj_plan.GetRightPlan(), left_key_expressions, right_key_expressions,
                                                  nlj_plan.GetJoinType());
      }
    }
  }
  return optimized_plan;
}

auto Optimizer::MakeHashJoinExpression(const AbstractExpressionRef expr,
                                       std::vector<AbstractExpressionRef> &left_key_expressions,
                                       std::vector<AbstractExpressionRef> &right_key_expressions) -> bool {
  const auto *comparison_expr = dynamic_cast<const ComparisonExpression *>(expr.get());
  if (comparison_expr != nullptr && comparison_expr->comp_type_ == ComparisonType::Equal) {
    if (comparison_expr->GetChildren().size() == 2) {
      const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(comparison_expr->GetChildAt(0).get());
      const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(comparison_expr->GetChildAt(1).get());
      if (left_expr != nullptr && right_expr != nullptr) {
        if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
          left_key_expressions.push_back(comparison_expr->GetChildAt(0));
          right_key_expressions.push_back(comparison_expr->GetChildAt(1));
          return true;
        }
        if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
          left_key_expressions.push_back(comparison_expr->GetChildAt(1));
          right_key_expressions.push_back(comparison_expr->GetChildAt(0));
          return true;
        }
      }
    }
  }
  const auto *logic_expr = dynamic_cast<const LogicExpression *>(expr.get());
  if (logic_expr != nullptr && logic_expr->logic_type_ == LogicType::And) {
    if (logic_expr->GetChildren().size() == 2) {
      std::size_t left_size = left_key_expressions.size();
      std::size_t right_size = right_key_expressions.size();
      if (MakeHashJoinExpression(logic_expr->GetChildAt(0), left_key_expressions, right_key_expressions) &&
          MakeHashJoinExpression(logic_expr->GetChildAt(1), left_key_expressions, right_key_expressions)) {
        return true;
      }
      // 失败时，回滚左右两个vector的size
      left_key_expressions.resize(left_size);
      right_key_expressions.resize(right_size);
    }
  }
  return false;
}

}  // namespace bustub
