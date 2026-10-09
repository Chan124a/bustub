//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seqscan_as_indexscan.cpp
//
// Identification: src/optimizer/seqscan_as_indexscan.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <memory>
#include <unordered_map>
#include <vector>
#include "optimizer/optimizer.h"

#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"

namespace bustub {
namespace {
using EqualityMap = std::unordered_map<uint32_t, AbstractExpressionRef>;

auto CollectEqualities(const AbstractExpressionRef &expr, EqualityMap *equalities) -> bool {
  if (auto logic = dynamic_cast<const LogicExpression *>(expr.get()); logic != nullptr) {
    if (logic->logic_type_ != LogicType::And) {
      return false;
    }
    return CollectEqualities(logic->GetChildAt(0), equalities) && CollectEqualities(logic->GetChildAt(1), equalities);
  }
  auto comparison = dynamic_cast<const ComparisonExpression *>(expr.get());
  if (comparison == nullptr || comparison->comp_type_ != ComparisonType::Equal) {
    // Other conjuncts remain in the index scan's residual filter.
    return true;
  }
  for (int side = 0; side < 2; side++) {
    auto column = dynamic_cast<const ColumnValueExpression *>(comparison->GetChildAt(side).get());
    auto constant = dynamic_cast<const ConstantValueExpression *>(comparison->GetChildAt(1 - side).get());
    if (column != nullptr && constant != nullptr && column->GetTupleIdx() == 0 && !constant->val_.IsNull()) {
      equalities->emplace(column->GetColIdx(), comparison->GetChildAt(1 - side));
      break;
    }
  }
  return true;
}

auto CollectDisjuncts(const AbstractExpressionRef &expr, std::vector<EqualityMap> *disjuncts) -> bool {
  if (auto logic = dynamic_cast<const LogicExpression *>(expr.get());
      logic != nullptr && logic->logic_type_ == LogicType::Or) {
    return CollectDisjuncts(logic->GetChildAt(0), disjuncts) && CollectDisjuncts(logic->GetChildAt(1), disjuncts);
  }
  EqualityMap equalities;
  if (!CollectEqualities(expr, &equalities)) {
    return false;
  }
  disjuncts->push_back(std::move(equalities));
  return true;
}
}  // namespace

/**
 * @brief Optimizes seq scan as index scan if there's an index on a table
 */
auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.push_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized = plan->CloneWithChildren(std::move(children));
  if (optimized->GetType() != PlanType::SeqScan) {
    return optimized;
  }
  const auto &scan = dynamic_cast<const SeqScanPlanNode &>(*optimized);
  if (scan.filter_predicate_ == nullptr) {
    return optimized;
  }
  std::vector<EqualityMap> disjuncts;
  if (!CollectDisjuncts(scan.filter_predicate_, &disjuncts)) {
    return optimized;
  }
  auto table = catalog_.GetTable(scan.table_oid_);
  size_t best_prefix = 0;
  std::shared_ptr<IndexInfo> best_index;
  for (const auto &index : catalog_.GetTableIndexes(table->name_)) {
    if (index->index_type_ != IndexType::BPlusTreeIndex) {
      continue;
    }
    size_t shortest = index->index_->GetKeyAttrs().size();
    for (const auto &disjunct : disjuncts) {
      size_t prefix = 0;
      for (auto attr : index->index_->GetKeyAttrs()) {
        if (disjunct.count(attr) == 0) {
          break;
        }
        prefix++;
      }
      shortest = std::min(shortest, prefix);
    }
    if (shortest > best_prefix) {
      best_prefix = shortest;
      best_index = index;
    }
  }
  if (best_index == nullptr) {
    return optimized;
  }
  auto index_scan = std::make_shared<IndexScanPlanNode>(scan.output_schema_, scan.table_oid_, best_index->index_oid_,
                                                        scan.filter_predicate_);
  for (const auto &disjunct : disjuncts) {
    std::vector<AbstractExpressionRef> keys;
    for (size_t i = 0; i < best_prefix; i++) {
      keys.push_back(disjunct.at(best_index->index_->GetKeyAttrs()[i]));
    }
    index_scan->lookup_keys_.push_back(std::move(keys));
  }
  return index_scan;
}

}  // namespace bustub
