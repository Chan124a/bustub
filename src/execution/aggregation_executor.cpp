//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <utility>
#include <vector>
#include "common/rid.h"
#include "execution/plans/aggregation_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.End()) {}

void AggregationExecutor::Init() {
  child_->Init();
  aht_.Clear();
  bool has_input = false;

  Tuple child_tuple{};
  RID child_rid{};

  while (child_->Next(&child_tuple, &child_rid)) {
    AggregateKey key = MakeAggregateKey(&child_tuple);
    AggregateValue value = MakeAggregateValue(&child_tuple);
    aht_.InsertCombine(key, value);
    has_input = true;
  }
  /*
   * 空表且没有 GROUP BY 时，期望返回一行聚合结果.
   * 在这个项目的测试语义中：
   * | 查询        | 期望结果      |
   * | count(*)   | 0            |
   * | count(col) | integer_null |
   * | sum(col)   | integer_null |
   * | min(col)   | integer_null |
   * | max(col)   | integer_null |
   *
   * 空表有 GROUP BY 时，期望是 0 行结果。
   */
  if (!has_input && plan_->GetGroupBys().empty()) {
    aht_.InsertInitial(AggregateKey{});
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }

  std::vector<Value> output_value = aht_iterator_.Key().group_bys_;
  output_value.insert(output_value.end(), aht_iterator_.Val().aggregates_.begin(),
                      aht_iterator_.Val().aggregates_.end());

  *tuple = Tuple{output_value, &GetOutputSchema()};
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
