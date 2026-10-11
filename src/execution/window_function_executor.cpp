//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.cpp
//
// Identification: src/execution/window_function_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/window_function_executor.h"
#include <algorithm>
#include <unordered_map>

#include "execution/execution_common.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {
namespace {
auto InitialValue(WindowFunctionType type) -> Value {
  if (type == WindowFunctionType::CountStarAggregate) {
    return ValueFactory::GetIntegerValue(0);
  }
  return ValueFactory::GetNullValueByType(TypeId::INTEGER);
}

void Accumulate(WindowFunctionType type, Value *current, const Value &input) {
  switch (type) {
    case WindowFunctionType::CountStarAggregate:
      *current = current->Add(ValueFactory::GetIntegerValue(1));
      break;
    case WindowFunctionType::CountAggregate:
      if (!input.IsNull()) {
        *current =
            current->IsNull() ? ValueFactory::GetIntegerValue(1) : current->Add(ValueFactory::GetIntegerValue(1));
      }
      break;
    case WindowFunctionType::SumAggregate:
      if (!input.IsNull()) {
        *current = current->IsNull() ? input : current->Add(input);
      }
      break;
    case WindowFunctionType::MinAggregate:
      if (!input.IsNull() && (current->IsNull() || input.CompareLessThan(*current) == CmpBool::CmpTrue)) {
        *current = input;
      }
      break;
    case WindowFunctionType::MaxAggregate:
      if (!input.IsNull() && (current->IsNull() || input.CompareGreaterThan(*current) == CmpBool::CmpTrue)) {
        *current = input;
      }
      break;
    case WindowFunctionType::Rank:
      break;
  }
}

auto SameSortKey(const SortKey &a, const SortKey &b) -> bool {
  if (a.size() != b.size()) {
    return false;
  }
  for (size_t i = 0; i < a.size(); i++) {
    if (a[i].IsNull() || b[i].IsNull()) {
      if (!a[i].IsNull() || !b[i].IsNull()) {
        return false;
      }
    } else if (a[i].CompareEquals(b[i]) != CmpBool::CmpTrue) {
      return false;
    }
  }
  return true;
}
}  // namespace

/**
 * Construct a new WindowFunctionExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The window aggregation plan to be executed
 */
WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/** Initialize the window aggregation */
void WindowFunctionExecutor::Init() {
  child_executor_->Init();
  output_.clear();
  next_tuple_ = 0;
  std::vector<Tuple> inputs;
  std::vector<Tuple> tuples;
  std::vector<RID> rids;
  while (child_executor_->Next(&tuples, &rids, BUSTUB_BATCH_SIZE)) {
    inputs.insert(inputs.end(), tuples.begin(), tuples.end());
  }
  if (inputs.empty()) {
    return;
  }
  const auto &schema = child_executor_->GetOutputSchema();
  if (!plan_->window_functions_.empty()) {
    const auto &order_bys = plan_->window_functions_.begin()->second.order_by_;
    if (!order_bys.empty()) {
      TupleComparator comparator(order_bys);
      std::stable_sort(inputs.begin(), inputs.end(), [&](const Tuple &a, const Tuple &b) {
        return comparator({GenerateSortKey(a, order_bys, schema), a}, {GenerateSortKey(b, order_bys, schema), b});
      });
    }
  }
  std::vector<std::vector<Value>> values(inputs.size());
  for (size_t row = 0; row < inputs.size(); row++) {
    for (uint32_t col = 0; col < plan_->columns_.size(); col++) {
      if (plan_->window_functions_.count(col) != 0) {
        values[row].push_back(ValueFactory::GetNullValueByType(GetOutputSchema().GetColumn(col).GetType()));
      } else {
        values[row].push_back(plan_->columns_[col]->Evaluate(&inputs[row], schema));
      }
    }
  }
  for (const auto &[column, function] : plan_->window_functions_) {
    struct State {
      Value aggregate_;
      size_t seen_{0};
      size_t rank_{0};
      SortKey previous_key_;
    };
    std::unordered_map<AggregateKey, State> partitions;
    const bool ordered = !function.order_by_.empty();
    for (size_t row = 0; row < inputs.size(); row++) {
      AggregateKey partition;
      for (const auto &expr : function.partition_by_) {
        partition.group_bys_.push_back(expr->Evaluate(&inputs[row], schema));
      }
      auto [it, inserted] = partitions.try_emplace(partition, State{InitialValue(function.type_), 0, 0, {}});
      auto &state = it->second;
      if (function.type_ == WindowFunctionType::Rank) {
        auto key = GenerateSortKey(inputs[row], function.order_by_, schema);
        state.seen_++;
        if (inserted || !SameSortKey(key, state.previous_key_)) {
          state.rank_ = state.seen_;
        }
        state.previous_key_ = std::move(key);
        values[row][column] = ValueFactory::GetIntegerValue(static_cast<int32_t>(state.rank_));
      } else {
        auto input = function.function_->Evaluate(&inputs[row], schema);
        Accumulate(function.type_, &state.aggregate_, input);
        if (ordered) {
          values[row][column] = state.aggregate_;
        }
      }
    }
    if (!ordered && function.type_ != WindowFunctionType::Rank) {
      for (size_t row = 0; row < inputs.size(); row++) {
        AggregateKey partition;
        for (const auto &expr : function.partition_by_) {
          partition.group_bys_.push_back(expr->Evaluate(&inputs[row], schema));
        }
        values[row][column] = partitions.at(partition).aggregate_;
      }
    }
  }
  output_.reserve(values.size());
  for (auto &row : values) {
    output_.emplace_back(row, &GetOutputSchema());
  }
}

/**
 * Yield the next tuple batch from the window aggregation.
 * @param[out] tuple_batch The next tuple batch produced by the window aggregation
 * @param[out] rid_batch The next tuple RID batch produced by the window aggregation
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto WindowFunctionExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                  size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();
  while (tuple_batch->size() < batch_size && next_tuple_ < output_.size()) {
    tuple_batch->push_back(output_[next_tuple_++]);
    rid_batch->emplace_back();
  }
  return !tuple_batch->empty();
}
}  // namespace bustub
