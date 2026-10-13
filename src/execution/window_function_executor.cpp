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
// 沿用 BusTub 聚合执行器的初始值约定：COUNT(*) 从 0 开始，其他聚合从 NULL 开始。
// 后续遇到第一个非 NULL 输入时，再为这些聚合建立有效的累计值。
auto InitialValue(WindowFunctionType type) -> Value {
  if (type == WindowFunctionType::CountStarAggregate) {
    return ValueFactory::GetIntegerValue(0);
  }
  return ValueFactory::GetNullValueByType(TypeId::INTEGER);
}

// 将一行的输入合并到当前分区的聚合结果中。COUNT(*) 统计所有行，
// COUNT(expr)、SUM、MIN 和 MAX 忽略 NULL 输入；RANK 由单独的排名逻辑处理。
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

// 判断两行是否具有相同的完整 ORDER BY 键，供 RANK 识别并列排名。
// 此处两个 NULL 视为同一排序值，不能直接采用 SQL 中 NULL = NULL 的比较结果。
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
  // Init 完成全部窗口计算，Next 只负责返回结果批次。
  // 每次重新初始化都清空结果和游标，以支持同一个执行器重复执行。
  child_executor_->Init();
  output_.clear();
  next_tuple_ = 0;
  std::vector<Tuple> inputs;
  std::vector<Tuple> tuples;
  std::vector<RID> rids;
  // 窗口结果可能依赖整个分区，因此先按批次读完子执行器并保存输入。
  while (child_executor_->Next(&tuples, &rids, BUSTUB_BATCH_SIZE)) {
    inputs.insert(inputs.end(), tuples.begin(), tuples.end());
  }
  if (inputs.empty()) {
    return;
  }
  const auto &schema = child_executor_->GetOutputSchema();
  // 规划器保证同一查询中的窗口函数使用相同的 ORDER BY，只需排序一次。
  // 全局排序也保证每个分区内部的相对顺序正确；不同分区可以在输入中交错出现。
  // 没有 ORDER BY 时保留子执行器的输入顺序；稳定排序保留相同键的原始相对顺序。
  if (!plan_->window_functions_.empty()) {
    const auto &order_bys = plan_->window_functions_.begin()->second.order_by_;
    if (!order_bys.empty()) {
      TupleComparator comparator(order_bys);
      std::stable_sort(inputs.begin(), inputs.end(), [&](const Tuple &a, const Tuple &b) {
        return comparator({GenerateSortKey(a, order_bys, schema), a}, {GenerateSortKey(b, order_bys, schema), b});
      });
    }
  }
  // 先计算普通输出列，为窗口列预留位置。窗口列在 columns_ 中是占位表达式，
  // 包含无效列号，不能调用 Evaluate；下面的窗口计算会覆盖这些临时 NULL 值。
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
  // 每个窗口函数独立维护分区状态，因为它们可能采用不同的 PARTITION BY。
  // column 是该窗口函数在最终输出元组中的列号。
  for (const auto &[column, function] : plan_->window_functions_) {
    struct State {
      // 普通窗口聚合的累计值。
      Value aggregate_;
      // RANK 使用的分区内已处理行数、当前排名和上一行排序键。
      size_t seen_{0};
      size_t rank_{0};
      SortKey previous_key_;
    };
    std::unordered_map<AggregateKey, State> partitions;
    const bool ordered = !function.order_by_.empty();
    for (size_t row = 0; row < inputs.size(); row++) {
      // PARTITION BY 的表达式值组成哈希表键；空键表示所有行属于同一分区。
      // AggregateKey 的相等比较会将相同位置的两个 NULL 归入同一个分区。
      AggregateKey partition;
      for (const auto &expr : function.partition_by_) {
        partition.group_bys_.push_back(expr->Evaluate(&inputs[row], schema));
      }
      // 只有首次遇到该分区时才插入初始状态，之后继续使用已有的累计结果。
      auto [it, inserted] = partitions.try_emplace(partition, State{InitialValue(function.type_), 0, 0, {}});
      auto &state = it->second;
      if (function.type_ == WindowFunctionType::Rank) {
        auto key = GenerateSortKey(inputs[row], function.order_by_, schema);
        state.seen_++;
        // 新排序键的排名等于它在分区中的行位置；相同排序键沿用上一个排名。
        // 例如键序列 [10, 10, 20] 的排名为 [1, 1, 3]，并列后会跳过名次。
        if (inserted || !SameSortKey(key, state.previous_key_)) {
          state.rank_ = state.seen_;
        }
        state.previous_key_ = std::move(key);
        values[row][column] = ValueFactory::GetIntegerValue(static_cast<int32_t>(state.rank_));
      } else {
        auto input = function.function_->Evaluate(&inputs[row], schema);
        Accumulate(function.type_, &state.aggregate_, input);
        if (ordered) {
          // 有 ORDER BY 时，记录从分区第一行累计到当前行的聚合结果。
          // Value 被复制到输出位置，后续累加不会改变前面行已记录的值。
          values[row][column] = state.aggregate_;
        }
      }
    }
    if (!ordered && function.type_ != WindowFunctionType::Rank) {
      // 没有 ORDER BY 时窗口覆盖整个分区。第一遍已经算出各分区的最终结果，
      // 第二遍将同一个最终值写入该分区的每一行。
      for (size_t row = 0; row < inputs.size(); row++) {
        AggregateKey partition;
        for (const auto &expr : function.partition_by_) {
          partition.group_bys_.push_back(expr->Evaluate(&inputs[row], schema));
        }
        values[row][column] = partitions.at(partition).aggregate_;
      }
    }
  }
  // 将普通列与窗口列按计划的输出 schema 序列化为元组，供 Next 分批返回。
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
  // 每次调用最多返回 batch_size 行；next_tuple_ 保存跨批次的读取位置。
  tuple_batch->clear();
  rid_batch->clear();
  while (tuple_batch->size() < batch_size && next_tuple_ < output_.size()) {
    tuple_batch->push_back(output_[next_tuple_++]);
    // 窗口输出是计算得到的新元组，没有对应的表记录，使用默认的无效 RID。
    // RID 批次仍与元组批次保持相同长度，便于父执行器按位置读取。
    rid_batch->emplace_back();
  }
  return !tuple_batch->empty();
}
}  // namespace bustub
