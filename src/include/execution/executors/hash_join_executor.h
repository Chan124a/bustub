//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <list>
#include <memory>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value.h"

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  struct HashJoinKey {
    std::vector<Value> value;
    bool is_null{false};
    bool operator==(const HashJoinKey &other) const {
      if (this->is_null || other.is_null) {
        return false;
      }
      if (other.value.size() != this->value.size()) {
        return false;
      }
      for (size_t i = 0; i < this->value.size(); i++) {
        CmpBool res = this->value[i].CompareNotEquals(other.value[i]);
        if (res == CmpBool::CmpTrue || res == CmpBool::CmpNull) {
          return false;
        }
      }
      return true;
    }
  };
  struct HashJoinFunc {
    size_t operator()(const HashJoinKey &key) const {
      hash_t hash_value{0};
      for (size_t i = 0; i < key.value.size(); i++) {
        if (i == 0) {
          hash_value = HashUtil::HashValue(&key.value[i]);
        } else {
          hash_value = HashUtil::CombineHashes(hash_value, HashUtil::HashValue(&key.value[i]));
        }
      }
      return hash_value;
    }
  };
  void MakeHashJoinKey(const Tuple *tuple, const std::vector<AbstractExpressionRef> &expressions, const Schema &schema,
                       HashJoinKey &hash_join_key);
  void AdvanceLeftTuple();
  void MakeLeftJoinNullTuple(Tuple *tuple);

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;
  std::unordered_map<HashJoinKey, std::vector<Tuple>, HashJoinFunc> ht_;
  bool has_left_{false};
  HashJoinKey left_key_{};
  Tuple left_tuple_{};
  size_t index_;
};

}  // namespace bustub
