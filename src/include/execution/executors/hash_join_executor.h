//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <array>
#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/page/intermediate_result_page.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);
  ~HashJoinExecutor() override;

  void Init() override;

  auto Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;

  struct JoinKey {
    std::vector<Value> values_;
    auto operator==(const JoinKey &other) const -> bool {
      if (values_.size() != other.values_.size()) {
        return false;
      }
      for (size_t i = 0; i < values_.size(); i++) {
        if (values_[i].CompareEquals(other.values_[i]) != CmpBool::CmpTrue) {
          return false;
        }
      }
      return true;
    }
    auto HasNull() const -> bool {
      for (const auto &value : values_) {
        if (value.IsNull()) {
          return true;
        }
      }
      return false;
    }
  };
  struct JoinKeyHash {
    auto operator()(const JoinKey &key) const -> size_t {
      size_t hash = 0;
      for (const auto &value : key.values_) {
        hash = HashUtil::CombineHashes(hash, value.IsNull() ? 0 : HashUtil::HashValue(&value));
      }
      return hash;
    }
  };
  static constexpr size_t NUM_PARTITIONS = 16;
  using Partition = std::vector<page_id_t>;
  std::array<Partition, NUM_PARTITIONS> left_partitions_;
  std::array<Partition, NUM_PARTITIONS> right_partitions_;
  std::unordered_map<JoinKey, std::vector<Tuple>, JoinKeyHash> right_map_;
  size_t partition_idx_{0};
  bool partition_loaded_{false};
  size_t left_page_idx_{0};
  uint32_t left_byte_offset_{0};
  std::optional<Tuple> current_left_;
  const std::vector<Tuple> *matches_{nullptr};
  size_t match_offset_{0};

  auto MakeKey(const Tuple &tuple, const Schema &schema, const std::vector<AbstractExpressionRef> &expressions) const
      -> JoinKey;
  void AppendToPartition(Partition *partition, const Tuple &tuple);
  void PartitionChild(AbstractExecutor *child, const std::vector<AbstractExpressionRef> &expressions,
                      std::array<Partition, NUM_PARTITIONS> *partitions);
  void LoadPartition();
  auto ReadLeftTuple(Tuple *tuple) -> bool;
  void DeletePages(Partition *partition);
  void Cleanup();
};

}  // namespace bustub
