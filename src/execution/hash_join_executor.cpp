//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <stdexcept>

#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new HashJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The HashJoin join plan to be executed
 * @param left_child The child executor that produces tuples for the left side of join
 * @param right_child The child executor that produces tuples for the right side of join
 */
HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

HashJoinExecutor::~HashJoinExecutor() { Cleanup(); }

auto HashJoinExecutor::MakeKey(const Tuple &tuple, const Schema &schema,
                               const std::vector<AbstractExpressionRef> &expressions) const -> JoinKey {
  JoinKey key;
  for (const auto &expression : expressions) {
    key.values_.push_back(expression->Evaluate(&tuple, schema));
  }
  return key;
}

void HashJoinExecutor::AppendToPartition(Partition *partition, const Tuple &tuple) {
  auto *bpm = exec_ctx_->GetBufferPoolManager();
  if (!partition->empty()) {
    auto guard = bpm->WritePage(partition->back());
    if (guard.AsMut<IntermediateResultPage>()->Append(tuple)) {
      return;
    }
  }
  auto page_id = bpm->NewPage();
  auto guard = bpm->WritePage(page_id);
  auto *page = guard.AsMut<IntermediateResultPage>();
  page->Init();
  if (!page->Append(tuple)) {
    throw std::length_error("Join tuple exceeds an intermediate page");
  }
  partition->push_back(page_id);
}

void HashJoinExecutor::PartitionChild(AbstractExecutor *child, const std::vector<AbstractExpressionRef> &expressions,
                                      std::array<Partition, NUM_PARTITIONS> *partitions) {
  std::vector<Tuple> tuples;
  std::vector<RID> rids;
  while (child->Next(&tuples, &rids, BUSTUB_BATCH_SIZE)) {
    for (const auto &tuple : tuples) {
      auto key = MakeKey(tuple, child->GetOutputSchema(), expressions);
      auto partition_idx = JoinKeyHash{}(key) % NUM_PARTITIONS;
      AppendToPartition(&(*partitions)[partition_idx], tuple);
    }
  }
}

void HashJoinExecutor::DeletePages(Partition *partition) {
  for (auto page_id : *partition) {
    exec_ctx_->GetBufferPoolManager()->DeletePage(page_id);
  }
  partition->clear();
}

void HashJoinExecutor::Cleanup() {
  for (auto &partition : left_partitions_) {
    DeletePages(&partition);
  }
  for (auto &partition : right_partitions_) {
    DeletePages(&partition);
  }
}

void HashJoinExecutor::LoadPartition() {
  right_map_.clear();
  for (auto page_id : right_partitions_[partition_idx_]) {
    auto guard = exec_ctx_->GetBufferPoolManager()->ReadPage(page_id);
    const auto *page = guard.As<IntermediateResultPage>();
    uint32_t offset = 0;
    while (offset < page->GetUsed()) {
      auto tuple = page->ReadTuple(&offset);
      auto key = MakeKey(tuple, right_child_->GetOutputSchema(), plan_->RightJoinKeyExpressions());
      if (!key.HasNull()) {
        right_map_[std::move(key)].push_back(std::move(tuple));
      }
    }
  }
  DeletePages(&right_partitions_[partition_idx_]);
  left_page_idx_ = 0;
  left_byte_offset_ = 0;
  partition_loaded_ = true;
}

auto HashJoinExecutor::ReadLeftTuple(Tuple *tuple) -> bool {
  const auto &pages = left_partitions_[partition_idx_];
  while (left_page_idx_ < pages.size()) {
    auto guard = exec_ctx_->GetBufferPoolManager()->ReadPage(pages[left_page_idx_]);
    const auto *page = guard.As<IntermediateResultPage>();
    if (left_byte_offset_ < page->GetUsed()) {
      *tuple = page->ReadTuple(&left_byte_offset_);
      return true;
    }
    left_page_idx_++;
    left_byte_offset_ = 0;
  }
  return false;
}

/** Initialize the join */
void HashJoinExecutor::Init() {
  Cleanup();
  left_child_->Init();
  right_child_->Init();
  PartitionChild(right_child_.get(), plan_->RightJoinKeyExpressions(), &right_partitions_);
  PartitionChild(left_child_.get(), plan_->LeftJoinKeyExpressions(), &left_partitions_);
  partition_idx_ = 0;
  partition_loaded_ = false;
  current_left_.reset();
  matches_ = nullptr;
  match_offset_ = 0;
}

/**
 * Yield the next tuple batch from the hash join.
 * @param[out] tuple_batch The next tuple batch produced by the hash join
 * @param[out] rid_batch The next tuple RID batch produced by the hash join
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto HashJoinExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                            size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();
  const auto &left_schema = left_child_->GetOutputSchema();
  const auto &right_schema = right_child_->GetOutputSchema();
  while (tuple_batch->size() < batch_size && partition_idx_ < NUM_PARTITIONS) {
    if (!partition_loaded_) {
      LoadPartition();
    }
    if (!current_left_.has_value()) {
      Tuple tuple;
      if (!ReadLeftTuple(&tuple)) {
        DeletePages(&left_partitions_[partition_idx_]);
        right_map_.clear();
        partition_idx_++;
        partition_loaded_ = false;
        continue;
      }
      current_left_ = std::move(tuple);
      auto key = MakeKey(*current_left_, left_schema, plan_->LeftJoinKeyExpressions());
      matches_ = nullptr;
      match_offset_ = 0;
      if (!key.HasNull()) {
        auto found = right_map_.find(key);
        if (found != right_map_.end()) {
          matches_ = &found->second;
        }
      }
    }
    const Tuple *right = nullptr;
    if (matches_ != nullptr && match_offset_ < matches_->size()) {
      right = &(*matches_)[match_offset_++];
    } else if (matches_ == nullptr && plan_->GetJoinType() == JoinType::LEFT) {
      // Emit one null-extended row for an unmatched left tuple.
    } else {
      current_left_.reset();
      continue;
    }
    std::vector<Value> values;
    for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
      values.push_back(current_left_->GetValue(&left_schema, i));
    }
    if (right == nullptr) {
      for (const auto &column : right_schema.GetColumns()) {
        values.push_back(ValueFactory::GetNullValueByType(column.GetType()));
      }
      current_left_.reset();
    } else {
      for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
        values.push_back(right->GetValue(&right_schema, i));
      }
    }
    tuple_batch->emplace_back(values, &GetOutputSchema());
    rid_batch->emplace_back();
  }
  return !tuple_batch->empty();
}

}  // namespace bustub
