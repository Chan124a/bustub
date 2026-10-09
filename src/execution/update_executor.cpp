//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "type/value_factory.h"

#include "execution/executors/update_executor.h"

namespace bustub {

/**
 * Construct a new UpdateExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The update plan to be executed
 * @param child_executor The child executor that feeds the update
 */
UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/** Initialize the update */
void UpdateExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid()).get();
  child_executor_->Init();
  done_ = false;
}

/**
 * Yield the number of rows updated in the table.
 * @param[out] tuple_batch The tuple batch with one integer indicating the number of rows updated in the table
 * @param[out] rid_batch The next tuple RID batch produced by the update (ignore, not used)
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: UpdateExecutor::Next() does not use the `rid_batch` out-parameter.
 * NOTE: UpdateExecutor::Next() returns true with the number of updated rows produced only once.
 */
auto UpdateExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                          size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();
  if (done_ || batch_size == 0) {
    return false;
  }
  done_ = true;
  int32_t count = 0;
  std::vector<Tuple> tuples;
  std::vector<RID> rids;
  const auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  while (child_executor_->Next(&tuples, &rids, BUSTUB_BATCH_SIZE)) {
    for (size_t i = 0; i < tuples.size(); i++) {
      auto meta = table_info_->table_->GetTupleMeta(rids[i]);
      if (meta.is_deleted_) {
        continue;
      }
      std::vector<Value> values;
      for (const auto &expr : plan_->target_expressions_) {
        values.push_back(expr->Evaluate(&tuples[i], child_executor_->GetOutputSchema()));
      }
      Tuple updated(values, &table_info_->schema_);
      meta.is_deleted_ = true;
      table_info_->table_->UpdateTupleMeta(meta, rids[i]);
      for (const auto &index : indexes) {
        index->index_->DeleteEntry(
            tuples[i].KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()), rids[i],
            exec_ctx_->GetTransaction());
      }
      auto new_rid = table_info_->table_->InsertTuple({0, false}, updated);
      if (!new_rid.has_value()) {
        continue;
      }
      for (const auto &index : indexes) {
        index->index_->InsertEntry(
            updated.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()), *new_rid,
            exec_ctx_->GetTransaction());
      }
      count++;
    }
  }
  tuple_batch->emplace_back(std::vector<Value>{ValueFactory::GetIntegerValue(count)}, &GetOutputSchema());
  return true;
}

}  // namespace bustub
