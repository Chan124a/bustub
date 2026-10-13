//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "type/value_factory.h"

#include "execution/executors/insert_executor.h"

namespace bustub {

/**
 * Construct a new InsertExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled
 */
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/** Initialize the insert */
void InsertExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid()).get();
  child_executor_->Init();
  done_ = false;
}

/**
 * Yield the number of rows inserted into the table.
 * @param[out] tuple_batch The tuple batch with one integer indicating the number of rows inserted into the table
 * @param[out] rid_batch The next tuple RID batch produced by the insert (ignore, not used)
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: InsertExecutor::Next() does not use the `rid_batch` out-parameter.
 * NOTE: InsertExecutor::Next() returns true with the number of inserted rows produced only once.
 */
auto InsertExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
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
    for (const auto &tuple : tuples) {
      auto *txn = exec_ctx_->GetTransaction();
      for (const auto &index : indexes) {
        if (index->is_primary_key_) {
          std::vector<RID> matches;
          index->index_->ScanKey(
              tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()), &matches,
              txn);
          if (!matches.empty()) {
            txn->SetTainted();
            throw ExecutionException("duplicate primary key");
          }
        }
      }
      auto rid = table_info_->table_->InsertTuple({txn->GetTransactionTempTs(), false}, tuple);
      if (!rid.has_value()) {
        txn->SetTainted();
        throw ExecutionException("table insertion failed");
      }
      txn->AppendWriteSet(table_info_->oid_, *rid);
      for (const auto &index : indexes) {
        auto inserted = index->index_->InsertEntry(
            tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()), *rid,
            exec_ctx_->GetTransaction());
        if (!inserted && index->is_primary_key_) {
          txn->SetTainted();
          throw ExecutionException("concurrent primary key insertion");
        }
      }
      count++;
    }
  }
  tuple_batch->emplace_back(std::vector<Value>{ValueFactory::GetIntegerValue(count)}, &GetOutputSchema());
  return true;
}

}  // namespace bustub
