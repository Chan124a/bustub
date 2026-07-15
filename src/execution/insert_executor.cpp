//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstddef>
#include <memory>

#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/executors/insert_executor.h"
#include "type/value_factory.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  is_inserted_ = false;
  child_executor_->Init();

  bool has_IX = exec_ctx_->GetTransaction()->IsTableIntentionExclusiveLocked(plan_->TableOid());
  bool has_X = exec_ctx_->GetTransaction()->IsTableExclusiveLocked(plan_->TableOid());
  bool has_SIX = exec_ctx_->GetTransaction()->IsTableSharedIntentionExclusiveLocked(plan_->TableOid());
  bool has_S = exec_ctx_->GetTransaction()->IsTableSharedLocked(plan_->TableOid());
  if (!has_IX && !has_X && !has_SIX) {
    if (has_S) {
      CheckLockOperation(
          [&] {
            return exec_ctx_->GetLockManager()->LockTable(
                exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE, plan_->TableOid());
          },
          "lock table failed.");
    } else {
      CheckLockOperation(
          [&] {
            return exec_ctx_->GetLockManager()->LockTable(
                exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->TableOid());
          },
          "lock table failed.");
    }
  }

  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto InsertExecutor::Next(Tuple *tuple, [[maybe_unused]] RID *rid) -> bool {
  if (is_inserted_) {
    return false;
  }
  is_inserted_ = true;

  int insert_count = 0;
  while (true) {
    Tuple child_tuple{};
    RID child_rid{};

    // Get the next tuple
    const auto status = child_executor_->Next(&child_tuple, &child_rid);

    if (!status) {
      *tuple = Tuple{std::vector<Value>{ValueFactory::GetIntegerValue(insert_count)}, &GetOutputSchema()};
      return true;
    }

    auto cur_rid =
        table_info_->table_->InsertTuple(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false}, child_tuple,
                                         exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(), plan_->TableOid());
    if (cur_rid == std::nullopt) {
      throw ExecutionException("insert failed");
    }
    TableWriteRecord table_write_record(plan_->TableOid(), *cur_rid, table_info_->table_.get());
    table_write_record.wtype_ = WType::INSERT;
    exec_ctx_->GetTransaction()->AppendTableWriteRecord(table_write_record);
    for (size_t i = 0; i < indexes_.size(); ++i) {
      auto key =
          child_tuple.KeyFromTuple(table_info_->schema_, indexes_[i]->key_schema_, indexes_[i]->index_->GetKeyAttrs());
      bool res = indexes_[i]->index_->InsertEntry(key, *cur_rid, exec_ctx_->GetTransaction());
      if (!res) {
        throw ExecutionException("failed to insert key to index");
      }
      IndexWriteRecord index_write_record(*cur_rid, plan_->TableOid(), WType::INSERT, key, indexes_[i]->index_oid_,
                                          exec_ctx_->GetCatalog());
      exec_ctx_->GetTransaction()->AppendIndexWriteRecord(index_write_record);
    }
    insert_count++;
  }
}

}  // namespace bustub
