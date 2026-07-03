//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include "storage/table/tuple.h"

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  is_update_ = false;
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto UpdateExecutor::Next(Tuple *tuple, [[maybe_unused]] RID *rid) -> bool {
  if (is_update_) {
    return false;
  }
  is_update_ = true;

  int update_count = 0;
  while (true) {
    Tuple child_tuple{};
    RID child_rid{};

    const auto status = child_executor_->Next(&child_tuple, &child_rid);

    if (!status) {
      *tuple = Tuple{std::vector<Value>{ValueFactory::GetIntegerValue(update_count)}, &GetOutputSchema()};
      return true;
    }

    auto cur_tuple_meta = table_info_->table_->GetTupleMeta(child_rid);
    cur_tuple_meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(cur_tuple_meta, child_rid);
    for (size_t i = 0; i < indexes_.size(); ++i) {
      indexes_[i]->index_->DeleteEntry(
          child_tuple.KeyFromTuple(table_info_->schema_, indexes_[i]->key_schema_, indexes_[i]->index_->GetKeyAttrs()),
          child_rid, exec_ctx_->GetTransaction());
    }

    std::vector<Value> values{};
    values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
    const auto &row_expr = plan_->target_expressions_;
    for (const auto &col : row_expr) {
      values.push_back(col->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }

    Tuple new_tuple{values, &table_info_->schema_};
    auto new_rid =
        table_info_->table_->InsertTuple(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false}, new_tuple,
                                         exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(), plan_->TableOid());
    if (new_rid == std::nullopt) {
      throw ExecutionException("insert failed");
    }
    for (size_t i = 0; i < indexes_.size(); ++i) {
      bool res = indexes_[i]->index_->InsertEntry(
          new_tuple.KeyFromTuple(table_info_->schema_, indexes_[i]->key_schema_, indexes_[i]->index_->GetKeyAttrs()),
          *new_rid, exec_ctx_->GetTransaction());
      if (!res) {
        throw ExecutionException("failed to insert key to index");
      }
    }
    update_count++;
  }
}

}  // namespace bustub
