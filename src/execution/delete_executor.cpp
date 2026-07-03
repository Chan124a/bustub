//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "common/rid.h"
#include "execution/executors/delete_executor.h"
#include "type/value_factory.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  is_deleted_ = false;
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto DeleteExecutor::Next(Tuple *tuple, [[maybe_unused]] RID *rid) -> bool {
  if (is_deleted_) {
    return false;
  }
  is_deleted_ = true;

  int delete_count = 0;
  while (true) {
    Tuple child_tuple{};
    RID child_rid{};

    // Get the next tuple
    const auto status = child_executor_->Next(&child_tuple, &child_rid);

    if (!status) {
      *tuple = Tuple{std::vector<Value>{ValueFactory::GetIntegerValue(delete_count)}, &GetOutputSchema()};
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
    delete_count++;
  }
}

}  // namespace bustub
