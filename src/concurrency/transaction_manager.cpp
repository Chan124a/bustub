//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "storage/table/table_heap.h"
namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
}

void TransactionManager::Abort(Transaction *txn) {
  /* TODO: revert all the changes in write set */
  auto table_write_set = txn->GetWriteSet();
  while (!table_write_set->empty()) {
    TableWriteRecord &table_write_record = table_write_set->back();
    auto cur_tuple_meta = table_write_record.table_heap_->GetTupleMeta(table_write_record.rid_);
    if (table_write_record.wtype_ == WType::INSERT) {
      cur_tuple_meta.is_deleted_ = true;
    } else if (table_write_record.wtype_ == WType::DELETE) {
      cur_tuple_meta.is_deleted_ = false;
    }
    table_write_record.table_heap_->UpdateTupleMeta(cur_tuple_meta, table_write_record.rid_);
    table_write_set->pop_back();
  }
  auto index_write_set = txn->GetIndexWriteSet();
  while (!index_write_set->empty()) {
    IndexWriteRecord &index_write_record = index_write_set->back();
    auto index_info = index_write_record.catalog_->GetIndex(index_write_record.index_oid_);
    if (index_write_record.wtype_ == WType::INSERT) {
      index_info->index_->DeleteEntry(index_write_record.tuple_, index_write_record.rid_, txn);
    } else if (index_write_record.wtype_ == WType::DELETE) {
      index_info->index_->InsertEntry(index_write_record.tuple_, index_write_record.rid_, txn);
    }
    index_write_set->pop_back();
  }
  ReleaseLocks(txn);

  txn->SetState(TransactionState::ABORTED);
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
