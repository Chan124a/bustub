//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// execution_common.cpp
//
// Identification: src/execution/execution_common.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/execution_common.h"

#include "catalog/catalog.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"

namespace bustub {

TupleComparator::TupleComparator(std::vector<OrderBy> order_bys) : order_bys_(std::move(order_bys)) {}

auto TupleComparator::operator()(const SortEntry &entry_a, const SortEntry &entry_b) const -> bool {
  for (size_t i = 0; i < order_bys_.size(); i++) {
    const auto &[direction, null_order, expr] = order_bys_[i];
    const auto &a = entry_a.first[i];
    const auto &b = entry_b.first[i];
    if (a.IsNull() || b.IsNull()) {
      if (a.IsNull() && b.IsNull()) {
        continue;
      }
      bool nulls_first = null_order == OrderByNullType::NULLS_FIRST ||
                         (null_order == OrderByNullType::DEFAULT && direction != OrderByType::DESC);
      return a.IsNull() == nulls_first;
    }
    if (a.CompareLessThan(b) == CmpBool::CmpTrue) {
      return direction != OrderByType::DESC;
    }
    if (a.CompareGreaterThan(b) == CmpBool::CmpTrue) {
      return direction == OrderByType::DESC;
    }
  }
  return false;
}

/**
 * Generate sort key for a tuple based on the order by expressions.
 *
 * TODO(P3): Implement this method.
 */
auto GenerateSortKey(const Tuple &tuple, const std::vector<OrderBy> &order_bys, const Schema &schema) -> SortKey {
  SortKey key;
  key.reserve(order_bys.size());
  for (const auto &[direction, null_order, expr] : order_bys) {
    key.push_back(expr->Evaluate(&tuple, schema));
  }
  return key;
}

/**
 * Above are all you need for P3.
 * You can ignore the remaining part of this file until P4.
 */

/**
 * @brief Reconstruct a tuple by applying the provided undo logs from the base tuple. All logs in the undo_logs are
 * applied regardless of the timestamp
 *
 * @param schema The schema of the base tuple and the returned tuple.
 * @param base_tuple The base tuple to start the reconstruction from.
 * @param base_meta The metadata of the base tuple.
 * @param undo_logs The list of undo logs to apply during the reconstruction, the front is applied first.
 * @return An optional tuple that represents the reconstructed tuple. If the tuple is deleted as the result, returns
 * std::nullopt.
 */
auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  std::vector<Value> values;
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    values.push_back(base_tuple.GetValue(schema, i));
  }
  bool deleted = base_meta.is_deleted_;
  for (const auto &log : undo_logs) {
    std::vector<Column> columns;
    for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
      if (log.modified_fields_[i]) {
        columns.push_back(schema->GetColumn(i));
      }
    }
    Schema partial_schema(columns);
    uint32_t partial_idx = 0;
    for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
      if (log.modified_fields_[i]) {
        values[i] = log.tuple_.GetValue(&partial_schema, partial_idx++);
      }
    }
    deleted = log.is_deleted_;
  }
  if (deleted) {
    return std::nullopt;
  }
  Tuple tuple(values, schema);
  tuple.SetRid(base_tuple.GetRid());
  return tuple;
}

/**
 * @brief Collects the undo logs sufficient to reconstruct the tuple w.r.t. the txn.
 *
 * @param rid The RID of the tuple.
 * @param base_meta The metadata of the base tuple.
 * @param base_tuple The base tuple.
 * @param undo_link The undo link to the latest undo log.
 * @param txn The transaction.
 * @param txn_mgr The transaction manager.
 * @return An optional vector of undo logs to pass to ReconstructTuple(). std::nullopt if the tuple did not exist at the
 * time.
 */
auto CollectUndoLogs(RID rid, const TupleMeta &base_meta, const Tuple &base_tuple, std::optional<UndoLink> undo_link,
                     Transaction *txn, TransactionManager *txn_mgr) -> std::optional<std::vector<UndoLog>> {
  std::vector<UndoLog> logs;
  if (base_meta.ts_ <= txn->GetReadTs() || base_meta.ts_ == txn->GetTransactionTempTs()) {
    return logs;
  }
  while (undo_link.has_value() && undo_link->IsValid()) {
    auto log = txn_mgr->GetUndoLogOptional(*undo_link);
    BUSTUB_ENSURE(log.has_value(), "required undo log has been reclaimed");
    logs.push_back(*log);
    if (log->ts_ <= txn->GetReadTs()) {
      return logs;
    }
    undo_link = log->prev_version_;
  }
  return std::nullopt;
}

/**
 * @brief Generates a new undo log as the transaction tries to modify this tuple at the first time.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param ts The timestamp of the base tuple.
 * @param prev_version The undo link to the latest undo log of this tuple.
 * @return The generated undo log.
 */
auto GenerateNewUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple, timestamp_t ts,
                        UndoLink prev_version) -> UndoLog {
  std::vector<bool> fields(schema->GetColumnCount(), false);
  std::vector<Column> columns;
  std::vector<Value> values;
  if (base_tuple != nullptr) {
    for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
      auto old_value = base_tuple->GetValue(schema, i);
      if (target_tuple == nullptr || !old_value.CompareExactlyEquals(target_tuple->GetValue(schema, i))) {
        fields[i] = true;
        columns.push_back(schema->GetColumn(i));
        values.push_back(old_value);
      }
    }
  }
  Schema partial_schema(columns);
  return {base_tuple == nullptr, fields, Tuple(values, &partial_schema), ts, prev_version};
}

/**
 * @brief Generate the updated undo log to replace the old one, whereas the tuple is already modified by this txn once.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param log The original undo log.
 * @return The updated undo log.
 */
auto GenerateUpdatedUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple,
                            const UndoLog &log) -> UndoLog {
  // A tombstone before the first write needs no field values, regardless of subsequent writes.
  if (log.is_deleted_) {
    return log;
  }
  auto fields = log.modified_fields_;
  std::vector<Column> old_columns;
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    if (fields[i]) {
      old_columns.push_back(schema->GetColumn(i));
    }
  }
  Schema old_schema(old_columns);
  std::vector<Column> columns;
  std::vector<Value> values;
  uint32_t partial_idx = 0;
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    if (fields[i]) {
      columns.push_back(schema->GetColumn(i));
      values.push_back(log.tuple_.GetValue(&old_schema, partial_idx++));
    } else {
      BUSTUB_ENSURE(base_tuple != nullptr, "undo of deletion must contain all original fields");
      auto old_value = base_tuple->GetValue(schema, i);
      if (target_tuple == nullptr || !old_value.CompareExactlyEquals(target_tuple->GetValue(schema, i))) {
        fields[i] = true;
        columns.push_back(schema->GetColumn(i));
        values.push_back(old_value);
      }
    }
  }
  Schema partial_schema(columns);
  return {log.is_deleted_, fields, Tuple(values, &partial_schema), log.ts_, log.prev_version_};
}

void WriteTuple(TransactionManager *txn_mgr, Transaction *txn, const TableInfo *table_info, RID rid,
                const Tuple *target_tuple, bool require_deleted) {
  auto *heap = table_info->table_.get();
  auto guard = heap->AcquireTablePageWriteLock(rid);
  auto *page = guard.AsMut<TablePage>();
  auto [meta, base_tuple] = heap->GetTupleWithLockAcquired(rid, page);
  const bool self_write = meta.ts_ == txn->GetTransactionTempTs();
  if (!self_write && meta.ts_ > txn->GetReadTs()) {
    txn->SetTainted();
    throw ExecutionException("write-write conflict");
  }
  if (require_deleted && !meta.is_deleted_) {
    txn->SetTainted();
    throw ExecutionException("duplicate primary key");
  }
  auto link = txn_mgr->GetUndoLink(rid);
  const auto *before = meta.is_deleted_ ? nullptr : &base_tuple;
  if (self_write) {
    if (link.has_value() && link->IsValid() && link->prev_txn_ == txn->GetTransactionId()) {
      auto log = txn->GetUndoLog(link->prev_log_idx_);
      txn->ModifyUndoLog(link->prev_log_idx_, GenerateUpdatedUndoLog(&table_info->schema_, before, target_tuple, log));
    }
  } else {
    link = txn->AppendUndoLog(
        GenerateNewUndoLog(&table_info->schema_, before, target_tuple, meta.ts_, link.value_or(UndoLink{})));
  }
  heap->UpdateTupleInPlaceWithLockAcquired({txn->GetTransactionTempTs(), target_tuple == nullptr},
                                           target_tuple == nullptr ? base_tuple : *target_tuple, rid, page);
  txn_mgr->UpdateUndoLink(rid, link);
  txn->AppendWriteSet(table_info->oid_, rid);
}

auto InsertIndexedTuple(TransactionManager *txn_mgr, Transaction *txn, const TableInfo *table_info,
                        const std::vector<std::shared_ptr<IndexInfo>> &indexes, const Tuple &tuple) -> RID {
  for (const auto &index : indexes) {
    if (index->is_primary_key_) {
      std::vector<RID> matches;
      auto key = tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->ScanKey(key, &matches, txn);
      if (!matches.empty()) {
        WriteTuple(txn_mgr, txn, table_info, matches.front(), &tuple, true);
        return matches.front();
      }
    }
  }
  auto rid = table_info->table_->InsertTuple({txn->GetTransactionTempTs(), false}, tuple);
  if (!rid.has_value()) {
    txn->SetTainted();
    throw ExecutionException("table insertion failed");
  }
  txn->AppendWriteSet(table_info->oid_, *rid);
  for (const auto &index : indexes) {
    auto key = tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
    if (!index->index_->InsertEntry(key, *rid, txn) && index->is_primary_key_) {
      txn->SetTainted();
      throw ExecutionException("concurrent primary key insertion");
    }
  }
  return *rid;
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  fmt::println(stderr, "debug_hook: {}", info);
  const auto &schema = table_info->schema_;
  for (auto iter = table_heap->MakeIterator(); !iter.IsEnd(); ++iter) {
    auto rid = iter.GetRID();
    auto [meta, tuple, link] = GetTupleAndUndoLink(txn_mgr, table_heap, rid);
    auto ts = meta.ts_ >= TXN_START_ID ? fmt::format("txn{}", meta.ts_ - TXN_START_ID) : fmt::format("{}", meta.ts_);
    fmt::println(stderr, "RID={}/{} ts={} {}tuple={}", rid.GetPageId(), rid.GetSlotNum(), ts,
                 meta.is_deleted_ ? "<del marker> " : "", tuple.ToString(&schema));
    while (link.has_value() && link->IsValid()) {
      auto log = txn_mgr->GetUndoLogOptional(*link);
      if (!log.has_value()) {
        fmt::println(stderr, "  <reclaimed>");
        break;
      }
      std::vector<Column> columns;
      for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
        if (log->modified_fields_[i]) {
          columns.push_back(schema.GetColumn(i));
        }
      }
      Schema partial_schema(columns);
      fmt::println(stderr, "  txn{}@{} ts={} {}tuple={}", link->prev_txn_ - TXN_START_ID, link->prev_log_idx_, log->ts_,
                   log->is_deleted_ ? "<del> " : "", log->tuple_.ToString(&partial_schema));
      link = log->prev_version_;
    }
  }
}

}  // namespace bustub
