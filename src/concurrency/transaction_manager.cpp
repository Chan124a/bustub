//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"
#include <algorithm>
#include <utility>

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Begins a new transaction.
 * @param isolation_level an optional isolation level of the transaction.
 * @return an initialized transaction
 */
auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  txn_ref->read_ts_ = last_commit_ts_.load();

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

/** Validate committed changes after the reader's snapshot against its scan predicates. */
auto TransactionManager::VerifyTxn(Transaction *txn) -> bool {
  if (txn->GetWriteSets().empty()) {
    return true;
  }
  std::unordered_map<table_oid_t, std::unordered_set<RID>> changed;
  {
    std::shared_lock<std::shared_mutex> lock(txn_map_mutex_);
    for (const auto &[id, other] : txn_map_) {
      if (other->GetTransactionState() == TransactionState::COMMITTED && other->GetCommitTs() > txn->GetReadTs()) {
        for (const auto &[table_oid, rids] : other->GetWriteSets()) {
          changed[table_oid].insert(rids.begin(), rids.end());
        }
      }
    }
  }
  for (const auto &[table_oid, rids] : changed) {
    const auto &predicates = txn->GetScanPredicates();
    auto read = predicates.find(table_oid);
    if (read == predicates.end()) {
      continue;
    }
    auto table = catalog_->GetTable(table_oid);
    const auto matches = [&read, &table](const std::optional<Tuple> &image) {
      if (!image.has_value()) {
        return false;
      }
      return std::any_of(read->second.begin(), read->second.end(),
                         [&image, &table](const AbstractExpressionRef &predicate) {
                           if (predicate == nullptr) {
                             return true;
                           }
                           auto value = predicate->Evaluate(&*image, table->schema_);
                           return !value.IsNull() && value.GetAs<bool>();
                         });
    };
    for (const auto &rid : rids) {
      auto [meta, tuple, link] = GetTupleAndUndoLink(this, table->table_.get(), rid);
      auto ts = meta.ts_;
      auto image = meta.is_deleted_ ? std::nullopt : std::make_optional(tuple);
      while (ts > txn->GetReadTs()) {
        std::optional<UndoLog> log;
        if (link.has_value() && link->IsValid()) {
          log = GetUndoLogOptional(*link);
          BUSTUB_ENSURE(log.has_value(), "validation requires a reclaimed undo log");
        }
        auto before =
            log.has_value() ? ReconstructTuple(&table->schema_, tuple, {ts, !image.has_value()}, {*log}) : std::nullopt;
        // An uncommitted head is skipped; only committed before/after images participate in validation.
        if (ts < TXN_START_ID && (matches(image) || matches(before))) {
          return false;
        }
        if (!log.has_value()) {
          break;
        }
        ts = log->ts_;
        link = log->prev_version_;
        tuple = before.value_or(tuple);
        image = std::move(before);
      }
    }
  }
  return true;
}

/**
 * Commits a transaction.
 * @param txn the transaction to commit, the txn will be managed by the txn manager so no need to delete it by
 * yourself
 */
auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  const auto commit_ts = last_commit_ts_.load() + 1;

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  for (const auto &[table_oid, rids] : txn->GetWriteSets()) {
    auto table = catalog_->GetTable(table_oid);
    for (const auto &rid : rids) {
      auto guard = table->table_->AcquireTablePageWriteLock(rid);
      auto *page = guard.AsMut<TablePage>();
      auto meta = table->table_->GetTupleMetaWithLockAcquired(rid, page);
      BUSTUB_ENSURE(meta.ts_ == txn->GetTransactionTempTs(), "committing a tuple owned by another transaction");
      meta.ts_ = commit_ts;
      page->UpdateTupleMeta(meta, rid);
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  txn->commit_ts_ = commit_ts;
  last_commit_ts_ = commit_ts;

  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

/**
 * Aborts a transaction
 * @param txn the transaction to abort, the txn will be managed by the txn manager so no need to delete it by yourself
 */
void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  for (const auto &[table_oid, rids] : txn->GetWriteSets()) {
    auto table = catalog_->GetTable(table_oid);
    auto *heap = table->table_.get();
    for (const auto &rid : rids) {
      auto guard = heap->AcquireTablePageWriteLock(rid);
      auto *page = guard.AsMut<TablePage>();
      auto [meta, tuple] = heap->GetTupleWithLockAcquired(rid, page);
      BUSTUB_ENSURE(meta.ts_ == txn->GetTransactionTempTs(), "aborting a tuple owned by another transaction");
      auto link = GetUndoLink(rid);
      if (link.has_value() && link->IsValid() && link->prev_txn_ == txn->GetTransactionId()) {
        auto log = txn->GetUndoLog(link->prev_log_idx_);
        auto restored = ReconstructTuple(&table->schema_, tuple, meta, {log});
        heap->UpdateTupleInPlaceWithLockAcquired({log.ts_, log.is_deleted_}, restored.value_or(tuple), rid, page);
        UpdateUndoLink(rid, log.prev_version_.IsValid() ? std::make_optional(log.prev_version_) : std::nullopt);
      } else {
        heap->UpdateTupleInPlaceWithLockAcquired({0, true}, tuple, rid, page);
        UpdateUndoLink(rid, std::nullopt);
      }
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

/** @brief Stop-the-world garbage collection. Will be called only when all transactions are not accessing the table
 * heap. */
void TransactionManager::GarbageCollection() {
  const auto watermark = GetWatermark();
  std::optional<timestamp_t> serializable_read_ts;
  {
    std::shared_lock<std::shared_mutex> lock(txn_map_mutex_);
    for (const auto &[id, txn] : txn_map_) {
      auto state = txn->GetTransactionState();
      if ((state == TransactionState::RUNNING || state == TransactionState::TAINTED) &&
          txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
        if (!serializable_read_ts.has_value() || txn->GetReadTs() < *serializable_read_ts) {
          serializable_read_ts = txn->GetReadTs();
        }
      }
    }
  }
  std::unordered_set<txn_id_t> needed;
  for (const auto &name : catalog_->GetTableNames()) {
    auto table = catalog_->GetTable(name);
    for (auto iter = table->table_->MakeIterator(); !iter.IsEnd(); ++iter) {
      auto [meta, tuple, link] = GetTupleAndUndoLink(this, table->table_.get(), iter.GetRID());
      auto ts = meta.ts_;
      while (ts > watermark && link.has_value() && link->IsValid()) {
        auto log = GetUndoLogOptional(*link);
        BUSTUB_ENSURE(log.has_value(), "required undo log has been reclaimed");
        needed.insert(link->prev_txn_);
        ts = log->ts_;
        link = log->prev_version_;
      }
    }
  }
  std::unique_lock<std::shared_mutex> lock(txn_map_mutex_);
  for (auto iter = txn_map_.begin(); iter != txn_map_.end();) {
    auto state = iter->second->GetTransactionState();
    const bool needed_for_validation = serializable_read_ts.has_value() && state == TransactionState::COMMITTED &&
                                       iter->second->GetCommitTs() > *serializable_read_ts &&
                                       !iter->second->GetWriteSets().empty();
    if ((state == TransactionState::COMMITTED || state == TransactionState::ABORTED) &&
        needed.count(iter->first) == 0 && !needed_for_validation) {
      iter = txn_map_.erase(iter);
    } else {
      ++iter;
    }
  }
}

}  // namespace bustub
