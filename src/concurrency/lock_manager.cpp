//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <algorithm>
#include <cstddef>
#include <memory>
#include <mutex>
#include <unordered_set>

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (!CanTxnTakeLock(txn, lock_mode)) {
    return false;
  }

  LockMode cur_lock_mode = LockMode::EXCLUSIVE;
  if (IsTableLocked(txn, oid, cur_lock_mode)) {
    return UpgradeLockTable(txn, cur_lock_mode, lock_mode, oid);
  }

  LockRequest *new_lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
  std::unique_lock<std::mutex> table_lock_map_guard(table_lock_map_latch_);
  if (!table_lock_map_.count(oid)) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  auto table_lock_queue = table_lock_map_[oid];
  table_lock_map_guard.unlock();
  std::unique_lock<std::mutex> lock(table_lock_queue->latch_);
  auto new_lock_request_iter =
      table_lock_queue->request_queue_.insert(table_lock_queue->request_queue_.end(), new_lock_request);
  GrantNewLocksIfPossible(table_lock_queue.get());

  table_lock_queue->cv_.wait(
      lock, [&] { return new_lock_request->granted_ || txn->GetState() == TransactionState::ABORTED; });
  if (txn->GetState() == TransactionState::ABORTED) {
    // 无论请求是否刚刚被 granted，都从队列中删除
    // 重新尝试授予后续请求
    delete (*new_lock_request_iter);
    table_lock_queue->request_queue_.erase(new_lock_request_iter);
    GrantNewLocksIfPossible(table_lock_queue.get());
    return false;
  }

  UpgradeTableLockSet(txn, LockMode::INTENTION_SHARED, false, lock_mode, oid);
  return true;
}

auto LockManager::UpgradeLockTable(Transaction *txn, LockMode old_lock_mode, LockMode new_lock_mode,
                                   const table_oid_t &oid) -> bool {
  if (old_lock_mode == new_lock_mode) {
    return true;
  }
  std::unique_lock<std::mutex> table_lock_map_guard(table_lock_map_latch_);
  auto it = table_lock_map_.find(oid);
  if (it == table_lock_map_.end()) {
    TransactionAbort(txn, AbortReason::TABLE_LOCK_NOT_PRESENT, false, nullptr);
  }
  auto table_lock_queue = it->second;
  table_lock_map_guard.unlock();

  std::unique_lock<std::mutex> lock(table_lock_queue->latch_);
  if (table_lock_queue->upgrading_ != INVALID_TXN_ID && table_lock_queue->upgrading_ != txn->GetTransactionId()) {
    TransactionAbort(txn, AbortReason::UPGRADE_CONFLICT, false, nullptr);
  }

  table_lock_queue->upgrading_ = txn->GetTransactionId();
  LockRequest *expect_upgrade_lock_request = nullptr;
  bool can_upgrade = true;
  for (auto &lock_request : table_lock_queue->request_queue_) {
    if (!lock_request->granted_) {
      break;
    }
    if (lock_request->txn_id_ == txn->GetTransactionId()) {
      if (!CanLockUpgrade(lock_request->lock_mode_, new_lock_mode)) {
        can_upgrade = false;
      }
      expect_upgrade_lock_request = lock_request;
      continue;
    }
  }
  if (expect_upgrade_lock_request == nullptr) {
    table_lock_queue->upgrading_ = INVALID_TXN_ID;
    TransactionAbort(txn, AbortReason::TABLE_LOCK_NOT_PRESENT, false, nullptr);
  }
  if (!can_upgrade) {
    table_lock_queue->upgrading_ = INVALID_TXN_ID;
    TransactionAbort(txn, AbortReason::INCOMPATIBLE_UPGRADE, false, nullptr);
  }
  LockRequest *upgrade_lock_request = new LockRequest(txn->GetTransactionId(), new_lock_mode, oid);
  auto upgrade_lock_request_iter =
      table_lock_queue->request_queue_.insert(table_lock_queue->request_queue_.end(), upgrade_lock_request);
  GrantNewLocksIfPossible(table_lock_queue.get());

  table_lock_queue->cv_.wait(
      lock, [&] { return upgrade_lock_request->granted_ || txn->GetState() == TransactionState::ABORTED; });
  if (txn->GetState() == TransactionState::ABORTED) {
    // 无论请求是否刚刚被 granted，都从队列中删除
    // 重新尝试授予后续请求
    delete (*upgrade_lock_request_iter);
    table_lock_queue->request_queue_.erase(upgrade_lock_request_iter);
    table_lock_queue->upgrading_ = INVALID_TXN_ID;
    GrantNewLocksIfPossible(table_lock_queue.get());
    return false;
  }
  expect_upgrade_lock_request->lock_mode_ = new_lock_mode;
  UpgradeTableLockSet(txn, old_lock_mode, true, new_lock_mode, oid);
  delete (*upgrade_lock_request_iter);
  table_lock_queue->request_queue_.erase(upgrade_lock_request_iter);
  table_lock_queue->upgrading_ = INVALID_TXN_ID;
  GrantNewLocksIfPossible(table_lock_queue.get());
  return true;
}

auto LockManager::UpgradeLockRow(Transaction *txn, LockMode old_lock_mode, LockMode new_lock_mode,
                                 const table_oid_t &oid, const RID &rid) -> bool {
  if (old_lock_mode == new_lock_mode) {
    return true;
  }
  std::unique_lock<std::mutex> row_lock_map_guard(row_lock_map_latch_);
  auto it = row_lock_map_.find(rid);
  if (it == row_lock_map_.end()) {
    TransactionAbort(txn, AbortReason::ROW_LOCK_NOT_PRESENT, false, nullptr);
  }
  auto row_lock_queue = it->second;
  row_lock_map_guard.unlock();

  std::unique_lock<std::mutex> lock(row_lock_queue->latch_);
  if (row_lock_queue->upgrading_ != INVALID_TXN_ID && row_lock_queue->upgrading_ != txn->GetTransactionId()) {
    TransactionAbort(txn, AbortReason::UPGRADE_CONFLICT, false, nullptr);
  }
  row_lock_queue->upgrading_ = txn->GetTransactionId();
  LockRequest *expect_upgrade_lock_request = nullptr;
  bool can_upgrade = true;
  for (auto &lock_request : row_lock_queue->request_queue_) {
    if (!lock_request->granted_) {
      break;
    }
    if (lock_request->txn_id_ == txn->GetTransactionId()) {
      if (!CanLockUpgrade(lock_request->lock_mode_, new_lock_mode)) {
        can_upgrade = false;
      }
      expect_upgrade_lock_request = lock_request;
      continue;
    }
  }
  if (expect_upgrade_lock_request == nullptr) {
    row_lock_queue->upgrading_ = INVALID_TXN_ID;
    TransactionAbort(txn, AbortReason::ROW_LOCK_NOT_PRESENT, false, nullptr);
  }
  if (!can_upgrade) {
    row_lock_queue->upgrading_ = INVALID_TXN_ID;
    TransactionAbort(txn, AbortReason::INCOMPATIBLE_UPGRADE, false, nullptr);
  }
  LockRequest *upgrade_lock_request = new LockRequest(txn->GetTransactionId(), new_lock_mode, oid, rid);
  auto upgrade_lock_request_iter =
      row_lock_queue->request_queue_.insert(row_lock_queue->request_queue_.end(), upgrade_lock_request);
  GrantNewLocksIfPossible(row_lock_queue.get());

  row_lock_queue->cv_.wait(
      lock, [&] { return upgrade_lock_request->granted_ || txn->GetState() == TransactionState::ABORTED; });
  if (txn->GetState() == TransactionState::ABORTED) {
    // 无论请求是否刚刚被 granted，都从队列中删除
    // 重新尝试授予后续请求
    delete (*upgrade_lock_request_iter);
    row_lock_queue->request_queue_.erase(upgrade_lock_request_iter);
    row_lock_queue->upgrading_ = INVALID_TXN_ID;
    GrantNewLocksIfPossible(row_lock_queue.get());
    return false;
  }
  expect_upgrade_lock_request->lock_mode_ = new_lock_mode;
  UpgradeRowLockSet(txn, old_lock_mode, true, new_lock_mode, oid, rid);
  delete (*upgrade_lock_request_iter);
  row_lock_queue->request_queue_.erase(upgrade_lock_request_iter);
  row_lock_queue->upgrading_ = INVALID_TXN_ID;
  GrantNewLocksIfPossible(row_lock_queue.get());
  return true;
}

auto LockManager::GetTableLockSet(Transaction *txn, LockMode lock_mode)
    -> std::shared_ptr<std::unordered_set<table_oid_t>> {
  switch (lock_mode) {
    case LockMode::SHARED:
      return txn->GetSharedTableLockSet();
    case LockMode::EXCLUSIVE:
      return txn->GetExclusiveTableLockSet();
    case LockMode::INTENTION_SHARED:
      return txn->GetIntentionSharedTableLockSet();
    case LockMode::INTENTION_EXCLUSIVE:
      return txn->GetIntentionExclusiveTableLockSet();
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return txn->GetSharedIntentionExclusiveTableLockSet();
  }
}

auto LockManager::GetRowLockSet(Transaction *txn, LockMode lock_mode)
    -> std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> {
  switch (lock_mode) {
    case LockMode::SHARED:
      return txn->GetSharedRowLockSet();
    case LockMode::EXCLUSIVE:
      return txn->GetExclusiveRowLockSet();
    default:
      TransactionAbort(txn, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW, false, nullptr);
  }
  return nullptr;
}

auto LockManager::UpgradeTableLockSet(Transaction *txn, LockMode old_lock_mode, bool delete_old, LockMode new_lock_mode,
                                      const table_oid_t &oid) -> void {
  auto txn_lock = txn->AcquireTxnUniqueLock();
  if (delete_old) {
    auto old_table_lock_set = GetTableLockSet(txn, old_lock_mode);
    old_table_lock_set->erase(oid);
  }

  std::shared_ptr<std::unordered_set<table_oid_t>> new_table_lock_set = GetTableLockSet(txn, new_lock_mode);
  new_table_lock_set->insert(oid);
}

auto LockManager::UpgradeRowLockSet(Transaction *txn, LockMode old_lock_mode, bool delete_old, LockMode new_lock_mode,
                                    const table_oid_t &oid, const RID &rid) -> void {
  auto txn_lock = txn->AcquireTxnUniqueLock();
  if (delete_old) {
    auto old_row_lock_set = GetRowLockSet(txn, old_lock_mode);
    auto old_row_lock_set_iter = old_row_lock_set->find(oid);
    if (old_row_lock_set_iter == old_row_lock_set->end()) {
      TransactionAbort(txn, AbortReason::TABLE_LOCK_NOT_PRESENT, false, nullptr);
    }
    (*old_row_lock_set)[oid].erase(rid);
  }
  auto new_row_lock_set = GetRowLockSet(txn, new_lock_mode);
  (*new_row_lock_set)[oid].insert(rid);
}

auto LockManager::IsTableLocked(Transaction *txn, const table_oid_t &oid, LockMode &lock_mode) -> bool {
  auto txn_lock = txn->AcquireTxnUniqueLock();
  if (txn->IsTableIntentionSharedLocked(oid)) {
    lock_mode = LockMode::INTENTION_SHARED;
    return true;
  }
  if (txn->IsTableSharedLocked(oid)) {
    lock_mode = LockMode::SHARED;
    return true;
  }
  if (txn->IsTableIntentionExclusiveLocked(oid)) {
    lock_mode = LockMode::INTENTION_EXCLUSIVE;
    return true;
  }
  if (txn->IsTableExclusiveLocked(oid)) {
    lock_mode = LockMode::EXCLUSIVE;
    return true;
  }
  if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
    lock_mode = LockMode::SHARED_INTENTION_EXCLUSIVE;
    return true;
  }
  return false;
}

auto LockManager::IsRowLocked(Transaction *txn, const table_oid_t &oid, const RID &rid, LockMode &lock_mode) -> bool {
  auto txn_lock = txn->AcquireTxnUniqueLock();
  if (txn->IsRowExclusiveLocked(oid, rid)) {
    lock_mode = LockMode::EXCLUSIVE;
    return true;
  }
  if (txn->IsRowSharedLocked(oid, rid)) {
    lock_mode = LockMode::SHARED;
    return true;
  }
  return false;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  LockMode cur_lock_mode = LockMode::EXCLUSIVE;
  if (!IsTableLocked(txn, oid, cur_lock_mode)) {
    TransactionAbort(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD, false, nullptr);
  }

  auto txn_lock = txn->AcquireTxnUniqueLock();
  auto shared_row_lock_set = txn->GetSharedRowLockSet();
  auto shared_row_lock_set_iter = shared_row_lock_set->find(oid);
  if (shared_row_lock_set_iter != shared_row_lock_set->end() && !shared_row_lock_set_iter->second.empty()) {
    TransactionAbort(txn, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS, false, nullptr);
  }
  auto exclusive_row_lock_set = txn->GetExclusiveRowLockSet();
  auto exclusive_row_lock_set_iter = exclusive_row_lock_set->find(oid);
  if (exclusive_row_lock_set_iter != exclusive_row_lock_set->end() && !exclusive_row_lock_set_iter->second.empty()) {
    TransactionAbort(txn, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS, false, nullptr);
  }
  txn_lock.unlock();

  std::unique_lock<std::mutex> table_lock_map_guard(table_lock_map_latch_);
  auto table_lock_queue_iter = table_lock_map_.find(oid);
  if (table_lock_queue_iter == table_lock_map_.end()) {
    TransactionAbort(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD, false, nullptr);
  }
  auto &lock_request_queue = table_lock_queue_iter->second;
  table_lock_map_guard.unlock();
  std::unique_lock<std::mutex> table_lock_queue_guard(lock_request_queue->latch_);
  if (lock_request_queue->request_queue_.empty()) {
    TransactionAbort(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD, false, nullptr);
  }
  LockRequest *expect_unlock_request = nullptr;
  for (auto lock_request_iter = lock_request_queue->request_queue_.begin();
       lock_request_iter != lock_request_queue->request_queue_.end(); ++lock_request_iter) {
    if (!(*lock_request_iter)->granted_) {
      break;
    }
    if ((*lock_request_iter)->txn_id_ == txn->GetTransactionId() && (*lock_request_iter)->oid_ == oid) {
      expect_unlock_request = (*lock_request_iter);
      lock_request_queue->request_queue_.erase(lock_request_iter);
      break;
    }
  }
  if (expect_unlock_request == nullptr) {
    TransactionAbort(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD, false, nullptr);
  }
  txn_lock.lock();
  auto table_lock_set = GetTableLockSet(txn, expect_unlock_request->lock_mode_);
  table_lock_set->erase(expect_unlock_request->oid_);
  UpdateTxnStateAfterUnlock(txn, expect_unlock_request->lock_mode_);
  txn_lock.unlock();
  delete expect_unlock_request;
  GrantNewLocksIfPossible(lock_request_queue.get());
  return true;
}

auto LockManager::UpdateTxnStateAfterUnlock(Transaction *txn, LockMode lock_mode) -> void {
  if (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE) {
    return;
  }
  if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
    return;
  }
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
      txn->SetState(TransactionState::SHRINKING);
      return;
    case IsolationLevel::READ_COMMITTED:
      if (lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
      return;
    case IsolationLevel::READ_UNCOMMITTED:
      if (lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
        return;
      } else {
        TransactionAbort(txn, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED, false, nullptr);
      }
  }
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE) {
    TransactionAbort(txn, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW, false, nullptr);
  }
  if (!CanTxnTakeLock(txn, lock_mode)) {
    return false;
  }
  if (!CheckAppropriateLockOnTable(txn, oid, lock_mode)) {
    TransactionAbort(txn, AbortReason::TABLE_LOCK_NOT_PRESENT, false, nullptr);
  }
  LockMode row_lock_mode = LockMode::INTENTION_EXCLUSIVE;
  if (IsRowLocked(txn, oid, rid, row_lock_mode)) {
    return UpgradeLockRow(txn, row_lock_mode, lock_mode, oid, rid);
  }

  LockRequest *new_lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid);
  std::unique_lock<std::mutex> row_lock_map_guard(row_lock_map_latch_);
  if (!row_lock_map_.count(rid)) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  auto row_lock_queue = row_lock_map_[rid];
  row_lock_map_guard.unlock();
  std::unique_lock<std::mutex> lock(row_lock_queue->latch_);
  auto new_lock_request_iter =
      row_lock_queue->request_queue_.insert(row_lock_queue->request_queue_.end(), new_lock_request);
  GrantNewLocksIfPossible(row_lock_queue.get());

  row_lock_queue->cv_.wait(lock,
                           [&] { return new_lock_request->granted_ || txn->GetState() == TransactionState::ABORTED; });
  if (txn->GetState() == TransactionState::ABORTED) {
    // 无论请求是否刚刚被 granted，都从队列中删除
    // 重新尝试授予后续请求
    delete (*new_lock_request_iter);
    row_lock_queue->request_queue_.erase(new_lock_request_iter);
    GrantNewLocksIfPossible(row_lock_queue.get());
    return false;
  }

  UpgradeRowLockSet(txn, LockMode::INTENTION_SHARED, false, lock_mode, oid, rid);
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  LockMode cur_lock_mode = LockMode::INTENTION_EXCLUSIVE;
  if (!IsRowLocked(txn, oid, rid, cur_lock_mode)) {
    TransactionAbort(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD, false, nullptr);
  }

  std::unique_lock<std::mutex> row_lock_map_guard(row_lock_map_latch_);
  auto row_lock_queue_iter = row_lock_map_.find(rid);
  if (row_lock_queue_iter == row_lock_map_.end()) {
    TransactionAbort(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD, false, nullptr);
  }
  auto &lock_request_queue = row_lock_queue_iter->second;
  row_lock_map_guard.unlock();
  std::unique_lock<std::mutex> row_lock_queue_guard(lock_request_queue->latch_);
  if (lock_request_queue->request_queue_.empty()) {
    TransactionAbort(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD, false, nullptr);
  }
  LockRequest *expect_unlock_request = nullptr;
  for (auto lock_request_iter = lock_request_queue->request_queue_.begin();
       lock_request_iter != lock_request_queue->request_queue_.end(); ++lock_request_iter) {
    if (!(*lock_request_iter)->granted_) {
      break;
    }
    if ((*lock_request_iter)->txn_id_ == txn->GetTransactionId() && (*lock_request_iter)->oid_ == oid) {
      expect_unlock_request = (*lock_request_iter);
      lock_request_queue->request_queue_.erase(lock_request_iter);
      break;
    }
  }
  if (expect_unlock_request == nullptr) {
    TransactionAbort(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD, false, nullptr);
  }

  auto txn_lock = txn->AcquireTxnUniqueLock();
  auto row_lock_set = GetRowLockSet(txn, expect_unlock_request->lock_mode_);
  auto row_lock_set_iter = row_lock_set->find(oid);
  if (row_lock_set_iter == row_lock_set->end()) {
    TransactionAbort(txn, AbortReason::ROW_LOCK_NOT_PRESENT, false, nullptr);
  }
  (*row_lock_set)[oid].erase(rid);
  if (!force) {
    UpdateTxnStateAfterUnlock(txn, expect_unlock_request->lock_mode_);
  }
  txn_lock.unlock();
  delete expect_unlock_request;
  GrantNewLocksIfPossible(lock_request_queue.get());
  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

auto LockManager::AreLocksCompatible(LockMode l1, LockMode l2) -> bool {
  /* 兼容性如下（✓ 可同时持有，× 冲突）：
   *  | 已持有 \ 请求 | IS | IX | S | SIX | X |
   *  | IS           | ✓  | ✓  | ✓ | ✓   | × |
   *  | IX           | ✓  | ✓  | × | ×   | × |
   *  | S            | ✓  | ×  | ✓ | ×   | × |
   *  | SIX          | ✓  | ×  | × | ×   | × |
   *  | X            | ×  | ×  | × | ×   | × |
   */
  switch (l1) {
    case LockMode::SHARED:
      return l2 == LockMode::INTENTION_SHARED || l2 == LockMode::SHARED;
    case LockMode::EXCLUSIVE:
      return false;
    case LockMode::INTENTION_SHARED:
      return l2 != LockMode::EXCLUSIVE;
    case LockMode::INTENTION_EXCLUSIVE:
      return l2 == LockMode::INTENTION_SHARED || l2 == LockMode::INTENTION_EXCLUSIVE;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return l2 == LockMode::INTENTION_SHARED;
  }
}

auto LockManager::CanTxnTakeLock(Transaction *txn, LockMode lock_mode) -> bool {
  /* Isolation Level
   * A transaction should hold X locks for all write operations until it commit or aborts, regardless of its isolation
   * level.
   * For REPEATABLE_READ, a transaction should take and hold S locks for all read operations until it commits or aborts.
   * For READ_COMMITTED, a transaction should take S locks for all read operations, but can release them immediately.
   * For READ_UNCOMMITTED, a transaction does not need to take any S locks for read operations.

   *  | 隔离级别          | 必须申请的锁 | GROWING 时允许 | SHRINKING 时允许|
   *  | REPEATABLE_READ  | 所有锁      | 所有锁         | 不允许任何锁      |
   *  | READ_COMMITTED   | 所有锁      | 所有锁         | 仅 IS、S         |
   *  | READ_UNCOMMITTED | 仅 IX、X    | IX、X         | 不允许           |
   */
  auto txn_lock = txn->AcquireTxnUniqueLock();
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
      switch (txn->GetState()) {
        case TransactionState::GROWING:
          return true;
        case TransactionState::SHRINKING:
          TransactionAbort(txn, AbortReason::LOCK_ON_SHRINKING, false, nullptr);
        case TransactionState::COMMITTED:
        case TransactionState::ABORTED:
          return false;
      }
    case IsolationLevel::READ_COMMITTED:
      switch (txn->GetState()) {
        case TransactionState::GROWING:
          return true;
        case TransactionState::SHRINKING:
          if (lock_mode != LockMode::INTENTION_SHARED && lock_mode != LockMode::SHARED) {
            TransactionAbort(txn, AbortReason::LOCK_ON_SHRINKING, false, nullptr);
          }
          return true;
        case TransactionState::COMMITTED:
        case TransactionState::ABORTED:
          return false;
      }
    case IsolationLevel::READ_UNCOMMITTED:
      switch (txn->GetState()) {
        case TransactionState::GROWING:
          if (lock_mode != LockMode::INTENTION_EXCLUSIVE && lock_mode != LockMode::EXCLUSIVE) {
            TransactionAbort(txn, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED, false, nullptr);
          }
          return true;
        case TransactionState::SHRINKING:
          TransactionAbort(txn, AbortReason::LOCK_ON_SHRINKING, false, nullptr);
        case TransactionState::COMMITTED:
        case TransactionState::ABORTED:
          return false;
      }
  }
}

auto LockManager::CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
  /* 合法升级:
   *  IS  → S / IX / SIX/ X
   *  S   → SIX / X
   *  IX  → SIX / X
   *  SIX → X
   */
  switch (curr_lock_mode) {
    case LockMode::INTENTION_SHARED:
      return requested_lock_mode != LockMode::INTENTION_SHARED;
    case LockMode::SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
      return requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE || requested_lock_mode == LockMode::EXCLUSIVE;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return requested_lock_mode == LockMode::EXCLUSIVE;
    default:
      return false;
  }
}

auto LockManager::CheckAppropriateLockOnTable(Transaction *txn, const table_oid_t &oid, LockMode row_lock_mode)
    -> bool {
  LockMode table_lock_mode = LockMode::EXCLUSIVE;
  if (!IsTableLocked(txn, oid, table_lock_mode)) {
    return false;
  }
  switch (row_lock_mode) {
    case LockMode::SHARED:
      return table_lock_mode == LockMode::INTENTION_SHARED || table_lock_mode == LockMode::INTENTION_EXCLUSIVE ||
             table_lock_mode == LockMode::SHARED || table_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE ||
             table_lock_mode == LockMode::EXCLUSIVE;

    case LockMode::EXCLUSIVE:
      return table_lock_mode == LockMode::INTENTION_EXCLUSIVE ||
             table_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE || table_lock_mode == LockMode::EXCLUSIVE;

    default:
      return false;
  }
}

void LockManager::GrantNewLocksIfPossible(LockRequestQueue *lock_request_queue) {
  if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
    LockRequest *upgrade_lock_request = nullptr;
    for (auto &lock_request : lock_request_queue->request_queue_) {
      if (lock_request->granted_) {
        continue;
      }
      if (lock_request->txn_id_ == lock_request_queue->upgrading_) {
        upgrade_lock_request = lock_request;
        break;
      }
    }

    bool is_compatible = true;
    for (auto &lock_request : lock_request_queue->request_queue_) {
      if (!lock_request->granted_) {
        break;
      }
      if (lock_request->txn_id_ == upgrade_lock_request->txn_id_) {
        continue;
      }
      if (!AreLocksCompatible(lock_request->lock_mode_, upgrade_lock_request->lock_mode_)) {
        is_compatible = false;
      }
    }
    if (is_compatible) {
      upgrade_lock_request->granted_ = true;
      lock_request_queue->cv_.notify_all();
    }
  } else {
    bool must_notify = false;
    for (auto &wait_lock_request : lock_request_queue->request_queue_) {
      if (wait_lock_request->granted_) {
        continue;
      }

      bool is_compatible = true;
      for (auto &grant_lock_request : lock_request_queue->request_queue_) {
        if (!grant_lock_request->granted_) {
          break;
        }
        if (!AreLocksCompatible(wait_lock_request->lock_mode_, grant_lock_request->lock_mode_)) {
          is_compatible = false;
          break;
        }
      }
      if (is_compatible) {
        wait_lock_request->granted_ = true;
        must_notify = true;
      } else {
        break;
      }
    }
    if (must_notify) {
      lock_request_queue->cv_.notify_all();
    }
  }
}

auto LockManager::TransactionAbort(Transaction *txn, AbortReason abortReason, bool notify,
                                   LockRequestQueue *lock_request_queue) -> void {
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), abortReason);
  if (notify && lock_request_queue != nullptr) {
    GrantNewLocksIfPossible(lock_request_queue);
  }
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  std::unique_lock<std::mutex> waits_for_guard(waits_for_latch_);
  auto iter = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
  if (iter == waits_for_[t1].end()) {
    waits_for_[t1].push_back(t2);
  }
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  std::unique_lock<std::mutex> waits_for_guard(waits_for_latch_);
  if (waits_for_.find(t1) == waits_for_.end()) {
    return;
  }
  auto iter = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
  if (iter == waits_for_[t1].end()) {
    return;
  }
  waits_for_[t1].erase(iter);
  if (waits_for_[t1].empty()) {
    waits_for_.erase(t1);
  }
}

auto LockManager::FindCycle(txn_id_t source_txn, std::vector<txn_id_t> &path, std::unordered_set<txn_id_t> &on_path,
                            std::unordered_set<txn_id_t> &visited, txn_id_t *abort_txn_id) -> bool {
  if (on_path.count(source_txn)) {
    auto cycle_start = std::find(path.begin(), path.end(), source_txn);
    *abort_txn_id = *std::max_element(cycle_start, path.end());
    return true;
  }
  if (visited.count(source_txn)) {
    return false;
  }
  visited.insert(source_txn);

  path.push_back(source_txn);
  on_path.insert(source_txn);
  if (waits_for_.count(source_txn)) {
    auto &waits_for_vec = waits_for_[source_txn];
    for (size_t i = 0; i < waits_for_vec.size(); i++) {
      if (FindCycle(waits_for_vec[i], path, on_path, visited, abort_txn_id)) {
        return true;
      }
    }
  }
  path.pop_back();
  on_path.erase(source_txn);
  return false;
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  *txn_id = INVALID_TXN_ID;

  std::vector<txn_id_t> path;
  std::unordered_set<txn_id_t> on_path;
  std::unordered_set<txn_id_t> visited;
  txn_id_t abort_txn_id;
  std::unique_lock<std::mutex> waits_for_guard(waits_for_latch_);
  for (auto &iter : waits_for_) {
    if (FindCycle(iter.first, path, on_path, visited, &abort_txn_id)) {
      *txn_id = abort_txn_id;
      return true;
    }
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  std::unique_lock<std::mutex> waits_for_guard(waits_for_latch_);
  for (auto &iter : waits_for_) {
    for (size_t i = 0; i < iter.second.size(); i++) {
      edges.push_back({iter.first, iter.second[i]});
    }
  }
  return edges;
}

auto LockManager::GetWaitRelationFromLockQueue(LockRequestQueue *lock_request_queue) -> void {
  // 先单独对升级请求构造依赖图，然后再对普通等待请求构造依赖图

  LockRequest *upgrade_lock_request = nullptr;
  if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
    for (auto &lock_request : lock_request_queue->request_queue_) {
      if (lock_request->granted_) {
        continue;
      }
      if (lock_request->txn_id_ == lock_request_queue->upgrading_) {
        upgrade_lock_request = lock_request;
        break;
      }
    }
    if (upgrade_lock_request != nullptr) {
      for (auto &lock_request : lock_request_queue->request_queue_) {
        if (!lock_request->granted_) {
          break;
        }
        if (lock_request->txn_id_ == upgrade_lock_request->txn_id_) {
          continue;
        }
        if (!AreLocksCompatible(lock_request->lock_mode_, upgrade_lock_request->lock_mode_)) {
          AddEdge(upgrade_lock_request->txn_id_, lock_request->txn_id_);
        }
      }
    }
  }
  // 只要队列有未授权的升级请求，所有其他普通等待请求都应依赖该升级事务，不应再按兼容性决定
  for (auto wait_lock_request = lock_request_queue->request_queue_.begin();
       wait_lock_request != lock_request_queue->request_queue_.end(); wait_lock_request++) {
    if ((*wait_lock_request)->granted_) {
      continue;
    }
    if (upgrade_lock_request != nullptr) {
      if (upgrade_lock_request->txn_id_ != (*wait_lock_request)->txn_id_) {
        AddEdge((*wait_lock_request)->txn_id_, upgrade_lock_request->txn_id_);
      }
    } else {
      for (auto lock_request_for_check = lock_request_queue->request_queue_.begin();
           lock_request_for_check != wait_lock_request; lock_request_for_check++) {
        if (upgrade_lock_request != nullptr && (*lock_request_for_check)->txn_id_ == upgrade_lock_request->txn_id_) {
          continue;
        }
        if (!AreLocksCompatible((*wait_lock_request)->lock_mode_, (*lock_request_for_check)->lock_mode_)) {
          AddEdge((*wait_lock_request)->txn_id_, (*lock_request_for_check)->txn_id_);
        }
      }
    }
  }
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      std::unique_lock<std::mutex> waits_for_guard(waits_for_latch_);
      waits_for_.clear();
      waits_for_guard.unlock();

      std::unique_lock<std::mutex> table_lock_map_guard(table_lock_map_latch_);
      for (auto &iter : table_lock_map_) {
        std::unique_lock<std::mutex> lock_queue_guard(iter.second->latch_);
        GetWaitRelationFromLockQueue(iter.second.get());
      }
      table_lock_map_guard.unlock();

      std::unique_lock<std::mutex> row_lock_map_guard(row_lock_map_latch_);
      for (auto &iter : row_lock_map_) {
        std::unique_lock<std::mutex> lock_queue_guard(iter.second->latch_);
        GetWaitRelationFromLockQueue(iter.second.get());
      }
      row_lock_map_guard.unlock();

      txn_id_t abort_txn_id = INVALID_TXN_ID;
      if (HasCycle(&abort_txn_id)) {
        Transaction *txn = txn_manager_->GetTransaction(abort_txn_id);
        txn->SetState(TransactionState::ABORTED);

        table_lock_map_guard.lock();
        for (auto &iter : table_lock_map_) {
          std::unique_lock<std::mutex> lock_queue_guard(iter.second->latch_);
          iter.second->cv_.notify_all();
        }
        table_lock_map_guard.unlock();

        row_lock_map_guard.lock();
        for (auto &iter : row_lock_map_) {
          std::unique_lock<std::mutex> lock_queue_guard(iter.second->latch_);
          iter.second->cv_.notify_all();
        }
        row_lock_map_guard.unlock();
      }
    }
  }
}

}  // namespace bustub
