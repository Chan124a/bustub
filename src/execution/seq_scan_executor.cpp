//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  bool has_IX = exec_ctx_->GetTransaction()->IsTableIntentionExclusiveLocked(plan_->GetTableOid());
  bool has_X = exec_ctx_->GetTransaction()->IsTableExclusiveLocked(plan_->GetTableOid());
  bool has_SIX = exec_ctx_->GetTransaction()->IsTableSharedIntentionExclusiveLocked(plan_->GetTableOid());
  bool has_IS = exec_ctx_->GetTransaction()->IsTableIntentionSharedLocked(plan_->GetTableOid());
  bool has_S = exec_ctx_->GetTransaction()->IsTableSharedLocked(plan_->GetTableOid());
  if (exec_ctx_->IsDelete()) {
    if (!has_IX && !has_X && !has_SIX) {
      if (has_S) {  // 已经有S则升级为SIX
        CheckLockOperation(
            [&] {
              return exec_ctx_->GetLockManager()->LockTable(
                  exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE, plan_->GetTableOid());
            },
            "lock table failed.");
      } else {
        CheckLockOperation(
            [&] {
              return exec_ctx_->GetLockManager()->LockTable(
                  exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->GetTableOid());
            },
            "lock table failed.");
      }
    }
  } else {
    auto isolation_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
    if (isolation_level == IsolationLevel::REPEATABLE_READ || isolation_level == IsolationLevel::READ_COMMITTED) {
      if (!has_IX && !has_X && !has_SIX && !has_S && !has_IS) {
        CheckLockOperation(
            [&] {
              return exec_ctx_->GetLockManager()->LockTable(
                  exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED, plan_->GetTableOid());
            },
            "lock table failed.");
      }
    }
  }
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  iter_.emplace(table_info->table_->MakeEagerIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!iter_->IsEnd()) {
    auto cur_rid = iter_->GetRID();
    bool is_new_lock = false;
    bool has_X = exec_ctx_->GetTransaction()->IsRowExclusiveLocked(plan_->GetTableOid(), cur_rid);
    bool has_S = exec_ctx_->GetTransaction()->IsRowSharedLocked(plan_->GetTableOid(), cur_rid);
    if (exec_ctx_->IsDelete()) {
      // 申请行写锁
      if (!has_X) {
        CheckLockOperation(
            [&] {
              return exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                                          plan_->GetTableOid(), cur_rid);
            },
            "lock row failed.");
        if (!has_S) {
          // 如果原来有读锁，那么上面申请行写
          // 如果原来没有读锁，那么上面申请的行写锁是本次查询获取的锁。
          is_new_lock = true;
        }
      }
    } else {
      // 申请行读锁
      auto isolation_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
      if (isolation_level == IsolationLevel::REPEATABLE_READ || isolation_level == IsolationLevel::READ_COMMITTED) {
        if (!has_S && !has_X) {
          CheckLockOperation(
              [&] {
                return exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                                            plan_->GetTableOid(), cur_rid);
              },
              "lock row failed.");
          is_new_lock = true;
        }
      }
    }

    auto cur_tuple = iter_->GetTuple();
    ++(*iter_);

    if (!cur_tuple.first.is_deleted_) {
      if (plan_->filter_predicate_ == nullptr) {
        if (!exec_ctx_->IsDelete() &&
            exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && is_new_lock) {
          // 对于隔离级别为READ_COMMITTED的读请求，可以立即释放本次查询申请的行读锁
          CheckLockOperation(
              [&] {
                return exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), plan_->GetTableOid(),
                                                              cur_rid, true);
              },
              "unlock row failed.");
        }
        *tuple = cur_tuple.second;
        *rid = cur_rid;
        return true;
      }
      auto value = plan_->filter_predicate_->Evaluate(&cur_tuple.second, GetOutputSchema());
      if (!value.IsNull() && value.GetAs<bool>()) {
        if (!exec_ctx_->IsDelete() &&
            exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && is_new_lock) {
          // 对于隔离级别为READ_COMMITTED的读请求，可以立即释放本次查询申请的行读锁
          CheckLockOperation(
              [&] {
                return exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), plan_->GetTableOid(),
                                                              cur_rid, true);
              },
              "unlock row failed.");
        }
        *tuple = cur_tuple.second;
        *rid = cur_rid;
        return true;
      }
    }

    if (exec_ctx_->IsDelete()) {
      if (is_new_lock) {
        CheckLockOperation(
            [&] {
              return exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), plan_->GetTableOid(), cur_rid,
                                                            true);
            },
            "unlock row failed.");
      }
    } else {
      auto isolation_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
      if (isolation_level == IsolationLevel::REPEATABLE_READ || isolation_level == IsolationLevel::READ_COMMITTED) {
        if (is_new_lock) {
          CheckLockOperation(
              [&] {
                return exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), plan_->GetTableOid(),
                                                              cur_rid, true);
              },
              "unlock row failed.");
        }
      }
    }
  }
  return false;
}

}  // namespace bustub
