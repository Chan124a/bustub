#include "execution/executors/topn_executor.h"
#include <utility>

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      top_entries_(TopNComparator{this}) {}

void TopNExecutor::Init() {
  while (!top_entries_.empty()) {
    top_entries_.pop();
  }
  result_.clear();

  child_executor_->Init();
  Tuple child_tuple{};
  RID child_rid{};
  if (plan_->GetN() > 0) {
    while (child_executor_->Next(&child_tuple, &child_rid)) {
      Entry candidate = {child_tuple, child_rid};
      if (top_entries_.size() < plan_->GetN()) {
        top_entries_.push(candidate);
      } else if (IsBetter(candidate, top_entries_.top())) {
        top_entries_.pop();
        top_entries_.push(candidate);
      }
    }
    while (!top_entries_.empty()) {
      result_.push_back(top_entries_.top());
      top_entries_.pop();
    }
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (result_.empty()) {
    return false;
  }
  *tuple = result_.back().first;
  *rid = result_.back().second;
  result_.pop_back();
  return true;
}

auto TopNExecutor::IsBetter(const Entry &a, const Entry &b) const -> bool {
  for (const auto &order : plan_->GetOrderBy()) {
    OrderByType type = order.first;
    BUSTUB_ASSERT(type != OrderByType::INVALID, "order type is invalid.");
    const auto &expr = order.second;
    Value v1 = expr->Evaluate(&a.first, child_executor_->GetOutputSchema());
    Value v2 = expr->Evaluate(&b.first, child_executor_->GetOutputSchema());
    // 为了满足sort排序时的传递性，我们必须统一NULL的排序方式，不能继续把NULL跟其他有效值当做相等
    // 先显式处理 v1.IsNull() / v2.IsNull()：
    // 1.都为 NULL：继续比较下一排序键；
    // 2.仅一方为 NULL：按统一策略直接决定先后，不能继续把它当作相等；
    // 3.都非 NULL：再依 ASC/DEFAULT 使用 <，DESC 使用 >。
    if (v1.IsNull() && v2.IsNull()) {
      continue;
    }
    if (v1.IsNull() || v2.IsNull()) {
      // 升序时，把NULL值排在非NULL值后面，降序则反过来。
      if (type == OrderByType::ASC || type == OrderByType::DEFAULT) {
        return v2.IsNull();
      } else if (type == OrderByType::DESC) {
        return v1.IsNull();
      }
    }
    if (v1.CompareEquals(v2) == CmpBool::CmpTrue) {
      continue;
    }

    if (type == OrderByType::ASC || type == OrderByType::DEFAULT) {
      return v1.CompareLessThan(v2) == CmpBool::CmpTrue;
    } else if (type == OrderByType::DESC) {
      return v1.CompareGreaterThan(v2) == CmpBool::CmpTrue;
    }
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() const -> size_t { return top_entries_.size(); };

}  // namespace bustub
