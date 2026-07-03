#include "execution/executors/sort_executor.h"
#include <algorithm>
#include <cstddef>
#include <utility>
#include "binder/bound_order_by.h"
#include "execution/expressions/abstract_expression.h"
#include "storage/table/tuple.h"
#include "type/type.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_->Init();
  child_pairs.clear();

  Tuple child_tuple{};
  RID child_rid{};
  while (child_->Next(&child_tuple, &child_rid)) {
    child_pairs.push_back({child_tuple, child_rid});
  }
  std::sort(child_pairs.begin(), child_pairs.end(),
            [&](const std::pair<Tuple, RID> &pair_a, const std::pair<Tuple, RID> &pair_b) {
              for (const auto &order : plan_->GetOrderBy()) {
                OrderByType type = order.first;
                BUSTUB_ASSERT(type != OrderByType::INVALID, "order type is invalid.");
                const auto &expr = order.second;
                Value v1 = expr->Evaluate(&pair_a.first, child_->GetOutputSchema());
                Value v2 = expr->Evaluate(&pair_b.first, child_->GetOutputSchema());
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
            });
  iter = child_pairs.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter == child_pairs.end()) {
    return false;
  }
  *tuple = iter->first;
  *rid = iter->second;
  ++iter;
  return true;
}

}  // namespace bustub
