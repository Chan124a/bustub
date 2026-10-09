//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.cpp
//
// Identification: src/storage/index/b_plus_tree.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree.h"
#include "buffer/traced_buffer_pool_manager.h"
#include "storage/index/b_plus_tree_debug.h"

namespace bustub {

FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LeafPosition(const LeafPage *leaf, const KeyType &key) const -> int {
  int lo = 0;
  int hi = leaf->GetSize();
  while (lo < hi) {
    int mid = (lo + hi) / 2;
    if (comparator_(leaf->KeyAt(mid), key) < 0) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return lo;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ChildPosition(const InternalPage *page, const KeyType &key) const -> int {
  int lo = 1;
  int hi = page->GetSize();
  while (lo < hi) {
    int mid = (lo + hi) / 2;
    if (comparator_(page->KeyAt(mid), key) <= 0) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return lo - 1;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RebuildLeaf(LeafPage *leaf, const std::vector<std::pair<KeyType, ValueType>> &entries,
                                 const std::vector<KeyType> &tombstones) {
  const auto next = leaf->GetNextPageId();
  leaf->Init(leaf_max_size_);
  leaf->SetNextPageId(next);
  for (const auto &entry : entries) {
    leaf->InsertAt(leaf->GetSize(), entry.first, entry.second);
  }
  if constexpr (LEAF_PAGE_TOMB_CNT > 0) {
    for (const auto &key : tombstones) {
      int pos = LeafPosition(leaf, key);
      if (pos == leaf->GetSize() || comparator_(leaf->KeyAt(pos), key) != 0) {
        continue;
      }
      if (leaf->GetTombstoneCount() == static_cast<size_t>(LEAF_PAGE_TOMB_CNT)) {
        auto oldest = leaf->GetTombstones().front();
        leaf->RemoveTombstoneAt(0);
        leaf->RemoveAt(LeafPosition(leaf, oldest));
        pos = LeafPosition(leaf, key);
      }
      leaf->InsertTombstone(pos);
    }
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : bpm_(std::make_shared<TracedBufferPoolManager>(buffer_pool_manager)),
      index_name_(std::move(name)),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.template AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/**
 * @brief Helper function to decide whether current b+tree is empty
 * @return Returns true if this B+ tree has no keys and values.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  auto guard = bpm_->ReadPage(header_page_id_);
  return guard.template As<BPlusTreeHeaderPage>()->root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/**
 * @brief Return the only value that associated with input key
 *
 * This method is used for point query
 *
 * @param key input key
 * @param[out] result vector that stores the only value that associated with input key, if the value exists
 * @return : true means key exists
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  auto header = bpm_->ReadPage(header_page_id_);
  page_id_t id = header.template As<BPlusTreeHeaderPage>()->root_page_id_;
  if (id == INVALID_PAGE_ID) {
    return false;
  }
  auto guard = bpm_->ReadPage(id);
  header.Drop();
  while (!guard.template As<BPlusTreePage>()->IsLeafPage()) {
    const auto *page = guard.template As<InternalPage>();
    id = page->ValueAt(ChildPosition(page, key));
    auto child = bpm_->ReadPage(id);
    guard = std::move(child);
  }
  const auto *leaf = guard.template As<LeafPage>();
  int pos = LeafPosition(leaf, key);
  if (pos == leaf->GetSize() || comparator_(leaf->KeyAt(pos), key) != 0) {
    return false;
  }
  for (const auto &tomb : leaf->GetTombstones()) {
    if (comparator_(tomb, key) == 0) {
      return false;
    }
  }
  result->push_back(leaf->ValueAt(pos));
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * @brief Insert constant key & value pair into b+ tree
 *
 * if current tree is empty, start new tree, update root page id and insert
 * entry; otherwise, insert into leaf page.
 *
 * @param key the key to insert
 * @param value the value associated with key
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false; otherwise, return true.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  // 常见情况只需修改一个叶页；持有头页读锁，可防止结构变化使刚找到的叶页失效。
  {
    // 读取头页，从中取得当前根页 ID。
    auto header_guard = bpm_->ReadPage(header_page_id_);
    auto root_id = header_guard.template As<BPlusTreeHeaderPage>()->root_page_id_;
    // 非空树才有可供快速插入的叶页；空树留给下方的写锁路径创建根页。
    if (root_id != INVALID_PAGE_ID) {
      // 从根页开始，沿搜索路径向下读取页面。
      auto guard = bpm_->ReadPage(root_id);
      while (!guard.template As<BPlusTreePage>()->IsLeafPage()) {
        // 当前页是内部页，按 key 找到应该进入的子页。
        const auto *page = guard.template As<InternalPage>();
        auto child = bpm_->ReadPage(page->ValueAt(ChildPosition(page, key)));
        // 转移 guard 的所有权，释放上一页的读锁，继续向下。
        guard = std::move(child);
      }
      // 记住目标叶页 ID，然后释放它的读锁以获取写锁。
      auto leaf_id = guard.GetPageId();
      guard.Drop();
      auto write_guard = bpm_->WritePage(leaf_id);
      // 将页面解释为可修改的叶页，并找出 key 的有序插入位置。
      auto *leaf = write_guard.template AsMut<LeafPage>();
      int pos = LeafPosition(leaf, key);
      // 相同 key 的物理条目已存在时，检查它是否已被墓碑标记为删除。
      if (pos < leaf->GetSize() && comparator_(leaf->KeyAt(pos), key) == 0) {
        // 墓碑列表按删除顺序保存；找到与此次插入 key 相同的墓碑。
        auto tombs = leaf->GetTombstones();
        auto it = std::find_if(tombs.begin(), tombs.end(), [&](const KeyType &t) { return comparator_(t, key) == 0; });
        // 没有墓碑说明 key 仍然有效，唯一索引不允许重复插入。
        if (it == tombs.end()) {
          return false;
        }
        // 删除旧墓碑和旧物理条目，再在原位置写入新 value，相当于复活该 key。
        leaf->RemoveTombstoneAt(static_cast<size_t>(it - tombs.begin()));
        leaf->RemoveAt(pos);
        leaf->InsertAt(pos, key, value);
        return true;
      }
      // 叶页尚有空位时，按有序位置直接插入，无须改动树结构。
      if (leaf->GetSize() < leaf->GetMaxSize()) {
        leaf->InsertAt(pos, key, value);
        return true;
      }
    }
  }

  // 叶页已满或树为空：分裂可能逐层传播到根，因此重新持有头页和整条路径的写锁。
  // 重新搜索也能看到快速路径释放锁后其他写入者所做的修改。
  Context ctx;
  // 将头页写 guard 放入上下文；函数返回时由上下文自动释放。
  ctx.header_page_.emplace(bpm_->WritePage(header_page_id_));
  auto *header = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  // 空树的第一次插入：分配并初始化一个兼作根页的叶页。
  if (header->root_page_id_ == INVALID_PAGE_ID) {
    auto id = bpm_->NewPage();
    auto guard = bpm_->WritePage(id);
    auto *leaf = guard.template AsMut<LeafPage>();
    leaf->Init(leaf_max_size_);
    // 新叶页只有一个条目，写入后将其设为根页。
    leaf->InsertAt(0, key, value);
    header->root_page_id_ = id;
    return true;
  }
  // 先锁根页，再逐层锁住选中的子页；write_set_ 保留从根到叶的路径。
  ctx.write_set_.push_back(bpm_->WritePage(header->root_page_id_));
  while (!ctx.write_set_.back().template As<BPlusTreePage>()->IsLeafPage()) {
    const auto *page = ctx.write_set_.back().template As<InternalPage>();
    auto child = page->ValueAt(ChildPosition(page, key));
    ctx.write_set_.push_back(bpm_->WritePage(child));
  }
  // 在写锁保护下重新定位 key；快速路径之后叶页内容可能已经变化。
  auto *leaf = ctx.write_set_.back().template AsMut<LeafPage>();
  int pos = LeafPosition(leaf, key);
  // 再次检查重复 key：若它带有墓碑，则移除旧条目并写入新值。
  if (pos < leaf->GetSize() && comparator_(leaf->KeyAt(pos), key) == 0) {
    auto tombs = leaf->GetTombstones();
    auto it = std::find_if(tombs.begin(), tombs.end(), [&](const KeyType &t) { return comparator_(t, key) == 0; });
    // 没有墓碑时，该 key 已存在且有效，插入失败。
    if (it == tombs.end()) {
      return false;
    }
    // 用墓碑在删除队列中的下标移除它，再替换对应的物理条目。
    leaf->RemoveTombstoneAt(static_cast<size_t>(it - tombs.begin()));
    leaf->RemoveAt(pos);
    leaf->InsertAt(pos, key, value);
    return true;
  }
  // 重试后若叶页有空位，则直接插入并结束。
  if (leaf->GetSize() < leaf->GetMaxSize()) {
    leaf->InsertAt(pos, key, value);
    return true;
  }

  // 叶页已满：先把旧条目按顺序复制到临时数组，便于统一处理插入和分裂。
  std::vector<std::pair<KeyType, ValueType>> entries;
  for (int i = 0; i < leaf->GetSize(); i++) {
    entries.emplace_back(leaf->KeyAt(i), leaf->ValueAt(i));
  }
  // 将新条目放到其排序位置；此时 entries 比原叶页多一个条目。
  entries.insert(entries.begin() + pos, {key, value});
  // 暂存墓碑，因为重建叶页时会重新初始化页面。
  auto tombs = leaf->GetTombstones();
  // 左叶页取前半部分，右叶页取后半部分。
  int split = static_cast<int>(entries.size() / 2);
  // 为右叶页分配页面并获取写锁。
  auto right_id = bpm_->NewPage();
  auto right_guard = bpm_->WritePage(right_id);
  auto *right = right_guard.template AsMut<LeafPage>();
  right->Init(leaf_max_size_);
  // 新右叶页继承旧叶页的后继，稍后再把旧叶页链接到新右叶页。
  right->SetNextPageId(leaf->GetNextPageId());
  // 按分裂点构造两页各自的有序条目。
  std::vector<std::pair<KeyType, ValueType>> left_entries(entries.begin(), entries.begin() + split);
  std::vector<std::pair<KeyType, ValueType>> right_entries(entries.begin() + split, entries.end());
  // 墓碑必须和对应的物理 key 留在同一侧，且维持原有顺序。
  std::vector<KeyType> left_tombs;
  std::vector<KeyType> right_tombs;
  for (const auto &tomb : tombs) {
    // 右页首 key 是两页的分界；小于它的墓碑归左页，其余归右页。
    (comparator_(tomb, right_entries.front().first) < 0 ? left_tombs : right_tombs).push_back(tomb);
  }
  // 清空并重建两个叶页，分别写回条目和墓碑。
  RebuildLeaf(leaf, left_entries, left_tombs);
  RebuildLeaf(right, right_entries, right_tombs);
  // 接好叶页链表，并用右页最小 key 作为要插入父页的分隔键。
  leaf->SetNextPageId(right_id);
  KeyType separator = right->KeyAt(0);
  // 保存分裂后的左页 ID，然后释放右页和路径末端的叶页 guard。
  page_id_t left_id = ctx.write_set_.back().GetPageId();
  right_guard.Drop();
  ctx.write_set_.pop_back();

  // 把 (separator, right_id) 插入父页；父页满时继续向上分裂。
  while (true) {
    // 路径已无父页，表示原根被分裂，需要创建新的内部根页。
    if (ctx.write_set_.empty()) {
      auto new_root_id = bpm_->NewPage();
      auto root_guard = bpm_->WritePage(new_root_id);
      auto *root = root_guard.template AsMut<InternalPage>();
      root->Init(internal_max_size_);
      // 内部页第 0 个 key 不参与查找；两个指针分别指向分裂后的左右子树。
      root->InsertAt(0, KeyType{}, left_id);
      root->InsertAt(1, separator, right_id);
      // 发布新根页 ID，树的高度增加一层。
      header->root_page_id_ = new_root_id;
      return true;
    }
    // 取得当前父页，并找到左子页指针之后的插入位置。
    auto *parent = ctx.write_set_.back().template AsMut<InternalPage>();
    int index = parent->ValueIndex(left_id) + 1;
    // 父页有空位时，插入右子页指针及其分隔键即可结束。
    if (parent->GetSize() < parent->GetMaxSize()) {
      parent->InsertAt(index, separator, right_id);
      return true;
    }
    // 父页也满了：复制所有子指针与分隔键，准备连同新条目一起分裂。
    std::vector<std::pair<KeyType, page_id_t>> children;
    // 第 0 个 key 是无效占位符，但其子页指针必须保留。
    children.emplace_back(KeyType{}, parent->ValueAt(0));
    for (int i = 1; i < parent->GetSize(); i++) {
      children.emplace_back(parent->KeyAt(i), parent->ValueAt(i));
    }
    // 在左子页之后插入新右子页，以及标识右子树下界的 separator。
    children.insert(children.begin() + index, {separator, right_id});
    // 确定新右内部页的起点；该位置的 key 将提升到上一层。
    int mid = static_cast<int>((children.size() + 1) / 2);
    separator = children[mid].first;
    // 当前父页成为分裂后的左页，新分配的页面成为右兄弟页。
    left_id = ctx.write_set_.back().GetPageId();
    right_id = bpm_->NewPage();
    auto sibling_guard = bpm_->WritePage(right_id);
    auto *sibling = sibling_guard.template AsMut<InternalPage>();
    // 重置两页，再把临时数组前后两段分别写回。
    parent->Init(internal_max_size_);
    sibling->Init(internal_max_size_);
    for (int i = 0; i < mid; i++) {
      parent->InsertAt(i, children[i].first, children[i].second);
    }
    for (int i = mid; i < static_cast<int>(children.size()); i++) {
      // 右页的第 0 个 key 不参与查找；其有效下界已提升为 separator。
      sibling->InsertAt(i - mid, children[i].first, children[i].second);
    }
    // 释放右兄弟页，并从路径中移除已处理的父页；下一轮处理其祖先。
    sibling_guard.Drop();
    ctx.write_set_.pop_back();
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/**
 * @brief Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 *
 * @param key input key
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // 快速路径：只持有头页读锁和目标叶页写锁，尝试在不调整树结构的情况下删除。
  {
    // 从头页读取根页 ID；空树中没有可删除的条目。
    auto header_guard = bpm_->ReadPage(header_page_id_);
    auto root_id = header_guard.template As<BPlusTreeHeaderPage>()->root_page_id_;
    if (root_id == INVALID_PAGE_ID) {
      return;
    }
    // 从根页沿 key 对应的子树向下查找叶页。
    auto guard = bpm_->ReadPage(root_id);
    while (!guard.template As<BPlusTreePage>()->IsLeafPage()) {
      const auto *page = guard.template As<InternalPage>();
      auto child = bpm_->ReadPage(page->ValueAt(ChildPosition(page, key)));
      // 将读 guard 移到子页，释放上一页的读锁。
      guard = std::move(child);
    }
    // 记下叶页 ID，并释放其读锁，以便重新取得写锁。
    auto leaf_id = guard.GetPageId();
    guard.Drop();
    auto write_guard = bpm_->WritePage(leaf_id);
    auto *leaf = write_guard.template AsMut<LeafPage>();
    // 找到 key 应在的有序位置；若该位置不是 key，则无需删除。
    int pos = LeafPosition(leaf, key);
    if (pos == leaf->GetSize() || comparator_(leaf->KeyAt(pos), key) != 0) {
      return;
    }
    // 墓碑表示该 key 已逻辑删除；避免对同一 key 重复删除。
    auto tombs = leaf->GetTombstones();
    for (const auto &tomb : tombs) {
      if (comparator_(tomb, key) == 0) {
        return;
      }
    }
    // 未启用墓碑，或墓碑队列已满时，本次删除会使某个物理条目消失。
    bool physically_deletes = LEAF_PAGE_TOMB_CNT == 0 || tombs.size() == static_cast<size_t>(LEAF_PAGE_TOMB_CNT);
    // 根叶页的特殊下限为 1；普通叶页使用页面定义的最小容量。
    int min_size = leaf_id == root_id ? 1 : leaf->GetMinSize();
    // 不会物理删除，或删除后仍不会低于最小容量时，可只修改此叶页。
    if (!physically_deletes || leaf->GetSize() > min_size) {
      // 编译期启用墓碑时，通过墓碑记录本次逻辑删除。
      if constexpr (LEAF_PAGE_TOMB_CNT > 0) {
        // 队列满时先淘汰最老的墓碑，并真正移除它指向的物理条目。
        if (leaf->GetTombstoneCount() == static_cast<size_t>(LEAF_PAGE_TOMB_CNT)) {
          auto oldest = tombs.front();
          leaf->RemoveTombstoneAt(0);
          leaf->RemoveAt(LeafPosition(leaf, oldest));
          // 移除条目会改变后续下标，因此重新计算当前 key 的位置。
          pos = LeafPosition(leaf, key);
        }
        // 将当前 key 的位置追加到墓碑队列。
        leaf->InsertTombstone(pos);
      } else {
        // 未启用墓碑时，立即从叶页移除 key/value 条目。
        leaf->RemoveAt(pos);
      }
      return;
    }
  }

  // 可能发生物理下溢或根页变化：重新锁住头页和整条路径，并重新检查 key。
  // 快速路径释放锁后，其他写入者可能已经修改了树。
  Context ctx;
  // 头页写锁保护根页 ID，write_set_ 将保存从根到叶的写 guard。
  ctx.header_page_.emplace(bpm_->WritePage(header_page_id_));
  auto *header = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  // 重试时树可能已被清空。
  if (header->root_page_id_ == INVALID_PAGE_ID) {
    return;
  }
  // 从根页开始，逐层锁住 key 所在路径的页面。
  ctx.write_set_.push_back(bpm_->WritePage(header->root_page_id_));
  while (!ctx.write_set_.back().template As<BPlusTreePage>()->IsLeafPage()) {
    const auto *page = ctx.write_set_.back().template As<InternalPage>();
    ctx.write_set_.push_back(bpm_->WritePage(page->ValueAt(ChildPosition(page, key))));
  }
  // 在叶页写锁下重新定位并验证 key。
  auto *leaf = ctx.write_set_.back().template AsMut<LeafPage>();
  int pos = LeafPosition(leaf, key);
  if (pos == leaf->GetSize() || comparator_(leaf->KeyAt(pos), key) != 0) {
    return;
  }
  // 若另一个写入者已经给 key 加了墓碑，此次删除直接结束。
  for (const auto &tomb : leaf->GetTombstones()) {
    if (comparator_(tomb, key) == 0) {
      return;
    }
  }
  // 与快速路径相同，按是否启用墓碑选择逻辑删除或物理删除。
  if constexpr (LEAF_PAGE_TOMB_CNT > 0) {
    // 墓碑队列满时，先兑现最早一次删除，腾出一个墓碑槽位。
    if (leaf->GetTombstoneCount() == static_cast<size_t>(LEAF_PAGE_TOMB_CNT)) {
      auto oldest = leaf->GetTombstones().front();
      leaf->RemoveTombstoneAt(0);
      int old_pos = LeafPosition(leaf, oldest);
      leaf->RemoveAt(old_pos);
      // 物理移除旧条目后，当前 key 的位置可能发生变化。
      pos = LeafPosition(leaf, key);
    }
    leaf->InsertTombstone(pos);
  } else {
    leaf->RemoveAt(pos);
  }

  // 暂存合并或根收缩后不再使用的页面，待释放所有 guard 后统一删除。
  std::vector<page_id_t> obsolete;
  // 从刚修改的叶页开始，逐层检查是否下溢并向上修复。
  while (true) {
    // 当前路径末端是本轮要检查的节点。
    auto &node_guard = ctx.write_set_.back();
    auto *node = node_guard.template AsMut<BPlusTreePage>();
    // 路径只剩根页时，按根页的特殊规则处理，不再查找兄弟页。
    if (ctx.write_set_.size() == 1) {
      if (node->IsLeafPage()) {
        // 根叶页没有任何物理条目时，整棵树变为空树。
        if (node->GetSize() == 0) {
          header->root_page_id_ = INVALID_PAGE_ID;
          obsolete.push_back(node_guard.GetPageId());
        }
      } else {
        // 内部根页只剩一个子指针时，让该子页直接成为新根。
        auto *root = node_guard.template AsMut<InternalPage>();
        if (root->GetSize() == 1) {
          header->root_page_id_ = root->ValueAt(0);
          obsolete.push_back(node_guard.GetPageId());
        }
      }
      // 根页检查完成，无需继续向上修复。
      break;
    }
    // 找到当前节点在父页中的子指针下标。
    auto *parent = ctx.write_set_[ctx.write_set_.size() - 2].template AsMut<InternalPage>();
    int index = parent->ValueIndex(node_guard.GetPageId());
    // 若非最左叶页仍有条目，更新父页中代表它的最小 key。
    if (node->IsLeafPage() && node->GetSize() > 0 && index > 0) {
      parent->SetKeyAt(index, node_guard.template As<LeafPage>()->KeyAt(0));
    }
    // 叶页与内部页使用各自的最小占用量。
    int min_size = node->IsLeafPage() ? node->GetMinSize() : (node->GetMaxSize() + 1) / 2;
    // 当前节点未低于最小占用量，向上修复到此结束。
    if (node->GetSize() >= min_size) {
      break;
    }
    // 优先选左兄弟；当前节点是最左子页时才选右兄弟。
    bool has_left = index > 0;
    int sib_index = has_left ? index - 1 : index + 1;
    // 写锁保护兄弟页，以便借条目或合并。
    auto sibling_guard = bpm_->WritePage(parent->ValueAt(sib_index));
    auto *sibling = sibling_guard.template AsMut<BPlusTreePage>();
    // 判断兄弟页能否借出条目，并检查两页是否因总量过大而无法合并。
    int sibling_min = sibling->IsLeafPage() ? sibling->GetMinSize() : (sibling->GetMaxSize() + 1) / 2;
    if (sibling->GetSize() > sibling_min && sibling->GetSize() + node->GetSize() > node->GetMaxSize()) {
      // 总量超过单页容量时，从兄弟页移动一个条目到当前节点。
      if (node->IsLeafPage()) {
        // dst 为不足额的叶页，src 为提供条目的兄弟叶页。
        auto *dst = node_guard.template AsMut<LeafPage>();
        auto *src = sibling_guard.template AsMut<LeafPage>();
        // 从左兄弟取最大条目，或从右兄弟取最小条目，以保持有序。
        int src_pos = has_left ? src->GetSize() - 1 : 0;
        KeyType moved_key = src->KeyAt(src_pos);
        ValueType moved_value = src->ValueAt(src_pos);
        // 暂存两页墓碑；重建页面时要恢复它们。
        auto src_tombs = src->GetTombstones();
        auto dst_tombs = dst->GetTombstones();
        // 构造移动条目之后两页各自的物理条目序列。
        std::vector<std::pair<KeyType, ValueType>> src_entries;
        std::vector<std::pair<KeyType, ValueType>> dst_entries;
        for (int i = 0; i < src->GetSize(); i++) {
          // 源页复制除被借走条目以外的所有条目。
          if (i != src_pos) {
            src_entries.emplace_back(src->KeyAt(i), src->ValueAt(i));
          }
        }
        for (int i = 0; i < dst->GetSize(); i++) {
          dst_entries.emplace_back(dst->KeyAt(i), dst->ValueAt(i));
        }
        // 左边借来的条目插在开头；右边借来的条目追加到末尾。
        dst_entries.insert(has_left ? dst_entries.begin() : dst_entries.end(), {moved_key, moved_value});
        // 若移动条目已有墓碑，也将墓碑转移到目标页。
        auto it = std::find_if(src_tombs.begin(), src_tombs.end(),
                               [&](const KeyType &t) { return comparator_(t, moved_key) == 0; });
        if (it != src_tombs.end()) {
          dst_tombs.push_back(*it);
          src_tombs.erase(it);
        }
        // 根据新的条目和墓碑序列重建两页，并修正父页分隔键。
        RebuildLeaf(src, src_entries, src_tombs);
        RebuildLeaf(dst, dst_entries, dst_tombs);
        parent->SetKeyAt(has_left ? index : sib_index, has_left ? dst->KeyAt(0) : src->KeyAt(0));
      } else {
        // 内部页借位时，还必须同步调整父页分隔键。
        auto *dst = node_guard.template AsMut<InternalPage>();
        auto *src = sibling_guard.template AsMut<InternalPage>();
        if (has_left) {
          // 左兄弟的末尾子指针移到当前页开头；旧父分隔键下移。
          KeyType new_separator = src->KeyAt(src->GetSize() - 1);
          auto child = src->ValueAt(src->GetSize() - 1);
          src->RemoveAt(src->GetSize() - 1);
          dst->InsertAt(0, KeyType{}, child);
          dst->SetKeyAt(1, parent->KeyAt(index));
          // 原左兄弟末尾的 key 成为新的父页分隔键。
          parent->SetKeyAt(index, new_separator);
        } else {
          // 右兄弟的首个子指针移到当前页末尾；旧父分隔键下移。
          auto child = src->ValueAt(0);
          KeyType new_separator = src->KeyAt(1);
          dst->InsertAt(dst->GetSize(), parent->KeyAt(sib_index), child);
          src->RemoveAt(0);
          // 右兄弟原来的下一个 key 成为新的父页分隔键。
          parent->SetKeyAt(sib_index, new_separator);
        }
      }
      // 借位未减少父页的子指针数量，无须继续向上处理。
      break;
    }

    // 两页可合并时，统一把右页内容并入左页；随后删去父页中的右子指针。
    auto *left = (has_left ? sibling_guard : node_guard).template AsMut<BPlusTreePage>();
    auto *right = (has_left ? node_guard : sibling_guard).template AsMut<BPlusTreePage>();
    if (node->IsLeafPage()) {
      // 叶页合并：按 key 顺序先收集左页，再收集右页的所有物理条目。
      auto *dst = reinterpret_cast<LeafPage *>(left);
      auto *src = reinterpret_cast<LeafPage *>(right);
      std::vector<std::pair<KeyType, ValueType>> entries;
      for (int i = 0; i < dst->GetSize(); i++) {
        entries.emplace_back(dst->KeyAt(i), dst->ValueAt(i));
      }
      for (int i = 0; i < src->GetSize(); i++) {
        entries.emplace_back(src->KeyAt(i), src->ValueAt(i));
      }
      // 将右页墓碑接在左页墓碑之后，供重建后的单个叶页使用。
      auto tombs = dst->GetTombstones();
      auto donor_tombs = src->GetTombstones();
      tombs.insert(tombs.end(), donor_tombs.begin(), donor_tombs.end());
      // 合并后左页直接指向原右页的后继叶页。
      page_id_t next = src->GetNextPageId();
      RebuildLeaf(dst, entries, tombs);
      dst->SetNextPageId(next);
    } else {
      // 内部页合并：先用父页分隔键连接左页和右页首个子指针。
      auto *dst = reinterpret_cast<InternalPage *>(left);
      auto *src = reinterpret_cast<InternalPage *>(right);
      int separator_index = has_left ? index : sib_index;
      dst->InsertAt(dst->GetSize(), parent->KeyAt(separator_index), src->ValueAt(0));
      // 右页其余有效 key 和子指针依次追加到左页。
      for (int i = 1; i < src->GetSize(); i++) {
        dst->InsertAt(dst->GetSize(), src->KeyAt(i), src->ValueAt(i));
      }
    }
    // 右页已被合并，记录待删除的页面 ID，并从父页移除其子指针。
    int removed_index = has_left ? index : sib_index;
    obsolete.push_back(parent->ValueAt(removed_index));
    parent->RemoveAt(removed_index);
    // 释放兄弟页 guard，移除当前层；下一轮检查父页是否下溢。
    sibling_guard.Drop();
    ctx.write_set_.pop_back();
  }
  // 删除物理页面前先释放路径及头页的写 guard。
  ctx.write_set_.clear();
  ctx.header_page_.reset();
  for (auto id : obsolete) {
    bpm_->DeletePage(id);
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/**
 * @brief Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 *
 * You may want to implement this while implementing Task #3.
 *
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  auto header = bpm_->ReadPage(header_page_id_);
  auto root_id = header.template As<BPlusTreeHeaderPage>()->root_page_id_;
  if (root_id == INVALID_PAGE_ID) {
    return {};
  }
  auto guard = bpm_->ReadPage(root_id);
  header.Drop();
  while (!guard.template As<BPlusTreePage>()->IsLeafPage()) {
    auto child_id = guard.template As<InternalPage>()->ValueAt(0);
    auto child = bpm_->ReadPage(child_id);
    guard = std::move(child);
  }
  return INDEXITERATOR_TYPE(bpm_, std::move(guard), 0, comparator_);
}

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto header = bpm_->ReadPage(header_page_id_);
  auto root_id = header.template As<BPlusTreeHeaderPage>()->root_page_id_;
  if (root_id == INVALID_PAGE_ID) {
    return {};
  }
  auto guard = bpm_->ReadPage(root_id);
  header.Drop();
  while (!guard.template As<BPlusTreePage>()->IsLeafPage()) {
    auto *page = guard.template As<InternalPage>();
    auto child_id = page->ValueAt(ChildPosition(page, key));
    auto child = bpm_->ReadPage(child_id);
    guard = std::move(child);
  }
  int index = LeafPosition(guard.template As<LeafPage>(), key);
  return INDEXITERATOR_TYPE(bpm_, std::move(guard), index, comparator_);
}

/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return {}; }

/**
 * @return Page id of the root of this tree
 *
 * You may want to implement this while implementing Task #3.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  auto guard = bpm_->ReadPage(header_page_id_);
  return guard.template As<BPlusTreeHeaderPage>()->root_page_id_;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
