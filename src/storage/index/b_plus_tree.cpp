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
  // Most inserts only touch one leaf. Keep a shared header latch so structural
  // changes cannot invalidate the path while upgrading that leaf's latch.
  {
    auto header_guard = bpm_->ReadPage(header_page_id_);
    auto root_id = header_guard.template As<BPlusTreeHeaderPage>()->root_page_id_;
    if (root_id != INVALID_PAGE_ID) {
      auto guard = bpm_->ReadPage(root_id);
      while (!guard.template As<BPlusTreePage>()->IsLeafPage()) {
        const auto *page = guard.template As<InternalPage>();
        auto child = bpm_->ReadPage(page->ValueAt(ChildPosition(page, key)));
        guard = std::move(child);
      }
      auto leaf_id = guard.GetPageId();
      guard.Drop();
      auto write_guard = bpm_->WritePage(leaf_id);
      auto *leaf = write_guard.template AsMut<LeafPage>();
      int pos = LeafPosition(leaf, key);
      if (pos < leaf->GetSize() && comparator_(leaf->KeyAt(pos), key) == 0) {
        auto tombs = leaf->GetTombstones();
        auto it = std::find_if(tombs.begin(), tombs.end(), [&](const KeyType &t) { return comparator_(t, key) == 0; });
        if (it == tombs.end()) {
          return false;
        }
        leaf->RemoveTombstoneAt(static_cast<size_t>(it - tombs.begin()));
        leaf->RemoveAt(pos);
        leaf->InsertAt(pos, key, value);
        return true;
      }
      if (leaf->GetSize() < leaf->GetMaxSize()) {
        leaf->InsertAt(pos, key, value);
        return true;
      }
    }
  }

  // Splits may propagate to the root, so retry with the header and entire
  // access path exclusively latched.
  Context ctx;
  ctx.header_page_.emplace(bpm_->WritePage(header_page_id_));
  auto *header = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  if (header->root_page_id_ == INVALID_PAGE_ID) {
    auto id = bpm_->NewPage();
    auto guard = bpm_->WritePage(id);
    auto *leaf = guard.template AsMut<LeafPage>();
    leaf->Init(leaf_max_size_);
    leaf->InsertAt(0, key, value);
    header->root_page_id_ = id;
    return true;
  }
  ctx.write_set_.push_back(bpm_->WritePage(header->root_page_id_));
  while (!ctx.write_set_.back().template As<BPlusTreePage>()->IsLeafPage()) {
    const auto *page = ctx.write_set_.back().template As<InternalPage>();
    auto child = page->ValueAt(ChildPosition(page, key));
    ctx.write_set_.push_back(bpm_->WritePage(child));
  }
  auto *leaf = ctx.write_set_.back().template AsMut<LeafPage>();
  int pos = LeafPosition(leaf, key);
  if (pos < leaf->GetSize() && comparator_(leaf->KeyAt(pos), key) == 0) {
    auto tombs = leaf->GetTombstones();
    auto it = std::find_if(tombs.begin(), tombs.end(), [&](const KeyType &t) { return comparator_(t, key) == 0; });
    if (it == tombs.end()) {
      return false;
    }
    leaf->RemoveTombstoneAt(static_cast<size_t>(it - tombs.begin()));
    leaf->RemoveAt(pos);
    leaf->InsertAt(pos, key, value);
    return true;
  }
  if (leaf->GetSize() < leaf->GetMaxSize()) {
    leaf->InsertAt(pos, key, value);
    return true;
  }

  std::vector<std::pair<KeyType, ValueType>> entries;
  for (int i = 0; i < leaf->GetSize(); i++) {
    entries.emplace_back(leaf->KeyAt(i), leaf->ValueAt(i));
  }
  entries.insert(entries.begin() + pos, {key, value});
  auto tombs = leaf->GetTombstones();
  int split = static_cast<int>(entries.size() / 2);
  auto right_id = bpm_->NewPage();
  auto right_guard = bpm_->WritePage(right_id);
  auto *right = right_guard.template AsMut<LeafPage>();
  right->Init(leaf_max_size_);
  right->SetNextPageId(leaf->GetNextPageId());
  std::vector<std::pair<KeyType, ValueType>> left_entries(entries.begin(), entries.begin() + split);
  std::vector<std::pair<KeyType, ValueType>> right_entries(entries.begin() + split, entries.end());
  std::vector<KeyType> left_tombs;
  std::vector<KeyType> right_tombs;
  for (const auto &tomb : tombs) {
    (comparator_(tomb, right_entries.front().first) < 0 ? left_tombs : right_tombs).push_back(tomb);
  }
  RebuildLeaf(leaf, left_entries, left_tombs);
  RebuildLeaf(right, right_entries, right_tombs);
  leaf->SetNextPageId(right_id);
  KeyType separator = right->KeyAt(0);
  page_id_t left_id = ctx.write_set_.back().GetPageId();
  right_guard.Drop();
  ctx.write_set_.pop_back();

  while (true) {
    if (ctx.write_set_.empty()) {
      auto new_root_id = bpm_->NewPage();
      auto root_guard = bpm_->WritePage(new_root_id);
      auto *root = root_guard.template AsMut<InternalPage>();
      root->Init(internal_max_size_);
      root->InsertAt(0, KeyType{}, left_id);
      root->InsertAt(1, separator, right_id);
      header->root_page_id_ = new_root_id;
      return true;
    }
    auto *parent = ctx.write_set_.back().template AsMut<InternalPage>();
    int index = parent->ValueIndex(left_id) + 1;
    if (parent->GetSize() < parent->GetMaxSize()) {
      parent->InsertAt(index, separator, right_id);
      return true;
    }
    std::vector<std::pair<KeyType, page_id_t>> children;
    children.emplace_back(KeyType{}, parent->ValueAt(0));
    for (int i = 1; i < parent->GetSize(); i++) {
      children.emplace_back(parent->KeyAt(i), parent->ValueAt(i));
    }
    children.insert(children.begin() + index, {separator, right_id});
    int mid = static_cast<int>((children.size() + 1) / 2);
    separator = children[mid].first;
    left_id = ctx.write_set_.back().GetPageId();
    right_id = bpm_->NewPage();
    auto sibling_guard = bpm_->WritePage(right_id);
    auto *sibling = sibling_guard.template AsMut<InternalPage>();
    parent->Init(internal_max_size_);
    sibling->Init(internal_max_size_);
    for (int i = 0; i < mid; i++) {
      parent->InsertAt(i, children[i].first, children[i].second);
    }
    for (int i = mid; i < static_cast<int>(children.size()); i++) {
      sibling->InsertAt(i - mid, children[i].first, children[i].second);
    }
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
  {
    auto header_guard = bpm_->ReadPage(header_page_id_);
    auto root_id = header_guard.template As<BPlusTreeHeaderPage>()->root_page_id_;
    if (root_id == INVALID_PAGE_ID) {
      return;
    }
    auto guard = bpm_->ReadPage(root_id);
    while (!guard.template As<BPlusTreePage>()->IsLeafPage()) {
      const auto *page = guard.template As<InternalPage>();
      auto child = bpm_->ReadPage(page->ValueAt(ChildPosition(page, key)));
      guard = std::move(child);
    }
    auto leaf_id = guard.GetPageId();
    guard.Drop();
    auto write_guard = bpm_->WritePage(leaf_id);
    auto *leaf = write_guard.template AsMut<LeafPage>();
    int pos = LeafPosition(leaf, key);
    if (pos == leaf->GetSize() || comparator_(leaf->KeyAt(pos), key) != 0) {
      return;
    }
    auto tombs = leaf->GetTombstones();
    for (const auto &tomb : tombs) {
      if (comparator_(tomb, key) == 0) {
        return;
      }
    }
    bool physically_deletes = LEAF_PAGE_TOMB_CNT == 0 || tombs.size() == static_cast<size_t>(LEAF_PAGE_TOMB_CNT);
    int min_size = leaf_id == root_id ? 1 : leaf->GetMinSize();
    if (!physically_deletes || leaf->GetSize() > min_size) {
      if constexpr (LEAF_PAGE_TOMB_CNT > 0) {
        if (leaf->GetTombstoneCount() == static_cast<size_t>(LEAF_PAGE_TOMB_CNT)) {
          auto oldest = tombs.front();
          leaf->RemoveTombstoneAt(0);
          leaf->RemoveAt(LeafPosition(leaf, oldest));
          pos = LeafPosition(leaf, key);
        }
        leaf->InsertTombstone(pos);
      } else {
        leaf->RemoveAt(pos);
      }
      return;
    }
  }

  // A physical underflow or root change needs the parent path. Recheck the
  // key after obtaining exclusive latches because another writer may have won.
  Context ctx;
  ctx.header_page_.emplace(bpm_->WritePage(header_page_id_));
  auto *header = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  if (header->root_page_id_ == INVALID_PAGE_ID) {
    return;
  }
  ctx.write_set_.push_back(bpm_->WritePage(header->root_page_id_));
  while (!ctx.write_set_.back().template As<BPlusTreePage>()->IsLeafPage()) {
    const auto *page = ctx.write_set_.back().template As<InternalPage>();
    ctx.write_set_.push_back(bpm_->WritePage(page->ValueAt(ChildPosition(page, key))));
  }
  auto *leaf = ctx.write_set_.back().template AsMut<LeafPage>();
  int pos = LeafPosition(leaf, key);
  if (pos == leaf->GetSize() || comparator_(leaf->KeyAt(pos), key) != 0) {
    return;
  }
  for (const auto &tomb : leaf->GetTombstones()) {
    if (comparator_(tomb, key) == 0) {
      return;
    }
  }
  if constexpr (LEAF_PAGE_TOMB_CNT > 0) {
    if (leaf->GetTombstoneCount() == static_cast<size_t>(LEAF_PAGE_TOMB_CNT)) {
      auto oldest = leaf->GetTombstones().front();
      leaf->RemoveTombstoneAt(0);
      int old_pos = LeafPosition(leaf, oldest);
      leaf->RemoveAt(old_pos);
      pos = LeafPosition(leaf, key);
    }
    leaf->InsertTombstone(pos);
  } else {
    leaf->RemoveAt(pos);
  }

  std::vector<page_id_t> obsolete;
  while (true) {
    auto &node_guard = ctx.write_set_.back();
    auto *node = node_guard.template AsMut<BPlusTreePage>();
    if (ctx.write_set_.size() == 1) {
      if (node->IsLeafPage()) {
        if (node->GetSize() == 0) {
          header->root_page_id_ = INVALID_PAGE_ID;
          obsolete.push_back(node_guard.GetPageId());
        }
      } else {
        auto *root = node_guard.template AsMut<InternalPage>();
        if (root->GetSize() == 1) {
          header->root_page_id_ = root->ValueAt(0);
          obsolete.push_back(node_guard.GetPageId());
        }
      }
      break;
    }
    auto *parent = ctx.write_set_[ctx.write_set_.size() - 2].template AsMut<InternalPage>();
    int index = parent->ValueIndex(node_guard.GetPageId());
    if (node->IsLeafPage() && node->GetSize() > 0 && index > 0) {
      parent->SetKeyAt(index, node_guard.template As<LeafPage>()->KeyAt(0));
    }
    int min_size = node->IsLeafPage() ? node->GetMinSize() : (node->GetMaxSize() + 1) / 2;
    if (node->GetSize() >= min_size) {
      break;
    }
    bool has_left = index > 0;
    int sib_index = has_left ? index - 1 : index + 1;
    auto sibling_guard = bpm_->WritePage(parent->ValueAt(sib_index));
    auto *sibling = sibling_guard.template AsMut<BPlusTreePage>();
    int sibling_min = sibling->IsLeafPage() ? sibling->GetMinSize() : (sibling->GetMaxSize() + 1) / 2;
    if (sibling->GetSize() > sibling_min && sibling->GetSize() + node->GetSize() > node->GetMaxSize()) {
      if (node->IsLeafPage()) {
        auto *dst = node_guard.template AsMut<LeafPage>();
        auto *src = sibling_guard.template AsMut<LeafPage>();
        int src_pos = has_left ? src->GetSize() - 1 : 0;
        KeyType moved_key = src->KeyAt(src_pos);
        ValueType moved_value = src->ValueAt(src_pos);
        auto src_tombs = src->GetTombstones();
        auto dst_tombs = dst->GetTombstones();
        std::vector<std::pair<KeyType, ValueType>> src_entries;
        std::vector<std::pair<KeyType, ValueType>> dst_entries;
        for (int i = 0; i < src->GetSize(); i++) {
          if (i != src_pos) {
            src_entries.emplace_back(src->KeyAt(i), src->ValueAt(i));
          }
        }
        for (int i = 0; i < dst->GetSize(); i++) {
          dst_entries.emplace_back(dst->KeyAt(i), dst->ValueAt(i));
        }
        dst_entries.insert(has_left ? dst_entries.begin() : dst_entries.end(), {moved_key, moved_value});
        auto it = std::find_if(src_tombs.begin(), src_tombs.end(),
                               [&](const KeyType &t) { return comparator_(t, moved_key) == 0; });
        if (it != src_tombs.end()) {
          dst_tombs.push_back(*it);
          src_tombs.erase(it);
        }
        RebuildLeaf(src, src_entries, src_tombs);
        RebuildLeaf(dst, dst_entries, dst_tombs);
        parent->SetKeyAt(has_left ? index : sib_index, has_left ? dst->KeyAt(0) : src->KeyAt(0));
      } else {
        auto *dst = node_guard.template AsMut<InternalPage>();
        auto *src = sibling_guard.template AsMut<InternalPage>();
        if (has_left) {
          KeyType new_separator = src->KeyAt(src->GetSize() - 1);
          auto child = src->ValueAt(src->GetSize() - 1);
          src->RemoveAt(src->GetSize() - 1);
          dst->InsertAt(0, KeyType{}, child);
          dst->SetKeyAt(1, parent->KeyAt(index));
          parent->SetKeyAt(index, new_separator);
        } else {
          auto child = src->ValueAt(0);
          KeyType new_separator = src->KeyAt(1);
          dst->InsertAt(dst->GetSize(), parent->KeyAt(sib_index), child);
          src->RemoveAt(0);
          parent->SetKeyAt(sib_index, new_separator);
        }
      }
      break;
    }

    // Merge the right page into the left page. Tombstones from the donor are newer.
    auto *left = (has_left ? sibling_guard : node_guard).template AsMut<BPlusTreePage>();
    auto *right = (has_left ? node_guard : sibling_guard).template AsMut<BPlusTreePage>();
    if (node->IsLeafPage()) {
      auto *dst = reinterpret_cast<LeafPage *>(left);
      auto *src = reinterpret_cast<LeafPage *>(right);
      std::vector<std::pair<KeyType, ValueType>> entries;
      for (int i = 0; i < dst->GetSize(); i++) {
        entries.emplace_back(dst->KeyAt(i), dst->ValueAt(i));
      }
      for (int i = 0; i < src->GetSize(); i++) {
        entries.emplace_back(src->KeyAt(i), src->ValueAt(i));
      }
      auto tombs = dst->GetTombstones();
      auto donor_tombs = src->GetTombstones();
      tombs.insert(tombs.end(), donor_tombs.begin(), donor_tombs.end());
      page_id_t next = src->GetNextPageId();
      RebuildLeaf(dst, entries, tombs);
      dst->SetNextPageId(next);
    } else {
      auto *dst = reinterpret_cast<InternalPage *>(left);
      auto *src = reinterpret_cast<InternalPage *>(right);
      int separator_index = has_left ? index : sib_index;
      dst->InsertAt(dst->GetSize(), parent->KeyAt(separator_index), src->ValueAt(0));
      for (int i = 1; i < src->GetSize(); i++) {
        dst->InsertAt(dst->GetSize(), src->KeyAt(i), src->ValueAt(i));
      }
    }
    int removed_index = has_left ? index : sib_index;
    obsolete.push_back(parent->ValueAt(removed_index));
    parent->RemoveAt(removed_index);
    sibling_guard.Drop();
    ctx.write_set_.pop_back();
  }
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
