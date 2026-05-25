#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/index/index_iterator.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
  root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return true; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
  if (root_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  BasicPageGuard page_guard = bpm_->FetchPageBasic(root_page_id_);
  auto page = page_guard.template AsMut<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    auto internal_page = page_guard.template AsMut<InternalPage>();
    int child_index = 0;
    for (int i = 1; i < internal_page->GetSize(); ++i) {
      if (comparator_(key, internal_page->KeyAt(i)) < 0) {
        break;
      }
      child_index = i;
    }

    page_guard = bpm_->FetchPageBasic(internal_page->ValueAt(child_index));
    page = page_guard.template AsMut<BPlusTreePage>();
  }

  auto leaf_page = page_guard.template AsMut<LeafPage>();
  int insert_index = 0;
  while (insert_index < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(insert_index), key) < 0) {
    insert_index++;
  }
  if (insert_index < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(insert_index), key) == 0) {
    result->push_back(leaf_page->ValueAt(insert_index));
    return true;
  }

  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
  if (root_page_id_ == INVALID_PAGE_ID) {
    auto root_page_guard = bpm_->NewPageGuarded(&root_page_id_);
    auto root_page = root_page_guard.template AsMut<LeafPage>();
    root_page->Init(leaf_max_size_);
    root_page->InsertAt(0, key, value);
    root_page->IncreaseSize(1);

    WritePageGuard header_page_guard = bpm_->FetchPageWrite(header_page_id_);
    auto header_page = header_page_guard.template AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = root_page_id_;
    return true;
  }

  BasicPageGuard page_guard = bpm_->FetchPageBasic(root_page_id_);
  auto page = page_guard.template AsMut<BPlusTreePage>();
  std::vector<BasicPageGuard> nodes;
  while (!page->IsLeafPage()) {
    auto internal_page = page_guard.template AsMut<InternalPage>();
    int child_index = 0;
    for (int i = 1; i < internal_page->GetSize(); ++i) {
      if (comparator_(key, internal_page->KeyAt(i)) < 0) {
        break;
      }
      child_index = i;
    }

    nodes.push_back(std::move(page_guard));
    page_id_t leaf_page_id = internal_page->ValueAt(child_index);
    page_guard = bpm_->FetchPageBasic(leaf_page_id);
    page = page_guard.template AsMut<BPlusTreePage>();
  }

  auto leaf_page = page_guard.template AsMut<LeafPage>();
  int insert_index = 0;
  while (insert_index < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(insert_index), key) < 0) {
    insert_index++;
  }
  if (insert_index < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(insert_index), key) == 0) {
    return false;
  }

  std::vector<std::pair<KeyType, ValueType>> leaf_entries;
  leaf_entries.reserve(leaf_page->GetSize() + 1);
  for (int i = 0; i < leaf_page->GetSize(); ++i) {
    leaf_entries.push_back({leaf_page->KeyAt(i), leaf_page->ValueAt(i)});
  }
  leaf_entries.insert(leaf_entries.begin() + insert_index, {key, value});
  leaf_page->SetSize(0);

  if ((int)leaf_entries.size() < leaf_max_size_) {
    for (int i = 0; i < (int)leaf_entries.size(); ++i) {
      leaf_page->InsertAt(i, leaf_entries[i].first, leaf_entries[i].second);
      leaf_page->IncreaseSize(1);
    }
    return true;
  }

  int split = leaf_entries.size() / 2;
  page_id_t new_leaf_page_id;
  auto new_leaf_page_guard = bpm_->NewPageGuarded(&new_leaf_page_id);
  auto new_leaf_page = new_leaf_page_guard.template AsMut<LeafPage>();
  new_leaf_page->Init(leaf_max_size_);
  new_leaf_page->SetSize(0);
  new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(new_leaf_page_id);
  leaf_page->SetSize(0);
  for (int i = 0; i < split; ++i) {
    leaf_page->InsertAt(i, leaf_entries[i].first, leaf_entries[i].second);
    leaf_page->IncreaseSize(1);
  }
  for (int i = 0; i < (int)leaf_entries.size() - split; ++i) {
    new_leaf_page->InsertAt(i, leaf_entries[i + split].first, leaf_entries[i + split].second);
    new_leaf_page->IncreaseSize(1);
  }

  page_id_t insert_page_id = new_leaf_page_id;
  KeyType insert_key = leaf_entries[split].first;
  while (true) {
    if (nodes.empty()) {
      page_id_t new_internal_page_id;
      auto new_internal_page_guard = bpm_->NewPageGuarded(&new_internal_page_id);
      auto new_internal_page = new_internal_page_guard.template AsMut<InternalPage>();
      new_internal_page->Init(internal_max_size_);
      new_internal_page->InsertAt(0, (KeyType){}, root_page_id_);
      new_internal_page->IncreaseSize(1);
      new_internal_page->InsertAt(1, insert_key, insert_page_id);
      new_internal_page->IncreaseSize(1);

      root_page_id_ = new_internal_page_id;
      WritePageGuard header_page_guard = bpm_->FetchPageWrite(header_page_id_);
      auto header_page = header_page_guard.template AsMut<BPlusTreeHeaderPage>();
      header_page->root_page_id_ = root_page_id_;
      return true;
    }

    auto parent_page_guard = std::move(nodes.back());
    auto parent_page = parent_page_guard.template AsMut<InternalPage>();
    nodes.pop_back();
    // first,we must find the inserted index
    int insert_index = 0;
    for (int i = 1; i < parent_page->GetSize(); ++i) {
      if (comparator_(insert_key, parent_page->KeyAt(i)) < 0) {
        break;
      }
      insert_index = i;
    }
    std::vector<std::pair<KeyType, page_id_t>> parent_entries;
    parent_entries.reserve(parent_page->GetSize() + 1);
    for (int i = 0; i < parent_page->GetSize(); ++i) {
      parent_entries.push_back({parent_page->KeyAt(i), parent_page->ValueAt(i)});
    }
    parent_entries.insert(parent_entries.begin() + insert_index + 1, {insert_key, insert_page_id});
    parent_page->SetSize(0);

    if ((int)parent_entries.size() <= internal_max_size_) {
      for (int i = 0; i < (int)parent_entries.size(); ++i) {
        parent_page->InsertAt(i, parent_entries[i].first, parent_entries[i].second);
        parent_page->IncreaseSize(1);
      }
      return true;
    }

    page_id_t new_internal_page_id;
    auto new_internal_page_guard = bpm_->NewPageGuarded(&new_internal_page_id);
    auto new_internal_page = new_internal_page_guard.template AsMut<InternalPage>();
    new_internal_page->Init(internal_max_size_);
    new_internal_page->SetSize(0);
    parent_page->SetSize(0);
    split = parent_entries.size() / 2;
    for (int i = 0; i < split; ++i) {
      parent_page->InsertAt(i, parent_entries[i].first, parent_entries[i].second);
      parent_page->IncreaseSize(1);
    }

    for (int i = 0; i < (int)parent_entries.size() - split; ++i) {
      if (i == 0) {
        new_internal_page->InsertAt(i, KeyType{}, parent_entries[i + split].second);
      } else {
        new_internal_page->InsertAt(i, parent_entries[i + split].first, parent_entries[i + split].second);
      }
      new_internal_page->IncreaseSize(1);
    }

    insert_page_id = new_internal_page_id;
    insert_key = parent_entries[split].first;
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
  if (root_page_id_ == INVALID_PAGE_ID) {
    return;
  }
  // 1. find the destination of left_page
  BasicPageGuard page_guard = bpm_->FetchPageBasic(root_page_id_);
  auto page = page_guard.template AsMut<BPlusTreePage>();
  std::vector<BasicPageGuard> nodes;
  page_id_t leaf_page_id = root_page_id_;
  while (!page->IsLeafPage()) {
    auto internal_page = page_guard.template AsMut<InternalPage>();
    int child_index = 0;
    for (int i = 1; i < internal_page->GetSize(); ++i) {
      if (comparator_(key, internal_page->KeyAt(i)) < 0) {
        break;
      }
      child_index = i;
    }

    nodes.push_back(std::move(page_guard));
    leaf_page_id = internal_page->ValueAt(child_index);
    page_guard = bpm_->FetchPageBasic(leaf_page_id);
    page = page_guard.template AsMut<BPlusTreePage>();
  }
  // 2. find the deleted index in left_page
  auto leaf_page = page_guard.template AsMut<LeafPage>();
  int delete_index = 0;
  while (delete_index < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(delete_index), key) < 0) {
    delete_index++;
  }
  if (delete_index >= leaf_page->GetSize() || comparator_(leaf_page->KeyAt(delete_index), key) != 0) {
    return;
  }

  BPlusTreePage *cur_page = leaf_page;
  page_id_t last_page_id = INVALID_PAGE_ID;
  bool continue_loop = RemoveKeyFromLeafPage(delete_index, cur_page, nodes, last_page_id);
  while (continue_loop) {
    continue_loop = RemoveKeyFromInternalPage(delete_index, cur_page, nodes, last_page_id);
  }
  return;
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::RemoveKeyFromLeafPage(int &delete_index, BPlusTreePage *&cur_page,
                                           std::vector<BasicPageGuard> &nodes, page_id_t &last_page_id) {
  LeafPage *page = reinterpret_cast<LeafPage *>(cur_page);
  KeyType remove_key = page->KeyAt(delete_index);
  page->RemoveAt(delete_index);
  page->DecreaseSize(1);
  // 3.1 no need to merge leaf_page
  if (page->GetSize() >= page->GetMinSize()) {
    return false;
  }
  // 3.2 need to merge leaf_page
  if (nodes.empty()) {
    if (page->GetSize() == 0) {
      root_page_id_ = last_page_id;
      WritePageGuard header_page_guard = bpm_->FetchPageWrite(header_page_id_);
      auto header_page = header_page_guard.template AsMut<BPlusTreeHeaderPage>();
      header_page->root_page_id_ = root_page_id_;
    }
    return false;
  }
  auto parent_page_guard = std::move(nodes.back());
  auto parent_page = parent_page_guard.template AsMut<InternalPage>();
  nodes.pop_back();

  int key_index = 0;
  for (int i = 1; i < parent_page->GetSize(); ++i) {
    if (comparator_(remove_key, parent_page->KeyAt(i)) < 0) {
      break;
    }
    key_index = i;
  }

  LeafPage *left_page = nullptr;
  page_id_t left_page_id = INVALID_PAGE_ID;
  LeafPage *right_page = nullptr;
  int right_index = 0;
  if (key_index + 1 < parent_page->GetSize()) {
    BasicPageGuard sibling_page_guard = bpm_->FetchPageBasic(parent_page->ValueAt(key_index + 1));
    right_page = sibling_page_guard.template AsMut<LeafPage>();
    left_page = page;
    left_page_id = parent_page->ValueAt(key_index);
    right_index = key_index + 1;
  } else {
    BasicPageGuard sibling_page_guard = bpm_->FetchPageBasic(parent_page->ValueAt(key_index - 1));
    left_page = sibling_page_guard.template AsMut<LeafPage>();
    left_page_id = parent_page->ValueAt(key_index - 1);
    right_page = page;
    right_index = key_index;
  }
  std::vector<std::pair<KeyType, ValueType>> entries;
  entries.reserve(left_page->GetSize() + right_page->GetSize());
  for (int i = 0; i < left_page->GetSize(); ++i) {
    entries.push_back({left_page->KeyAt(i), left_page->ValueAt(i)});
  }
  for (int i = 0; i < right_page->GetSize(); ++i) {
    entries.push_back({right_page->KeyAt(i), right_page->ValueAt(i)});
  }
  // 4.1 no need to delete KEY from parent_page.we are retaining two left_page
  if ((int)entries.size() >= leaf_max_size_) {
    left_page->SetSize(0);
    right_page->SetSize(0);
    int split = entries.size() / 2;
    for (int i = 0; i < split; ++i) {
      left_page->InsertAt(i, entries[i].first, entries[i].second);
      left_page->IncreaseSize(1);
    }
    for (int i = 0; i < (int)entries.size() - split; ++i) {
      right_page->InsertAt(i, entries[i + split].first, entries[i + split].second);
      right_page->IncreaseSize(1);
    }
    KeyType key = right_page->KeyAt(0);
    page_id_t value = parent_page->ValueAt(right_index);
    parent_page->InsertAt(right_index, key, value);
    return false;
  }
  // 4.2 we merge two left_page and we should delete KEY from parent_page
  left_page->SetSize(0);
  for (int i = 0; i < (int)entries.size(); ++i) {
    left_page->InsertAt(i, entries[i].first, entries[i].second);
    left_page->IncreaseSize(1);
  }

  delete_index = right_index;
  cur_page = parent_page;
  last_page_id = left_page_id;
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::RemoveKeyFromInternalPage(int &delete_index, BPlusTreePage *&cur_page,
                                               std::vector<BasicPageGuard> &nodes, page_id_t &last_page_id) {
  InternalPage *page = reinterpret_cast<InternalPage *>(cur_page);
  KeyType remove_key = page->KeyAt(delete_index);
  page->RemoveAt(delete_index);
  page->DecreaseSize(1);
  if (page->GetSize() >= page->GetMinSize()) {
    return false;
  }
  if (nodes.empty()) {
    if (page->GetSize() == 0) {
      root_page_id_ = last_page_id;
      WritePageGuard header_page_guard = bpm_->FetchPageWrite(header_page_id_);
      auto header_page = header_page_guard.template AsMut<BPlusTreeHeaderPage>();
      header_page->root_page_id_ = root_page_id_;
    }
    return false;
  }
  auto parent_page_guard = std::move(nodes.back());
  auto parent_page = parent_page_guard.template AsMut<InternalPage>();
  nodes.pop_back();

  int key_index = 0;
  for (int i = 1; i < parent_page->GetSize(); ++i) {
    if (comparator_(remove_key, parent_page->KeyAt(i)) < 0) {
      break;
    }
    key_index = i;
  }

  InternalPage *left_page = nullptr;
  page_id_t left_page_id = INVALID_PAGE_ID;
  InternalPage *right_page = nullptr;
  int right_index = 0;
  if (key_index + 1 < parent_page->GetSize()) {
    BasicPageGuard sibling_page_guard = bpm_->FetchPageBasic(parent_page->ValueAt(key_index + 1));
    right_page = sibling_page_guard.template AsMut<InternalPage>();
    left_page = page;
    left_page_id = parent_page->ValueAt(key_index);
    right_index = key_index + 1;
  } else {
    BasicPageGuard sibling_page_guard = bpm_->FetchPageBasic(parent_page->ValueAt(key_index - 1));
    left_page = sibling_page_guard.template AsMut<InternalPage>();
    left_page_id = parent_page->ValueAt(key_index - 1);
    right_page = page;
    right_index = key_index;
  }
  std::vector<std::pair<KeyType, page_id_t>> entries;
  entries.reserve(left_page->GetSize() + right_page->GetSize());
  for (int i = 0; i < left_page->GetSize(); ++i) {
    entries.push_back({left_page->KeyAt(i), left_page->ValueAt(i)});
  }
  for (int i = 0; i < right_page->GetSize(); ++i) {
    if (i == 0) {
      KeyType key = parent_page->KeyAt(right_index);
      entries.push_back({key, right_page->ValueAt(i)});
    } else {
      entries.push_back({right_page->KeyAt(i), right_page->ValueAt(i)});
    }
  }

  if ((int)entries.size() > leaf_max_size_) {
    left_page->SetSize(0);
    right_page->SetSize(0);
    int split = entries.size() / 2;
    for (int i = 0; i < split; ++i) {
      if (i == 0) {
        left_page->InsertAt(i, KeyType{}, entries[i].second);
      } else {
        left_page->InsertAt(i, entries[i].first, entries[i].second);
      }
      left_page->IncreaseSize(1);
    }
    for (int i = 0; i < (int)entries.size() - split; ++i) {
      if (i == 0) {
        right_page->InsertAt(i, KeyType{}, entries[i + split].second);
      } else {
        right_page->InsertAt(i, entries[i + split].first, entries[i + split].second);
      }
      right_page->IncreaseSize(1);
    }
    KeyType key = entries[split].first;
    page_id_t value = parent_page->ValueAt(right_index);
    parent_page->InsertAt(right_index, key, value);
    return false;
  }

  left_page->SetSize(0);
  for (int i = 0; i < (int)entries.size(); ++i) {
    left_page->InsertAt(i, entries[i].first, entries[i].second);
    left_page->IncreaseSize(1);
  }

  delete_index = right_index;
  cur_page = parent_page;
  last_page_id = left_page_id;
  return true;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(bpm_, root_page_id_, 0);
  }

  page_id_t leaf_page_id = root_page_id_;
  BasicPageGuard page_guard = bpm_->FetchPageBasic(leaf_page_id);
  auto page = page_guard.template AsMut<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    auto internal_page = page_guard.template AsMut<InternalPage>();
    leaf_page_id = internal_page->ValueAt(0);
    page_guard = bpm_->FetchPageBasic(internal_page->ValueAt(0));
    page = page_guard.template AsMut<BPlusTreePage>();
  }

  return INDEXITERATOR_TYPE(bpm_, leaf_page_id, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(bpm_, root_page_id_, 0);
  }

  page_id_t leaf_page_id = root_page_id_;
  BasicPageGuard page_guard = bpm_->FetchPageBasic(leaf_page_id);
  auto page = page_guard.template AsMut<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    auto internal_page = page_guard.template AsMut<InternalPage>();
    int child_index = 0;
    for (int i = 1; i < internal_page->GetSize(); ++i) {
      if (comparator_(key, internal_page->KeyAt(i)) < 0) {
        break;
      }
      child_index = i;
    }
    leaf_page_id = internal_page->ValueAt(child_index);
    page_guard = bpm_->FetchPageBasic(internal_page->ValueAt(child_index));
    page = page_guard.template AsMut<BPlusTreePage>();
  }

  auto leaf_page = page_guard.template AsMut<LeafPage>();
  int index = 0;
  while (index < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(index), key) < 0) {
    index++;
  }
  if (index >= leaf_page->GetSize() || comparator_(leaf_page->KeyAt(index), key) != 0) {
    return INDEXITERATOR_TYPE(bpm_, leaf_page_id, leaf_page->GetSize());
  }
  return INDEXITERATOR_TYPE(bpm_, leaf_page_id, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, 0); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TAB*LE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
