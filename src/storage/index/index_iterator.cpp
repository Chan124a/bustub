/**
 * index_iterator.cpp
 */
#include <cassert>
#include "common/config.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, page_id_t page_id, int index)
    : bpm_(bpm), page_id_(page_id), index_(index) {
  if (page_id_ != INVALID_PAGE_ID) {
    page_guard_ = bpm_->FetchPageRead(page_id_);
    leaf_page_ = page_guard_.template As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    key_value_pair_ = {leaf_page_->KeyAt(index_), leaf_page_->ValueAt(index_)};
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  bpm_ = nullptr;
  page_id_ = INVALID_PAGE_ID;
  page_guard_.Drop();
  index_ = 0;
  key_value_pair_ = {KeyType{}, ValueType{}};
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return page_id_ == INVALID_PAGE_ID || index_ >= leaf_page_->GetSize(); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return key_value_pair_; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (page_id_ == INVALID_PAGE_ID) {
    return *this;
  }
  if (index_ + 1 < leaf_page_->GetSize()) {
    index_++;
    key_value_pair_ = {leaf_page_->KeyAt(index_), leaf_page_->ValueAt(index_)};
    return *this;
  }
  if (leaf_page_->GetNextPageId() != INVALID_PAGE_ID) {
    page_id_ = leaf_page_->GetNextPageId();
    page_guard_ = bpm_->FetchPageRead(page_id_);
    leaf_page_ = page_guard_.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    index_ = 0;
    key_value_pair_ = {leaf_page_->KeyAt(index_), leaf_page_->ValueAt(index_)};
    return *this;
  }
  page_guard_.Drop();
  page_id_ = INVALID_PAGE_ID;
  leaf_page_ = nullptr;
  index_ = 0;
  key_value_pair_ = {KeyType{}, ValueType{}};
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  return bpm_ == itr.bpm_ && page_id_ == itr.page_id_ && leaf_page_ == itr.leaf_page_ && index_ == itr.index_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool { return !(*this == itr); }

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
