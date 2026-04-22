#include "primer/trie.h"
#include <memory>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  auto cur = root_;
  for (char ch : key) {
    if (cur == nullptr) {
      return nullptr;
    }
    auto it = cur->children_.find(ch);
    cur = it == cur->children_.end() ? nullptr : it->second;
  }
  if (cur == nullptr) {
    return nullptr;
  }

  const TrieNodeWithValue<T> *nodeWithValue = dynamic_cast<const TrieNodeWithValue<T> *>(cur.get());
  if (nodeWithValue == nullptr) {
    return nullptr;
  }
  return nodeWithValue->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  // 1. 递归或迭代地从叶到根构建新路径
  // 2. 在 key 末尾创建 TrieNodeWithValue<T>
  // 3. 从叶往根，每层 clone 父节点并更新其 children_
  // 4. 用新的 root 构造新 Trie 返回

  std::vector<decltype(root_)> vec;
  auto cur = root_;
  for (char ch : key) {
    vec.push_back(cur);
    if (cur) {
      auto it = cur->children_.find(ch);
      cur = it == cur->children_.end() ? nullptr : it->second;
    }
  }

  std::shared_ptr<const TrieNode> node = std::make_shared<TrieNodeWithValue<T>>(
      cur ? cur->children_ : std::map<char, std::shared_ptr<const TrieNode>>{}, std::make_shared<T>(std::move(value)));

  for (int i = vec.size() - 1; i >= 0; --i) {
    std::shared_ptr<const TrieNode> parent = vec[i];
    auto cloneNode = parent ? parent->Clone() : std::make_unique<TrieNode>();
    cloneNode->children_[key[i]] = node;
    node = std::move(cloneNode);
  }

  return Trie(node);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value anymore,
  // you should convert it to `TrieNode`. If a node doesn't have children anymore, you should remove it.
  std::vector<decltype(root_)> vec;
  auto cur = root_;
  for (char ch : key) {
    if (cur == nullptr) {
      return *this;
    }
    vec.push_back(cur);
    auto it = cur->children_.find(ch);
    cur = it == cur->children_.end() ? nullptr : it->second;
  }
  if (cur == nullptr) {
    return *this;
  }

  std::shared_ptr<const TrieNode> node = std::make_shared<TrieNode>(cur->children_);
  if (!node->is_value_node_ && node->children_.size() == 0) {
    node = nullptr;
  }
  for (int i = vec.size() - 1; i >= 0; --i) {
    std::shared_ptr<const TrieNode> parent = vec[i];
    auto cloneNode = parent->Clone();
    if (node == nullptr) {
      cloneNode->children_.erase(key[i]);
      if (cloneNode->children_.size() == 0 && !cloneNode->is_value_node_) {
        cloneNode = nullptr;
      }
    } else {
      cloneNode->children_[key[i]] = node;
    }
    node = std::move(cloneNode);
  }
  return Trie(node);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
