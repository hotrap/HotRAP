#include "promotion_cache.h"

namespace ROCKSDB_NAMESPACE {
bool PromotionCache::Get(Slice key, PinnableSlice *value) const {
  ReadLock l_(&lock_);
  // TODO: Avoid the copy here after upgrading to C++14
  auto it = cache_.find(key.ToString());
  if (it == cache_.end()) return false;
  value->PinSelf(it->second);
  return true;
}

void PromotionCache::Promote(std::string key, Slice value) {
  WriteLock l_(&lock_);
  // TODO: Avoid requiring the ownership of key here after upgrading to C++14
  auto it = cache_.find(key);
  if (it != cache_.end()) return;
  size_ += key.size() + value.size();
  if (size_ > max_size_) max_size_ = size_;
  auto ret = cache_.insert(std::make_pair(std::move(key), value.ToString()));
  (void)ret;
  assert(ret.second == true);
}
// [begin, end)
std::vector<std::pair<std::string, std::string>> PromotionCache::TakeRange(
    Slice smallest, Slice largest) {
  WriteLock l_(&lock_);
  std::vector<std::pair<std::string, std::string>> ret;
  auto begin_it = cache_.lower_bound(smallest.ToString());
  auto it = begin_it;
  while (it != cache_.end() && ucmp_->Compare(it->first, largest) <= 0) {
    // TODO: Is it possible to avoid copying here?
    ret.emplace_back(it->first, it->second);
    size_ -= it->first.size() + it->second.size();
    ++it;
  }
  cache_.erase(begin_it, it);
  return ret;
}
}  // namespace ROCKSDB_NAMESPACE
