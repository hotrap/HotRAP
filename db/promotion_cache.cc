#include "promotion_cache.h"

namespace ROCKSDB_NAMESPACE {
bool PromotionCache::Get(Slice key, PinnableSlice *value) const {
  auto guard = mut_.Read();
  const MutableCache &mut = guard.deref();
  // TODO: Avoid the copy here after upgrading to C++14
  auto it = mut.cache.find(key.ToString());
  if (it != mut.cache.end()) {
    value->PinSelf(it->second);
    return true;
  }
  return false;
}

void PromotionCache::Promote(std::string key, Slice value) {
  auto guard = mut_.Write();
  MutableCache &mut = guard.deref_mut();
  // TODO: Avoid requiring the ownership of key here after upgrading to C++14
  auto it = mut.cache.find(key);
  if (it != mut.cache.end()) return;
  mut.size += key.size() + value.size();
  if (mut.size > mut.max_size) mut.max_size = mut.size;
  auto ret = mut.cache.insert(std::make_pair(std::move(key), value.ToString()));
  (void)ret;
  assert(ret.second == true);
}
// [begin, end)
std::vector<std::pair<std::string, std::string>> PromotionCache::TakeRange(
    Slice smallest, Slice largest) {
  auto guard = mut_.Write();
  MutableCache &mut = guard.deref_mut();
  std::vector<std::pair<std::string, std::string>> ret;
  auto begin_it = mut.cache.lower_bound(smallest.ToString());
  auto it = begin_it;
  while (it != mut.cache.end() && ucmp_->Compare(it->first, largest) <= 0) {
    // TODO: Is it possible to avoid copying here?
    ret.emplace_back(it->first, it->second);
    mut.size -= it->first.size() + it->second.size();
    ++it;
  }
  mut.cache.erase(begin_it, it);
  return ret;
}
}  // namespace ROCKSDB_NAMESPACE
