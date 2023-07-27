#pragma once

#include <map>

#include "port/port.h"
#include "rocksdb/slice.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {
class PromotionCache {
 public:
  PromotionCache(const Comparator *ucmp)
      : ucmp_(ucmp), cache_(Compare(ucmp)), size_(0), max_size_(0) {}
  PromotionCache(const PromotionCache &) = delete;
  PromotionCache &operator=(const PromotionCache &) = delete;
  bool Get(Slice key, PinnableSlice *value) const {
    ReadLock l_(&lock_);
    // TODO: Avoid the copy here after upgrading to C++14
    auto it = cache_.find(key.ToString());
    if (it == cache_.end()) return false;
    value->PinSelf(it->second);
    return true;
  }
  void Promote(std::string key, Slice value) {
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
  std::vector<std::pair<std::string, std::string>> TakeRange(Slice smallest,
                                                             Slice largest) {
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
  size_t max_size() const { return max_size_; }

 private:
  class Compare {
   public:
    Compare(const Comparator *ucmp) : ucmp_(ucmp) {}
    bool operator()(const std::string &lhs, const std::string &rhs) const {
      return ucmp_->Compare(lhs, rhs) < 0;
    }

   private:
    const Comparator *ucmp_;
  };
  const Comparator *ucmp_;
  port::RWMutex lock_;
  std::map<std::string, std::string, Compare> cache_;
  size_t size_;

  size_t max_size_;
};
}  // namespace ROCKSDB_NAMESPACE
