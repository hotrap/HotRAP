#pragma once

#include <map>

#include "port/port.h"
#include "rocksdb/slice.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {
class PromotionCache {
 public:
  PromotionCache(const Comparator *ucmp)
      : ucmp_(ucmp),
        mut_{MutableCache{
            std::map<std::string, std::string, Compare>{Compare(ucmp)}, 0, 0}} {
  }
  PromotionCache(const PromotionCache &) = delete;
  PromotionCache &operator=(const PromotionCache &) = delete;
  bool Get(Slice key, PinnableSlice *value) const;
  void Promote(std::string key, Slice value);
  // [begin, end)
  std::vector<std::pair<std::string, std::string>> TakeRange(Slice smallest,
                                                             Slice largest);
  size_t max_size() const { return mut_.Read().deref().max_size; }

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
  struct MutableCache {
    std::map<std::string, std::string, Compare> cache;
    size_t size;
    size_t max_size;
  };
  const Comparator *ucmp_;
  RWMutexProtected<MutableCache> mut_;
};
}  // namespace ROCKSDB_NAMESPACE
