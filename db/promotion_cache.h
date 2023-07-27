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
  bool Get(Slice key, PinnableSlice *value) const;
  void Promote(std::string key, Slice value);
  // [begin, end)
  std::vector<std::pair<std::string, std::string>> TakeRange(Slice smallest,
                                                             Slice largest);
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
