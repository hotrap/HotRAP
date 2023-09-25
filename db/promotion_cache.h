#pragma once

#include <atomic>
#include <map>
#include <unordered_set>

#include "monitoring/instrumented_mutex.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

class ColumnFamilyData;
class DBImpl;
class InternalStats;

class UserKeyCompare {
 public:
  UserKeyCompare(const Comparator *ucmp) : ucmp_(ucmp) {}
  bool operator()(const std::string &lhs, const std::string &rhs) const {
    return ucmp_->Compare(lhs, rhs) < 0;
  }

 private:
  const Comparator *ucmp_;
};

struct ImmPromotionCache {
  std::map<std::string, std::string, UserKeyCompare> cache;
  size_t size;
  MutexProtected<std::unordered_set<std::string>> updated;
  ImmPromotionCache(
      std::map<std::string, std::string, UserKeyCompare> &&arg_cache,
      size_t arg_size)
      : cache(std::move(arg_cache)), size(arg_size) {}
};
struct ImmPromotionCacheList {
  std::list<ImmPromotionCache> list;
  size_t size = 0;
};

class PromotionCache {
 public:
  PromotionCache(int target_level, const Comparator *ucmp);
  PromotionCache(const PromotionCache &) = delete;
  PromotionCache &operator=(const PromotionCache &) = delete;
  bool Get(InternalStats *internal_stats, Slice key,
           PinnableSlice *value) const;
  // REQUIRES: PromotionCaches mutex not held
  void Promote(DBImpl &db, ColumnFamilyData &cfd, size_t write_buffer_size,
               std::string key, Slice value);
  // [begin, end)
  std::vector<std::pair<std::string, std::string>> TakeRange(Slice smallest,
                                                             Slice largest);
  // REQUIRES: DB mutex held
  void Flush();

  const RWMutexProtected<ImmPromotionCacheList> &imm_list() const {
    return imm_list_;
  }

  size_t max_size() const { return max_size_.load(std::memory_order_relaxed); }

 private:
  struct MutableCache {
    std::map<std::string, std::string, UserKeyCompare> cache;
    size_t size;
  };
  const int target_level_;
  const Comparator *ucmp_;
  RWMutexProtected<MutableCache> mut_;
  RWMutexProtected<ImmPromotionCacheList> imm_list_;
  std::atomic<size_t> max_size_;
};
}  // namespace ROCKSDB_NAMESPACE