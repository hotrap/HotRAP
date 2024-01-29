#pragma once

#include <atomic>
#include <condition_variable>
#include <map>
#include <mutex>
#include <queue>
#include <unordered_set>

#include "monitoring/instrumented_mutex.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "util/mutexlock.h"
#include "tbb/concurrent_hash_map.h"

namespace ROCKSDB_NAMESPACE {

class ColumnFamilyData;
class DBImpl;
class InternalStats;
struct SuperVersion;

class PromotionCache;
struct PCData {
  std::string value;
  int count{0};
};
using PCHashTable = tbb::concurrent_hash_map<std::string, PCData>;

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
  std::map<std::string, PCData, UserKeyCompare> cache;
  size_t size;
  MutexProtected<std::unordered_set<std::string>> updated;
  ImmPromotionCache(
      std::map<std::string, PCData, UserKeyCompare> &&arg_cache,
      size_t arg_size)
      : cache(std::move(arg_cache)), size(arg_size) {}
  ImmPromotionCache(
      PCHashTable &&arg_cache,
      const Comparator* ucmp,
      size_t arg_size)
      : cache(UserKeyCompare(ucmp)), size(arg_size) {
        for (auto&& a : arg_cache) {
          cache.insert(std::make_pair(a.first, a.second));
        }
      }
};
struct ImmPromotionCacheList {
  std::list<ImmPromotionCache> list;
  size_t size = 0;
};
struct MutablePromotionCache {
  MutablePromotionCache(const Comparator *ucmp)
      : ucmp_(ucmp), size(new std::atomic<size_t>(0)) {}
  // Return the size of the mutable promotion cache
  size_t Insert(InternalStats *internal_stats, const std::string& key, Slice value);
  // [begin, end)
  std::vector<std::pair<std::string, std::string>> TakeRange(
      InternalStats *internal_stats, CompactionRouter *router, Slice smallest,
      Slice largest);

 private:
  const Comparator *ucmp_;
  PCHashTable cache;
  std::unique_ptr<std::atomic<size_t>> size;
  friend class PromotionCache;
};

class PromotionCache {
 public:
  PromotionCache(DBImpl &db, int target_level, const Comparator *ucmp);
  PromotionCache(const PromotionCache &) = delete;
  PromotionCache &operator=(const PromotionCache &) = delete;
  ~PromotionCache();
  // Should be called with db mutex held
  void stop_checker_no_wait();
  // Not thread-safe
  void wait_for_checker_to_stop();
  bool Get(InternalStats *internal_stats, Slice key,
           PinnableSlice *value) const;
  // REQUIRES: DB mutex held
  // Will unlock the DB mutex.
  void SwitchMutablePromotionCache(DBImpl &db, ColumnFamilyData &cfd,
                                   MutablePromotionCache *mut) const;
  // REQUIRES: DB mutex held
  void Flush();

  const RWMutexProtected<MutablePromotionCache> &mut() const { return mut_; }
  const RWMutexProtected<ImmPromotionCacheList> &imm_list() const {
    return imm_list_;
  }

  std::atomic<size_t> &max_size() const { return max_size_; }

 private:
  struct CheckerQueueElem {
    DBImpl *db;
    SuperVersion *sv;
    std::list<ImmPromotionCache>::iterator iter;
  };
  void checker();

  DBImpl &db_;
  const int target_level_;
  RWMutexProtected<MutablePromotionCache> mut_;
  RWMutexProtected<ImmPromotionCacheList> imm_list_;
  mutable std::atomic<size_t> max_size_;

  mutable std::mutex checker_lock_;
  mutable std::queue<CheckerQueueElem> checker_queue_;
  mutable std::condition_variable signal_check_;
  bool should_stop_;
  std::thread checker_;
};
}  // namespace ROCKSDB_NAMESPACE
