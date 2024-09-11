#pragma once

#include <atomic>
#include <condition_variable>
#include <map>
#include <mutex>
#include <queue>
#include <unordered_set>

#include "db/dbformat.h"
#include "monitoring/instrumented_mutex.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/types.h"
#include "tbb/concurrent_hash_map.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

class ColumnFamilyData;
class DBImpl;
class InternalStats;
struct SuperVersion;

class PromotionCache;
struct PCData {
  SequenceNumber sequence;
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

class InternalKeyCompare {
 public:
  InternalKeyCompare(const Comparator *ucmp) : icmp_(ucmp) {}
  bool operator()(const std::pair<std::string, std::string> &lhs,
                  const std::pair<std::string, std::string> &rhs) const {
    return icmp_.Compare(lhs.first, rhs.first) < 0;
  }

  const InternalKeyComparator &icmp() const { return icmp_; }

 private:
  InternalKeyComparator icmp_;
};

struct ImmPromotionCache {
  std::unordered_map<std::string, PCData> cache;
  size_t size;
  MutexProtected<std::unordered_set<std::string>> updated;
  ImmPromotionCache(std::unordered_map<std::string, PCData> &&arg_cache,
                    size_t arg_size)
      : cache(std::move(arg_cache)), size(arg_size) {}
};
struct ImmPromotionCacheList {
  std::list<ImmPromotionCache> list;
  size_t size = 0;
};
struct MutablePromotionCache {
  MutablePromotionCache(const Comparator *ucmp) : ucmp_(ucmp), size_(0) {}
  MutablePromotionCache(const MutablePromotionCache &&rhs)
      : ucmp_(rhs.ucmp_),
        cache(std::move(rhs.cache)),
        size_(rhs.size_.load(std::memory_order_relaxed)) {}

  // Return the size of the mutable promotion cache
  size_t Insert(Slice user_key, SequenceNumber sequence, Slice value) const;
  std::vector<std::pair<std::string, std::string>> TakeRange(
      InternalStats *internal_stats, CompactionRouter *router, Slice smallest,
      Slice largest);

  const Comparator *ucmp_;
  mutable PCHashTable cache;
  mutable std::atomic<size_t> size_;
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
  bool Get(InternalStats *internal_stats, Slice user_key,
           PinnableSlice *value) const;
  void SwitchMutablePromotionCache(DBImpl &db, ColumnFamilyData &cfd,
                                   size_t write_buffer_size) const;
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
    // Data will be inserted back to this mutable promotion cache if there are
    // too few data to flush.
    const RWMutexProtected<MutablePromotionCache> *mut;
  };
  void checker();

  DBImpl &db_;
  const size_t target_level_;
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
