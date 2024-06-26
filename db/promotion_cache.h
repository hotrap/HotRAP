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

struct PromotedRange {
  SequenceNumber sequence;
  std::string first_user_key;
  std::string last_user_key;
};
class ComparePromotedRange {
 public:
  ComparePromotedRange(const Comparator *ucmp) : ucmp_(ucmp) {}
  bool operator()(const PromotedRange &a, const PromotedRange &b) const {
    return ucmp_->Compare(a.last_user_key, b.last_user_key) < 0;
  }

 private:
  const Comparator *ucmp_;
};

struct ParsedPromotedRange {
  SequenceNumber sequence;
  Slice first_user_key;
  Slice last_user_key;
};

class PromotionCache;
struct PCData {
  SequenceNumber sequence;
  std::string value;
  int count;
};
using PCHashTable = tbb::concurrent_hash_map<std::string, PCData>;

class UserKeyCompare {
 public:
  // Just for compatibility with std::map
  UserKeyCompare() { assert(false); }
  UserKeyCompare(const Comparator *ucmp) : ucmp_(ucmp) {}
  bool operator()(const std::string &lhs, const std::string &rhs) const {
    return ucmp_->Compare(lhs, rhs) < 0;
  }

 private:
  const Comparator *ucmp_;
};

class InternalKeyCompare {
 public:
  // Just for compatibility with std::map
  InternalKeyCompare() { assert(false); };
  InternalKeyCompare(const Comparator *ucmp) : icmp_(ucmp) {}
  bool operator()(const std::string &lhs, const std::string &rhs) const {
    return icmp_.Compare(lhs, rhs) < 0;
  }
  template <typename T>
  bool operator()(const std::pair<std::string, T> &lhs,
                  const std::pair<std::string, T> &rhs) const {
    return icmp_.Compare(lhs.first, rhs.first) < 0;
  }

  const InternalKeyComparator &icmp() const { return icmp_; }

 private:
  InternalKeyComparator icmp_;
};

struct RangeInfo {
  std::string first_user_key;
  SequenceNumber sequence;
  uint64_t num_bytes;
  uint64_t count;
};

struct ImmPromotionCache {
  std::unordered_map<std::string, PCData> cache;
  size_t size;
  MutexProtected<std::unordered_set<std::string>> updated;
  std::map<std::string, RangeInfo, UserKeyCompare> ranges;

  ImmPromotionCache(
      std::unordered_map<std::string, PCData> &&arg_cache, size_t arg_size,
      std::map<std::string, RangeInfo, UserKeyCompare> &&arg_ranges)
      : cache(std::move(arg_cache)),
        size(arg_size),
        ranges(std::move(arg_ranges)) {}
};
struct ImmPromotionCacheList {
  std::list<ImmPromotionCache> list;
  size_t size = 0;
};

struct RangeAccessRecord {
  Slice first_user_key;
  Slice last_user_key;
  SequenceNumber sequence;
};
class MutablePromotionCache {
 public:
  MutablePromotionCache() = delete;
  MutablePromotionCache(const Comparator *ucmp)
      : ucmp_(ucmp),
        size_(0),
        records_(InternalKeyCompare(ucmp_)),
        ranges_(UserKeyCompare(ucmp_)) {}
  MutablePromotionCache(const MutablePromotionCache &&rhs)
      : ucmp_(rhs.ucmp_),
        cache(std::move(rhs.cache)),
        size_(rhs.size_.load(std::memory_order_relaxed)),
        records_(InternalKeyCompare(ucmp_)),
        ranges_(UserKeyCompare(ucmp_)) {}

  // Return the size of the mutable promotion cache
  size_t Insert(InternalStats *internal_stats, Slice key, Slice value) const;
  size_t InsertRangeAccessRecords(
      std::vector<std::pair<std::string, std::string>> &&records,
      std::string &&first_user_key, std::string &&last_user_key,
      SequenceNumber sequence, uint64_t num_bytes);

  // [begin, end)
  std::vector<std::pair<std::string, std::string>> TakeRange(
      InternalStats *internal_stats, CompactionRouter *router, Slice smallest,
      Slice largest);

 private:
  void Insert(
      std::map<std::string, std::string, InternalKeyCompare>::iterator hint,
      std::pair<std::string, std::string> &&record) {
    size_.fetch_add(record.first.size() + record.second.size(),
                    std::memory_order_relaxed);
    records_.insert(hint, std::move(record));
  }

  void MergeRange(std::vector<std::pair<std::string, std::string>> &&records,
                  SequenceNumber sequence, Slice first_user_key);

  const Comparator *ucmp_;
  mutable PCHashTable cache;
  mutable std::atomic<size_t> size_;

  port::RWMutex mutex_;
  std::map<std::string, std::string, InternalKeyCompare> records_;
  // key: The last user key in the range
  std::map<std::string, RangeInfo, UserKeyCompare> ranges_;

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
