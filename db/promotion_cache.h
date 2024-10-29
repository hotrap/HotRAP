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
class PCData {
 public:
  PCData(std::deque<std::pair<SequenceNumber, std::string>> &&seq_value,
         bool repeated_accessed, bool only_by_point_query)
      : seq_value_(std::move(seq_value)),
        repeated_accessed_(repeated_accessed),
        only_by_point_query_(only_by_point_query) {
    assert(!seq_value_.empty());
  }

  const std::deque<std::pair<SequenceNumber, std::string>> &seq_value() const {
    return seq_value_;
  }
  std::deque<std::pair<SequenceNumber, std::string>> &seq_value() {
    return seq_value_;
  }
  bool only_by_point_query() const { return only_by_point_query_; }
  void set_only_by_point_query(bool v) { only_by_point_query_ = v; }
  bool repeated_accessed() const {
    return repeated_accessed_.load(std::memory_order_relaxed);
  }
  void set_repeated_accessed(bool v) const {
    repeated_accessed_.store(v, std::memory_order_relaxed);
  }

 private:
  // SequenceNumber from largest to smallest
  std::deque<std::pair<SequenceNumber, std::string>> seq_value_;
  mutable std::atomic<bool> repeated_accessed_;

  // Just for debugging
  bool only_by_point_query_;
};
struct ImmPCData {
  // SequenceNumber from largest to smallest
  std::deque<std::pair<SequenceNumber, std::string>> seq_value;
  bool only_by_point_query;
  bool repeated_accessed;
};

class UserKeyCompare {
 public:
  // Just for compatibility with std::map
  UserKeyCompare() : ucmp_(nullptr) {}
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

struct PromotedRange {
  std::string first_user_key;
  std::string last_user_key;
  SequenceNumber sequence;
  PromotedRange(const PromotedRange &a)
      : first_user_key(a.first_user_key),
        last_user_key(a.last_user_key),
        sequence(a.sequence) {}
  PromotedRange(std::string &&_first_user_key, std::string &&_last_user_key,
                SequenceNumber _sequence)
      : first_user_key(std::move(_first_user_key)),
        last_user_key(std::move(_last_user_key)),
        sequence(_sequence) {}
  PromotedRange(const std::string &_first_user_key,
                const std::string &_last_user_key, SequenceNumber _sequence)
      : first_user_key(_first_user_key),
        last_user_key(_last_user_key),
        sequence(_sequence) {}
};

struct PromotedRangeInfo {
  std::string first_user_key;
  SequenceNumber sequence;
  PromotedRangeInfo(std::string &&_first_user_key, SequenceNumber _sequence)
      : first_user_key(std::move(_first_user_key)), sequence(_sequence) {}
};

void InsertPromotedRanges(
    std::map<std::string, PromotedRangeInfo, UserKeyCompare> &promoted_ranges,
    const Comparator *ucmp,
    const std::vector<PromotedRange> &new_promoted_ranges);

struct RangeInfo {
  std::string first_user_key;
  SequenceNumber sequence;
  uint64_t num_bytes;
  uint64_t count;
};

struct ImmPromotionCache {
  std::unordered_map<std::string, ImmPCData> cache;
  MutexProtected<std::unordered_set<std::string>> updated;

  // Only accessed by the checker.
  size_t size;
  std::map<std::string, RangeInfo, UserKeyCompare> ranges;

  ImmPromotionCache(
      std::unordered_map<std::string, ImmPCData> &&arg_cache, size_t arg_size,
      std::map<std::string, RangeInfo, UserKeyCompare> &&arg_ranges)
      : cache(std::move(arg_cache)),
        size(arg_size),
        ranges(std::move(arg_ranges)) {}
};
struct ImmPromotionCacheList {
  std::list<ImmPromotionCache> list;
  size_t size = 0;
};

class MutablePromotionCache {
 public:
  MutablePromotionCache() = delete;
  MutablePromotionCache(const Comparator *ucmp)
      : ucmp_(ucmp), keys_(ucmp_), size_(0), ranges_(UserKeyCompare(ucmp_)) {}

  // Return the size of the mutable promotion cache
  size_t Insert(std::string &&user_key, SequenceNumber sequencd,
                std::string &&value);
  size_t InsertOneRange(
      std::vector<std::pair<std::string, std::string>> &&records,
      std::string &&first_user_key, std::string &&last_user_key,
      SequenceNumber sequence, uint64_t num_bytes);
  void InsertRanges(std::map<std::string, RangeInfo, UserKeyCompare> &&ranges,
                    std::vector<std::pair<std::string, ImmPCData>> &&keys);

  std::vector<std::pair<std::string, std::string>> TakeRange(
      std::vector<PromotedRange> &ranges, InternalStats *internal_stats,
      RALT *ralt, Slice smallest, Slice largest);

 private:
  // REQUIRES: it->first >= range1.first_user_key
  void MergeRange(
      std::map<std::string, RangeInfo, UserKeyCompare>::iterator &it,
      std::string &&new_range_last, RangeInfo &&new_range);
  void MergeOneKeyInRange(
      std::map<std::string, PCData, UserKeyCompare>::iterator &it,
      std::string &&user_key,
      std::deque<std::pair<SequenceNumber, std::string>> &&seq_values,
      SequenceNumber sequence);
  void MarkNotOnlyByPointQuery(
      std::map<std::string, PCData, UserKeyCompare>::iterator &it,
      Slice range_last);

  const Comparator *ucmp_;

  std::map<std::string, PCData, UserKeyCompare> keys_;
  size_t size_;
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

  const port::RWMutex &being_or_has_been_compacted_lock() const {
    return being_or_has_been_compacted_lock_;
  }
  const RWMutexProtected<MutablePromotionCache> &mut() const { return mut_; }
  size_t InsertToMut(std::string &&user_key, SequenceNumber sequence,
                     std::string &&value) const;
  void ConsumeBuffer(WriteGuard<MutablePromotionCache> &mut) const;

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
    const PromotionCache *pc;
  };
  void checker();
  void check(CheckerQueueElem &elem);

  DBImpl &db_;
  const size_t target_level_;

  // When inserting to the mutable promotion cache:
  // 1. Lock being_or_has_been_compacted_lock_
  // 2. Check being_or_has_been_compacted
  // 3. Lock and insert to mutable promotion cache. If fail to lock, then insert
  // to mut_buffer
  // 4. Unlock being_or_has_been_compacted_lock_
  //
  // When promote by compaction (TakeRange):
  // 1. Lock being_or_has_been_compacted_lock_
  // 2. Set being_or_has_been_compacted
  // 3. Unlock being_or_has_been_compacted_lock_
  // 4. Lock mutable promotion cache
  // 5. Consume mut_buffer_
  // 6. TakeRange
  // 7. Unlock mutable promotion cache
  //
  // If the insertion to the mutable promotion cache happens before 1, records
  // in the compaction range will be taken from the promotion cache.
  // If the insertion to the mutable promotion cache happens after 3, records
  // in the compaction range won't be inserted into the mutable pormotion cache.
  //
  // For other operations that locks the mutable promotion cache:
  // Consume mut_buffer_ before unlocking, so that the buffer won't grow too
  // big.
  mutable port::RWMutex being_or_has_been_compacted_lock_;
  struct MutBufItem {
    std::string user_key;
    SequenceNumber seq;
    std::string value;
    MutBufItem(std::string &&_user_key, SequenceNumber _seq,
               std::string &&_value)
        : user_key(std::move(_user_key)), seq(_seq), value(std::move(_value)) {}
  };
  MutexProtected<std::vector<MutBufItem>> mut_buffer_;
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
