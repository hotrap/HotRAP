#pragma once

#include <atomic>
#include <condition_variable>
#include <map>
#include <mutex>
#include <queue>
#include <unordered_set>

#include "db/dbformat.h"
#include "monitoring/instrumented_mutex.h"
#include "options/cf_options.h"
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

class PromotionBuffer;
struct PCData {
  SequenceNumber sequence;
  std::string value;
  int count{0};
  PCData(SequenceNumber _sequence, std::string &&_value, int _count)
      : sequence(_sequence), value(std::move(_value)), count(_count) {}
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

struct ImmPromotionBuffer {
  std::unordered_map<std::string, PCData> cache;
  size_t size;
  MutexProtected<std::unordered_set<std::string>> updated;
  ImmPromotionBuffer(std::unordered_map<std::string, PCData> &&arg_cache,
                     size_t arg_size)
      : cache(std::move(arg_cache)), size(arg_size) {}
};
struct ImmPromotionBufferList {
  std::list<ImmPromotionBuffer> list;
  size_t size = 0;
};

class PromotionBuffer {
 public:
  PromotionBuffer(DBImpl &db, ColumnFamilyData &cfd, int target_level,
                  const Comparator *ucmp);
  PromotionBuffer(const PromotionBuffer &) = delete;
  PromotionBuffer &operator=(const PromotionBuffer &) = delete;
  ~PromotionBuffer();

  // Should be called with db mutex held
  void stop_checker_no_wait();
  // Not thread-safe
  void wait_for_checker_to_stop();

  bool Get(InternalStats *internal_stats, Slice user_key,
           PinnableSlice *value) const;

  const port::RWMutex &being_or_has_been_compacted_lock() const {
    return being_or_has_been_compacted_lock_;
  }
  void Insert(const MutableCFOptions &mutable_cf_options,
              std::string &&user_key, SequenceNumber sequence,
              std::string &&value) const;

  std::vector<std::pair<std::string, std::string>> TakeRange(
      InternalStats *internal_stats, RALT *ralt, Slice smallest,
      Slice largest) const;

  const RWMutexProtected<ImmPromotionBufferList> &imm_list() const {
    return imm_list_;
  }

  // For statistics
  size_t max_size() const { return max_size_.load(std::memory_order_relaxed); }
  size_t mut_buffer_max_size() const { return mut_buffer_.max_size(); }

 private:
  class Mutable {
   public:
    Mutable(const Comparator *ucmp) : ucmp_(ucmp), size_(0) {}
    Mutable(const Mutable &&rhs)
        : ucmp_(rhs.ucmp_),
          cache(std::move(rhs.cache)),
          size_(rhs.size_.load(std::memory_order_relaxed)) {}

    // Return the size of the mutable promotion cache
    size_t Insert(std::string &&user_key, SequenceNumber sequence,
                  std::string &&value) const;

    std::vector<std::pair<std::string, std::string>> TakeRange(
        InternalStats *internal_stats, RALT *ralt, Slice smallest,
        Slice largest);

   private:
    const Comparator *ucmp_;
    mutable PCHashTable cache;
    mutable std::atomic<size_t> size_;
    friend class PromotionBuffer;
  };

  void ConsumeBuffer(WriteGuard<Mutable> &mut) const;

  void try_update_max_size(size_t mut_size) const;

  void ScheduleSwitchMut() const;
  struct CheckerQueueElem {
    SuperVersion *sv;
    std::list<ImmPromotionBuffer>::iterator iter;
  };
  void SwitchMutablePromotionBuffer();
  void switcher();
  void checker();
  void check(CheckerQueueElem &elem);

  DBImpl &db_;
  ColumnFamilyData &cfd_;
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
  class MutBuffer {
   public:
    void insert(std::string &&user_key, SequenceNumber seq,
                std::string &&value) const {
      std::unique_lock<std::mutex> lock(lock_);
      size_ += user_key.size() + value.size();
      max_size_ = std::max(max_size_, size_);
      buffer_.emplace_back(std::move(user_key), seq, std::move(value));
    }
    std::optional<std::vector<MutBufItem>> take_all() const {
      std::unique_lock<std::mutex> lock(lock_);
      if (buffer_.empty()) return std::nullopt;
      std::vector<MutBufItem> ret;
      std::swap(ret, buffer_);
      size_ = 0;
      return std::make_optional(std::move(ret));
    }
    size_t max_size() const {
      std::unique_lock<std::mutex> lock(lock_);
      return max_size_;
    }

   private:
    mutable std::mutex lock_;
    mutable std::vector<MutBufItem> buffer_;
    mutable size_t size_ = 0;
    mutable size_t max_size_ = 0;
  };
  MutBuffer mut_buffer_;
  RWMutexProtected<Mutable> mut_;

  RWMutexProtected<ImmPromotionBufferList> imm_list_;

  mutable std::mutex switcher_lock_;
  mutable bool should_switch_;
  mutable std::condition_variable switcher_signal_;
  bool switcher_should_stop_;
  std::thread switcher_;

  mutable std::mutex checker_lock_;
  mutable std::queue<CheckerQueueElem> checker_queue_;
  mutable std::condition_variable checker_signal_;
  bool checker_should_stop_;
  std::thread checker_;

  ReadOptions read_options_;

  // For statistics
  mutable std::atomic<size_t> max_size_;
};
}  // namespace ROCKSDB_NAMESPACE
