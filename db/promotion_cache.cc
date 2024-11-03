#include "promotion_cache.h"

#include <bits/types/clockid_t.h>
#include <pthread.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <ctime>
#include <functional>
#include <ios>
#include <mutex>
#include <thread>

#include "column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/dbformat.h"
#include "db/internal_stats.h"
#include "db/job_context.h"
#include "db/lookup_key.h"
#include "db/version_set.h"
#include "logging/logging.h"
#include "monitoring/statistics.h"
#include "rocksdb/options.h"
#include "rocksdb/ralt.h"
#include "rocksdb/statistics.h"
#include "rocksdb/types.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

PromotionCache::PromotionCache(DBImpl &db, int target_level,
                               const Comparator *ucmp)
    : db_(db),
      target_level_(target_level),
      mut_(ucmp),
      should_stop_(false),
      checker_([this] { this->checker(); }),
      max_size_(0) {}

PromotionCache::~PromotionCache() {
  assert(should_stop_);
  if (checker_.joinable()) checker_.join();
}

void PromotionCache::stop_checker_no_wait() {
  db_.mutex()->AssertHeld();
  {
    std::unique_lock<std::mutex> lock(checker_lock_);
    should_stop_ = true;
  }
  signal_check_.notify_one();
  // Checker won't read the queue any more, so no need to lock here
  while (!checker_queue_.empty()) {
    CheckerQueueElem elem = checker_queue_.front();
    checker_queue_.pop();
    SuperVersion *sv = elem.sv;
    if (sv->Unref()) {
      sv->Cleanup();
      delete sv;
    }
  }
}
void PromotionCache::wait_for_checker_to_stop() { checker_.join(); }
bool PromotionCache::Get(InternalStats *internal_stats, Slice user_key,
                         PinnableSlice *value) const {
  {
    auto res = mut_.TryRead();
    if (!res.has_value()) return false;
    const auto &mut = res.value();
    PCHashTable::accessor it;
    if (mut->cache.find(it, user_key.ToString())) {
      if (value) value->PinSelf(it->second.value);
      it->second.count += 1;
      return true;
    }
  }
  // We shouldn't read immutable promotion caches here, because it's possible
  // that the newer version has been compacted into the slow disk while the old
  // version is in an immutable promotion cache.
  return false;
}

static inline uint64_t timestamp_ns() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}
static inline time_t cpu_timestamp_ns(clockid_t clockid) {
  struct timespec t;
  if (-1 == clock_gettime(clockid, &t)) {
    perror("clock_gettime");
    return 0;
  }
  return t.tv_sec * 1000000000 + t.tv_nsec;
}
static void print_stats_in_bg(std::atomic<bool> *should_stop,
                              std::string db_path, int target_level,
                              clockid_t clock_id) {
  std::string cputimes_path(db_path + "/checker-" +
                            std::to_string(target_level) + "-cputime");
  std::ofstream(cputimes_path) << "Timestamp(ns) cputime(ns)\n";
  time_t start_cpu_ts = cpu_timestamp_ns(clock_id);
  while (!should_stop->load(std::memory_order_relaxed)) {
    auto timestamp = timestamp_ns();

    std::ofstream(cputimes_path, std::ios_base::app)
        << timestamp << ' ' << cpu_timestamp_ns(clock_id) - start_cpu_ts
        << std::endl;

    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

static void mark_updated(ImmPromotionCache &cache,
                         const std::string &user_key) {
  auto updated = cache.updated.Lock();
  updated->insert(user_key);
}
// Will unref sv
void PromotionCache::checker() {
  pthread_setname_np(pthread_self(), "checker");

  struct RefHash {
    size_t operator()(std::reference_wrapper<const std::string> x) const {
      return std::hash<std::string>()(x.get());
    }
  };
  struct RefEq {
    bool operator()(std::reference_wrapper<const std::string> lhs,
                    std::reference_wrapper<const std::string> rhs) const {
      return lhs.get() == rhs.get();
    }
  };
  std::atomic<bool> printer_should_stop(false);
  clockid_t clock_id;
  pthread_getcpuclockid(pthread_self(), &clock_id);
  std::thread printer(print_stats_in_bg, &printer_should_stop,
                      db_.db_absolute_path_, target_level_, clock_id);
  for (;;) {
    CheckerQueueElem elem;
    {
      std::unique_lock<std::mutex> lock(checker_lock_);
      signal_check_.wait(
          lock, [&] { return should_stop_ || !checker_queue_.empty(); });
      if (should_stop_) break;
      elem = checker_queue_.front();
      checker_queue_.pop();
    }
    DBImpl *db = elem.db;
    assert(db == &db_);
    SuperVersion *sv = elem.sv;
    auto iter = elem.iter;
    ColumnFamilyData *cfd = sv->cfd;
    const auto &hotrap_timers = cfd->internal_stats()->hotrap_timers();
    ImmPromotionCache &cache = *iter;

    std::unordered_set<std::reference_wrapper<const std::string>, RefHash,
                       RefEq>
        stably_hot;
    RALT *ralt = sv->mutable_cf_options.ralt;
    const Comparator *ucmp = cfd->ioptions()->user_comparator;
    auto stats = cfd->ioptions()->stats;
    if (ralt) {
      TimerGuard check_stably_hot_start =
          hotrap_timers.timer(TimerType::kCheckStablyHot).start();
      for (const auto &item : cache.cache) {
        const std::string &user_key = item.first;
        bool is_stably_hot = item.second.count > 1 || ralt->IsHot(user_key);
        ralt->Access(item.first, item.second.value.size());
        if (is_stably_hot) {
          stably_hot.insert(user_key);
        } else {
          RecordTick(stats, Tickers::ACCESSED_COLD_BYTES,
                     user_key.size() + item.second.value.size());
        }
      }
    }
    TimerGuard check_newer_version_start =
        hotrap_timers.timer(TimerType::kCheckNewerVersion).start();
    for (const std::string &user_key : stably_hot) {
      LookupKey key(user_key, kMaxSequenceNumber);
      Status s;
      MergeContext merge_context;
      SequenceNumber max_covering_tombstone_seq = 0;
      if (sv->imm->Get(key, nullptr, nullptr, &s, &merge_context,
                       &max_covering_tombstone_seq, ReadOptions())) {
        mark_updated(cache, user_key);
        continue;
      }
      if (!s.ok()) {
        ROCKS_LOG_FATAL(cfd->ioptions()->logger, "Unexpected error: %s\n",
                        s.ToString().c_str());
      }
      if (sv->current->Get(nullptr, ReadOptions(), key, nullptr, nullptr, &s,
                           &merge_context, &max_covering_tombstone_seq, nullptr,
                           nullptr, nullptr, nullptr, nullptr, false,
                           target_level_)) {
        mark_updated(cache, user_key);
        continue;
      }
      assert(s.IsNotFound());
    }

    db->mutex()->Lock();
    // No need to SetNextLogNumber, because we don't delete any log file
    size_t bytes_to_flush = 0;
    {
      auto updated = cache.updated.Lock();
      // We are the only one who is accessing this imm PC, so we can modify it.
      auto it = cache.cache.begin();
      while (it != cache.cache.end()) {
        const std::string &user_key = it->first;
        const std::string &value = it->second.value;
        if (stably_hot.find(user_key) == stably_hot.end()) {
          it = cache.cache.erase(it);
          continue;
        }
        if (updated->find(user_key) != updated->end()) {
          RecordTick(stats, Tickers::HAS_NEWER_VERSION_BYTES,
                     user_key.size() + value.size());
          it = cache.cache.erase(it);
          continue;
        }
        bytes_to_flush += user_key.size() + value.size();
        ++it;
      }
    }
    if (bytes_to_flush * 2 < sv->mutable_cf_options.write_buffer_size) {
      // There are too few data to flush. There will be too much compaction I/O
      // in L1 if we force to flush them to L0. Therefore, we just insert them
      // back to the mutable promotion cache.
      TimerGuard start =
          hotrap_timers.timer(TimerType::kWriteBackToMutablePromotionCache)
              .start();
      auto mut = elem.pc->mut_.Write();
      for (const auto &item : cache.cache) {
        mut->Insert(std::string(item.first), item.second.sequence,
                    std::string(item.second.value));
      }
      db->mutex()->Unlock();
      elem.pc->ConsumeBuffer(mut);
    } else {
      RecordTick(stats, Tickers::PROMOTED_FLUSH_BYTES, bytes_to_flush);

      MemTable *m = cfd->ConstructNewMemtable(sv->mutable_cf_options, 0);
      m->Ref();
      autovector<MemTable *> memtables_to_free;
      SuperVersionContext svc(true);
      for (const auto &item : cache.cache) {
        Status s = m->Add(item.second.sequence, kTypeValue, item.first,
                          item.second.value, nullptr);
        if (!s.ok()) {
          ROCKS_LOG_FATAL(cfd->ioptions()->logger,
                          "check_newer_version: Unexpected error: %s",
                          s.ToString().c_str());
        }
      }
      cfd->imm()->Add(m, &memtables_to_free);
      db->InstallSuperVersionAndScheduleWork(cfd, &svc, sv->mutable_cf_options);
      DBImpl::FlushRequest flush_req;
      db->GenerateFlushRequest(autovector<ColumnFamilyData *>({cfd}),
                               &flush_req);
      db->SchedulePendingFlush(flush_req, FlushReason::kPromotionCacheFull);
      db->MaybeScheduleFlushOrCompaction();

      db->mutex()->Unlock();

      for (MemTable *table : memtables_to_free) delete table;
      svc.Clean();
    }

    std::list<ImmPromotionCache> tmp;
    {
      auto caches = cfd->promotion_caches().Read();
      auto it = caches->find(target_level_);
      assert(it->first == target_level_);
      auto list = it->second.imm_list().Write();
      list->size -= iter->size;
      tmp.splice(tmp.begin(), list->list, iter);
    }
    // tmp will be freed without holding the lock of imm_list

    if (sv->Unref()) {
      db->mutex()->Lock();
      sv->Cleanup();
      db->mutex()->Unlock();
      delete sv;
    }
  }
  printer_should_stop.store(true, std::memory_order_relaxed);
  printer.join();
}
size_t PromotionCache::Mutable::Insert(std::string &&user_key,
                                       SequenceNumber sequence,
                                       std::string &&value) const {
  size_t size;

  PCHashTable::accessor it;
  size_t kvsize = user_key.size() + value.size();
  if (cache.insert(it, std::move(user_key))) {
    it->second.sequence = sequence;
    it->second.value = std::move(value);
    it->second.count = 1;
    size = size_.fetch_add(kvsize, std::memory_order_relaxed) + kvsize;
  } else {
    size = size_.load(std::memory_order_relaxed);
  }
  return size;
}
std::vector<std::pair<std::string, std::string>>
PromotionCache::Mutable::TakeRange(InternalStats *internal_stats, RALT *ralt,
                                   Slice smallest, Slice largest) {
  auto guard =
      internal_stats->hotrap_timers().timer(TimerType::kTakeRange).start();
  std::vector<std::pair<InternalKey, std::pair<std::string, uint64_t>>>
      records_in_range;
  for (const auto &record : cache) {
    if (ucmp_->Compare(record.first, largest) <= 0 &&
        ucmp_->Compare(record.first, smallest) >= 0) {
      InternalKey key(record.first, record.second.sequence, kTypeValue);
      records_in_range.emplace_back(
          std::move(key),
          std::make_pair(record.second.value, record.second.count));
    }
  }
  std::vector<std::pair<std::string, std::string>> ret;
  for (auto &record : records_in_range) {
    InternalKey key = std::move(record.first);
    Slice user_key = key.user_key();
    std::string value = std::move(record.second.first);
    uint64_t count = record.second.second;
    assert(cache.erase(user_key.ToString()));
    size_.fetch_sub(key.size() + value.size(), std::memory_order_relaxed);
    bool is_hot = ralt->IsHot(user_key);
    ralt->Access(user_key, value.size());
    if (count > 1 || is_hot) {
      ret.emplace_back(std::move(*key.rep()), std::move(value));
    }
  }
  std::sort(ret.begin(), ret.end(), InternalKeyCompare(ucmp_));
  return ret;
}

// Return the mutable promotion size, or 0 if inserted to buffer.
size_t PromotionCache::Insert(std::string &&user_key, SequenceNumber sequence,
                              std::string &&value) const {
  auto mut = mut_.TryRead();
  if (!mut.has_value()) {
    mut_buffer_.Lock()->emplace_back(std::move(user_key), sequence,
                                     std::move(value));
    return 0;
  }
  return mut.value()->Insert(std::move(user_key), sequence, std::move(value));
}

void PromotionCache::ConsumeBuffer(WriteGuard<Mutable> &mut) const {
  auto mut_buffer = mut_buffer_.Lock();
  if (mut_buffer->empty()) return;
  std::vector<MutBufItem> buffer;
  std::swap(buffer, *mut_buffer);
  for (MutBufItem &item : buffer) {
    mut->Insert(std::move(item.user_key), item.seq, std::move(item.value));
  }
}

void PromotionCache::SwitchMutablePromotionCache(
    DBImpl &db, ColumnFamilyData &cfd, size_t write_buffer_size) const {
  std::unordered_map<std::string, PCData> cache;
  size_t mut_size;
  {
    auto start = cfd.internal_stats()
                     ->hotrap_timers()
                     .timer(TimerType::kSwitchMutablePromotionCache)
                     .start();
    auto mut = mut_.Write();
    mut_size = mut->size_.load(std::memory_order_relaxed);
    if (mut_size < write_buffer_size) {
      return;
    }
    for (auto &&a : mut->cache) {
      // Don't move keys in case that the hash map still needs it in clear().
      cache.emplace(a.first, std::move(a.second));
    }
    mut->cache.clear();
    mut->size_.store(0, std::memory_order_relaxed);
    ConsumeBuffer(mut);
  }
  db.mutex()->Lock();
  std::list<rocksdb::ImmPromotionCache>::iterator iter;
  {
    auto imm_list = imm_list_.Write();
    imm_list->size += mut_size;
    iter = imm_list->list.emplace(imm_list->list.end(), std::move(cache),
                                  mut_size);
  }
  SuperVersion *sv = cfd.GetSuperVersion();
  // check_newer_version is responsible to unref it
  sv->Ref();
  db.mutex()->Unlock();

  size_t queue_len;
  {
    std::unique_lock<std::mutex> lock_(checker_lock_);
    checker_queue_.emplace(CheckerQueueElem{
        .db = &db,
        .sv = sv,
        .iter = iter,
        .pc = this,
    });
    queue_len = checker_queue_.size();
  }
  ROCKS_LOG_INFO(db.immutable_db_options().logger, "Checker queue length %zu\n",
                 queue_len);
  signal_check_.notify_one();
}
std::vector<std::pair<std::string, std::string>> PromotionCache::TakeRange(
    InternalStats *internal_stats, RALT *ralt, Slice smallest,
    Slice largest) const {
  auto mut = mut_.Write();
  ConsumeBuffer(mut);
  return mut->TakeRange(internal_stats, ralt, smallest, largest);
}

}  // namespace ROCKSDB_NAMESPACE
