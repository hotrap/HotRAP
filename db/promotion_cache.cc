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
#include "db/job_context.h"
#include "db/lookup_key.h"
#include "db/version_set.h"
#include "logging/logging.h"
#include "monitoring/statistics.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {
// Returns the previous value
template <typename T>
T atomic_max_relaxed(std::atomic<T> &dst, T src) {
  T x = dst.load(std::memory_order_relaxed);
  while (src > x) {
    T expected = x;
    if (dst.compare_exchange_weak(expected, src)) break;
    x = dst.load(std::memory_order_relaxed);
  }
  return x;
}

PromotionCache::PromotionCache(DBImpl &db, int target_level,
                               const Comparator *ucmp)
    : db_(db),
      target_level_(target_level),
      ucmp_(ucmp),
      mut_{MutableCache{std::map<std::string, std::string, UserKeyCompare>{
                            UserKeyCompare(ucmp)},
                        0}},
      max_size_(0),
      should_stop_(false),
      checker_([this] { this->checker(); }) {}

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
bool PromotionCache::Get(InternalStats *internal_stats, Slice key,
                         PinnableSlice *value) const {
  auto timer_guard = internal_stats->hotrap_timers()
                         .timer(TimerType::kPromotionCacheGet)
                         .start();
  {
    auto mut = mut_.Read();
    // TODO: Avoid the copy here after upgrading to C++14
    auto it = mut->cache.find(key.ToString());
    if (it != mut->cache.end()) {
      if (value) value->PinSelf(it->second);
      return true;
    }
  }
  auto imm_list = imm_list_.Read();
  for (const ImmPromotionCache &imm : imm_list->list) {
    // TODO: Avoid the copy here after upgrading to C++14
    auto it = imm.cache.find(key.ToString());
    if (it != imm.cache.end()) {
      if (value) value->PinSelf(it->second);
      return true;
    }
  }
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
    InternalStats &internal_stats = *cfd->internal_stats();
    ImmPromotionCache &cache = *iter;
    auto stats = cfd->ioptions()->stats;
    TimerGuard check_newer_version_start =
        internal_stats.hotrap_timers()
            .timer(TimerType::kCheckNewerVersion)
            .start();
    for (const auto &item : cache.cache) {
      const std::string &user_key = item.first;
      LookupKey key(user_key, kMaxSequenceNumber);
      Status s;
      MergeContext merge_context;
      SequenceNumber max_covering_tombstone_seq;
      if (sv->imm->Get(key, nullptr, nullptr, &s, &merge_context,
                       &max_covering_tombstone_seq, ReadOptions())) {
        mark_updated(cache, user_key);
        continue;
      }
      if (!s.ok()) {
        ROCKS_LOG_FATAL(cfd->ioptions()->logger, "Unexpected error: %s\n",
                        s.ToString().c_str());
      }
      sv->current->Get(nullptr, ReadOptions(), key, nullptr, nullptr, &s,
                       &merge_context, &max_covering_tombstone_seq, nullptr,
                       nullptr, nullptr, nullptr, nullptr, false,
                       target_level_);
      if (!s.IsNotFound()) {
        if (!s.ok()) {
          ROCKS_LOG_FATAL(cfd->ioptions()->logger, "Unexpected error: %s\n",
                          s.ToString().c_str());
        }
        mark_updated(cache, user_key);
      }
    }
    // TODO: Is this really thread-safe?
    MemTable *m = cfd->ConstructNewMemtable(sv->mutable_cf_options, 0);
    m->Ref();
    autovector<MemTable *> memtables_to_free;
    SuperVersionContext svc(true);

    db->mutex()->Lock();
    m->SetNextLogNumber(db->logfile_number_);
    size_t flushed_bytes = 0;
    {
      auto updated = cache.updated.Lock();
      for (const auto &item : cache.cache) {
        const std::string &user_key = item.first;
        const std::string &value = item.second;
        if (updated->find(user_key) != updated->end()) {
          RecordTick(stats, Tickers::HAS_NEWER_VERSION_BYTES,
                     user_key.size() + value.size());
          continue;
        }
        // It seems that sequence number 0 is treated specially. So we use 1
        // here.
        Status s = m->Add(1, ValueType::kTypeValue, user_key, value, nullptr);
        if (!s.ok()) {
          ROCKS_LOG_FATAL(cfd->ioptions()->logger,
                          "check_newer_version: Unexpected error: %s",
                          s.ToString().c_str());
        }
        flushed_bytes += user_key.size() + value.size();
      }
    }
    cfd->imm()->Add(m, &memtables_to_free);
    db->InstallSuperVersionAndScheduleWork(cfd, &svc, sv->mutable_cf_options);
    DBImpl::FlushRequest flush_req;
    db->GenerateFlushRequest(autovector<ColumnFamilyData *>({cfd}), &flush_req);
    db->SchedulePendingFlush(flush_req, FlushReason::kPromotionCacheFull);
    db->MaybeScheduleFlushOrCompaction();
    db->mutex()->Unlock();

    for (MemTable *table : memtables_to_free) delete table;
    svc.Clean();
    RecordTick(stats, Tickers::PROMOTED_FLUSH_BYTES, flushed_bytes);

    {
      auto caches = cfd->promotion_caches().Read();
      auto it = caches->find(target_level_);
      assert(it->first == target_level_);
      auto list = it->second.imm_list().Write();
      list->size -= iter->size;
      list->list.erase(iter);
    }

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
void PromotionCache::Promote(DBImpl &db, ColumnFamilyData &cfd,
                             size_t write_buffer_size, std::string key,
                             Slice value) {
  {
    auto mut = mut_.Write();
    // TODO: Avoid requiring the ownership of key here after upgrading to C++14
    auto it = mut->cache.find(key);
    if (it != mut->cache.end()) return;
    mut->size += key.size() + value.size();
    size_t tot = mut->size + imm_list_.Read()->size;
    atomic_max_relaxed(max_size_, tot);
    auto ret =
        mut->cache.insert(std::make_pair(std::move(key), value.ToString()));
    (void)ret;
    assert(ret.second == true);
    if (mut->size < write_buffer_size) return;
  }
  db.mutex()->Lock();
  auto mut = mut_.Write();
  if (mut->size < write_buffer_size) {
    db.mutex()->Unlock();
    return;
  }
  SuperVersion *sv = cfd.GetSuperVersion();
  // check_newer_version is responsible to unref it
  sv->Ref();
  auto imm_list = imm_list_.Write();
  imm_list->size += mut->size;
  auto iter = imm_list->list.emplace(imm_list->list.end(),
                                     std::move(mut->cache), mut->size);
  mut->cache = std::map<std::string, std::string, UserKeyCompare>(
      cfd.ioptions()->user_comparator);
  mut->size = 0;
  db.mutex()->Unlock();

  size_t queue_len;
  {
    std::unique_lock<std::mutex> lock_(checker_lock_);
    checker_queue_.emplace(CheckerQueueElem{
        .db = &db,
        .sv = sv,
        .iter = iter,
    });
    queue_len = checker_queue_.size();
  }
  ROCKS_LOG_INFO(db.immutable_db_options().logger, "Checker queue length %zu\n",
                 queue_len);
  signal_check_.notify_one();
}
// [begin, end)
std::vector<std::pair<std::string, std::string>> PromotionCache::TakeRange(
    Slice smallest, Slice largest) {
  auto mut = mut_.Write();
  std::vector<std::pair<std::string, std::string>> ret;
  auto begin_it = mut->cache.lower_bound(smallest.ToString());
  auto it = begin_it;
  while (it != mut->cache.end() && ucmp_->Compare(it->first, largest) <= 0) {
    // TODO: Is it possible to avoid copying here?
    ret.emplace_back(it->first, it->second);
    mut->size -= it->first.size() + it->second.size();
    ++it;
  }
  mut->cache.erase(begin_it, it);
  return ret;
}
}  // namespace ROCKSDB_NAMESPACE
