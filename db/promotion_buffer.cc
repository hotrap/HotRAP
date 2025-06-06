#include "promotion_buffer.h"

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
#include "rocksdb/options.h"
#include "rocksdb/ralt.h"
#include "rocksdb/statistics.h"
#include "rocksdb/types.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

PromotionBuffer::PromotionBuffer(DBImpl &db, ColumnFamilyData &cfd,
                                 int target_level, const Comparator *ucmp)
    : db_(db),
      cfd_(cfd),
      target_level_(target_level),
      mut_(ucmp),
      should_switch_(false),
      switcher_should_stop_(false),
      switcher_([this] { this->switcher(); }),
      checker_should_stop_(false),
      checker_([this] { this->checker(); }),
      max_size_(0) {
  read_options_.read_tier = kBlockCacheTier;
}

PromotionBuffer::~PromotionBuffer() {
  assert(switcher_should_stop_);
  if (switcher_.joinable()) switcher_.join();
  assert(checker_should_stop_);
  if (checker_.joinable()) checker_.join();
}

void PromotionBuffer::stop_checker_no_wait() {
  db_.mutex()->AssertHeld();
  {
    std::unique_lock<std::mutex> lock(switcher_lock_);
    switcher_should_stop_ = true;
  }
  switcher_signal_.notify_one();
  {
    std::unique_lock<std::mutex> lock(checker_lock_);
    checker_should_stop_ = true;
  }
  checker_signal_.notify_one();
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
void PromotionBuffer::wait_for_checker_to_stop() { checker_.join(); }
bool PromotionBuffer::Get(InternalStats *internal_stats, Slice user_key,
                          PinnableSlice *value) const {
  auto res = mut_.TryRead();
  if (!res.has_value()) return false;
  const auto &mut = res.value();
  PCHashTable::accessor it;
  if (mut->cache.find(it, user_key.ToString())) {
    if (value) value->PinSelf(it->second.value);
    it->second.count += 1;
    return true;
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

// Will unref sv
void PromotionBuffer::checker() {
  pthread_setname_np(pthread_self(), "checker");

  std::atomic<bool> printer_should_stop(false);
  clockid_t clock_id;
  pthread_getcpuclockid(pthread_self(), &clock_id);
  std::thread printer(print_stats_in_bg, &printer_should_stop, db_.dbname_,
                      target_level_, clock_id);
  for (;;) {
    CheckerQueueElem elem;
    {
      std::unique_lock<std::mutex> lock(checker_lock_);
      checker_signal_.wait(lock, [&] {
        return checker_should_stop_ || !checker_queue_.empty();
      });
      if (checker_should_stop_) break;
      elem = checker_queue_.front();
      checker_queue_.pop();
    }
    check(elem);
    if (elem.sv->Unref()) {
      db_.mutex()->Lock();
      elem.sv->Cleanup();
      db_.mutex()->Unlock();
      delete elem.sv;
    }
  }
  printer_should_stop.store(true, std::memory_order_relaxed);
  printer.join();
}

static void mark_updated(ImmPromotionBuffer &cache,
                         const std::string &user_key) {
  auto updated = cache.updated.Lock();
  updated->insert(user_key);
}
void PromotionBuffer::check(CheckerQueueElem &elem) {
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

  SuperVersion *sv = elem.sv;
  RALT *ralt = sv->mutable_cf_options.get_ralt();
  if (ralt == nullptr) return;

  auto iter = elem.iter;
  ColumnFamilyData *cfd = sv->cfd;
  const auto &hotrap_timers = cfd->internal_stats()->hotrap_timers();
  ImmPromotionBuffer &cache = *iter;

  std::unordered_set<std::reference_wrapper<const std::string>, RefHash, RefEq>
      stably_hot;
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
    if (sv->imm->Get(key, nullptr, nullptr, nullptr, &s, &merge_context,
                     &max_covering_tombstone_seq, read_options_)) {
      mark_updated(cache, user_key);
      continue;
    }
    if (!s.ok()) {
      ROCKS_LOG_FATAL(cfd->ioptions()->logger, "Unexpected error: %s\n",
                      s.ToString().c_str());
    }
    PinnedIteratorsManager pinned_iters_mgr;
    sv->current->Get(nullptr, read_options_, key, nullptr, nullptr, nullptr, &s,
                     &merge_context, &max_covering_tombstone_seq,
                     &pinned_iters_mgr, nullptr, nullptr, nullptr, nullptr,
                     nullptr, false, target_level_);
    if (s.ok() || s.IsIncomplete()) {
      mark_updated(cache, user_key);
      continue;
    }
    assert(s.IsNotFound());
  }

  db_.mutex()->Lock();
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
        hotrap_timers.timer(TimerType::kWriteBackToMutablePromotionBuffer)
            .start();
    auto mut = mut_.Write();
    for (const auto &item : cache.cache) {
      mut->Insert(std::string(item.first), item.second.sequence,
                  std::string(item.second.value));
    }
    db_.mutex()->Unlock();
    ConsumeBuffer(mut);
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
    db_.InstallSuperVersionAndScheduleWork(cfd, &svc, sv->mutable_cf_options);
    DBImpl::FlushRequest flush_req;
    db_.GenerateFlushRequest(autovector<ColumnFamilyData *>({cfd}),
                             FlushReason::kPromotionBufferFull, &flush_req);
    db_.SchedulePendingFlush(flush_req);
    db_.MaybeScheduleFlushOrCompaction();

    db_.mutex()->Unlock();

    for (MemTable *table : memtables_to_free) delete table;
    svc.Clean();
  }

  std::list<ImmPromotionBuffer> tmp;
  {
    auto list = imm_list_.Write();
    list->size -= iter->size;
    tmp.splice(tmp.begin(), list->list, iter);
  }
  // tmp will be freed without holding the lock of imm_list
}

size_t PromotionBuffer::Mutable::Insert(std::string &&user_key,
                                        SequenceNumber sequence,
                                        std::string &&value) const {
  size_t size;

  PCHashTable::accessor it;
  size_t kvsize = user_key.size() + value.size();
  bool inserted = cache.emplace(
      it, std::piecewise_construct, std::forward_as_tuple(std::move(user_key)),
      std::forward_as_tuple(sequence, std::move(value), 1));
  if (inserted) {
    size = size_.fetch_add(kvsize, std::memory_order_relaxed) + kvsize;
  } else {
    size = size_.load(std::memory_order_relaxed);
  }
  return size;
}
std::vector<std::pair<std::string, std::string>>
PromotionBuffer::Mutable::TakeRange(InternalStats *internal_stats, RALT *ralt,
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
void PromotionBuffer::Insert(const MutableCFOptions &mutable_cf_options,
                             std::string &&user_key, SequenceNumber sequence,
                             std::string &&value) const {
  size_t mut_size;
  {
    auto mut = mut_.TryRead();
    if (!mut.has_value()) {
      mut_buffer_.insert(std::move(user_key), sequence, std::move(value));
      return;
    }
    mut_size =
        mut.value()->Insert(std::move(user_key), sequence, std::move(value));
  }
  try_update_max_size(mut_size);
  if (mut_size < mutable_cf_options.write_buffer_size) return;
  ScheduleSwitchMut();
}

void PromotionBuffer::ConsumeBuffer(WriteGuard<Mutable> &mut) const {
  auto buffer = mut_buffer_.take_all();
  if (!buffer.has_value()) return;
  for (MutBufItem &item : buffer.value()) {
    mut->Insert(std::move(item.user_key), item.seq, std::move(item.value));
  }
}

void PromotionBuffer::try_update_max_size(size_t mut_size) const {
  // Try to update stats.
  auto imm_list = imm_list_.TryRead();
  if (!imm_list.has_value()) return;
  size_t tot = mut_size + imm_list.value()->size;
  rusty::intrinsics::atomic_max_relaxed(max_size_, tot);
}

void PromotionBuffer::ScheduleSwitchMut() const {
  {
    std::unique_lock<std::mutex> switcher_lock(switcher_lock_);
    if (should_switch_) return;
    should_switch_ = true;
  }
  auto start = cfd_.internal_stats()
                   ->hotrap_timers()
                   .timer(TimerType::kScheduleSwitchMut)
                   .start();
  switcher_signal_.notify_one();
}

void PromotionBuffer::SwitchMutablePromotionBuffer() {
  SuperVersion *sv;
  std::list<rocksdb::ImmPromotionBuffer>::iterator iter;
  size_t mut_size;
  {
    // We need to take snapshot before emptying mutable promotion cache to make
    // sure that a newer version from FD would either be in the snapshot and
    // detected by the checker, or invalidate the old version in the mutable
    // promotion cache during promotion by compaction.

    // We need to make sure that SwitchMemtable happens either before taking
    // snapshot or after the new immutable promotion cache is created.
    // We achieve this by protecting the operations with imm_list's lock.

    auto start = cfd_.internal_stats()
                     ->hotrap_timers()
                     .timer(TimerType::kSwitchMutablePromotionBuffer)
                     .start();

    db_.mutex()->Lock();
    auto imm_list = imm_list_.Write();
    sv = cfd_.GetSuperVersion();
    // Checker is responsible to unref it
    sv->Ref();
    db_.mutex()->Unlock();

    auto mut = mut_.Write();
    mut_size = mut->size_.load(std::memory_order_relaxed);
    if (mut_size < sv->mutable_cf_options.write_buffer_size) {
      sv->Unref();
      std::unique_lock<std::mutex> lock(switcher_lock_);
      assert(should_switch_);
      should_switch_ = false;
      return;
    }

    std::unordered_map<std::string, PCData> cache;
    for (auto &&a : mut->cache) {
      // Don't move keys in case that the hash map still needs it in clear().
      cache.emplace(a.first, std::move(a.second));
    }
    mut->cache.clear();
    mut->size_.store(0, std::memory_order_relaxed);

    imm_list->size += mut_size;
    iter = imm_list->list.emplace(imm_list->list.end(), std::move(cache),
                                  mut_size);
    imm_list.drop();

    {
      std::unique_lock<std::mutex> lock(switcher_lock_);
      assert(should_switch_);
      should_switch_ = false;
    }

    ConsumeBuffer(mut);
    mut_size = mut->size_.load(std::memory_order_relaxed);
  }
  if (mut_size >= sv->mutable_cf_options.write_buffer_size) {
    ScheduleSwitchMut();
  }

  size_t queue_len;
  {
    std::unique_lock<std::mutex> lock_(checker_lock_);
    checker_queue_.emplace(CheckerQueueElem{
        .sv = sv,
        .iter = iter,
    });
    queue_len = checker_queue_.size();
  }
  ROCKS_LOG_INFO(db_.immutable_db_options().logger,
                 "Checker queue length %zu\n", queue_len);
  checker_signal_.notify_one();
}
void PromotionBuffer::switcher() {
  pthread_setname_np(pthread_self(), "switcher");
  for (;;) {
    {
      std::unique_lock<std::mutex> lock(switcher_lock_);
      switcher_signal_.wait(
          lock, [&] { return switcher_should_stop_ || should_switch_; });
      if (switcher_should_stop_) break;
    }
    SwitchMutablePromotionBuffer();
  }
}

std::vector<std::pair<std::string, std::string>> PromotionBuffer::TakeRange(
    InternalStats *internal_stats, RALT *ralt, Slice smallest,
    Slice largest) const {
  auto mut = mut_.Write();
  // We need to consume the buffer so that all records inserted before marking
  // being_or_has_been_compacted can be seen by TakeRange.
  ConsumeBuffer(mut);
  return mut->TakeRange(internal_stats, ralt, smallest, largest);
}

}  // namespace ROCKSDB_NAMESPACE
