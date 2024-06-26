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
#include "rocksdb/compaction_router.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/types.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

PromotionCache::PromotionCache(DBImpl &db, int target_level,
                               const Comparator *ucmp)
    : db_(db),
      target_level_(target_level),
      mut_(ucmp),
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
    CompactionRouter *router = sv->mutable_cf_options.compaction_router;
    const Comparator *ucmp = cfd->ioptions()->user_comparator;
    auto stats = cfd->ioptions()->stats;
    if (router) {
      TimerGuard check_stably_hot_start =
          hotrap_timers.timer(TimerType::kCheckStablyHot).start();
      if (cache.ranges.empty()) {
        for (const auto &item : cache.cache) {
          const std::string &user_key = item.first;
          bool is_stably_hot = item.second.count > 1 || router->IsHot(user_key);
          router->Access(item.first, item.second.value.size());
          if (is_stably_hot) {
            stably_hot.insert(user_key);
          } else {
            RecordTick(stats, Tickers::ACCESSED_COLD_BYTES,
                       user_key.size() + item.second.value.size());
          }
        }
      } else {
        std::vector<std::reference_wrapper<const std::string>> user_keys;
        {
          auto sort_start = hotrap_timers.timer(TimerType::kSort).start();
          for (const auto &item : cache.cache) {
            user_keys.push_back(item.first);
          }
          std::sort(user_keys.begin(), user_keys.end(), UserKeyCompare(ucmp));
        }
        auto it = user_keys.begin();
        for (const auto &range : cache.ranges) {
          if (range.second.count <= 1 &&
              !router->IsHot(range.second.first_user_key, range.first)) {
            router->AccessRange(range.second.first_user_key, range.first,
                                range.second.num_bytes, 0);
            RecordTick(stats, Tickers::ACCESSED_COLD_BYTES,
                       range.second.num_bytes);
          } else {
            // FIXME(jiansheng): AccessRange after flush
            router->AccessRange(range.second.first_user_key, range.first,
                                range.second.num_bytes, range.second.sequence);
            while (it != user_keys.end() &&
                   ucmp->Compare(it->get(), range.second.first_user_key) < 0) {
              ++it;
            }
            while (it != user_keys.end() &&
                   ucmp->Compare(it->get(), range.first) <= 0) {
              stably_hot.insert(*it);
              ++it;
            }
          }
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
        const std::string &value = item.second.value;
        if (stably_hot.find(user_key) == stably_hot.end()) continue;
        if (updated->find(user_key) != updated->end()) {
          RecordTick(stats, Tickers::HAS_NEWER_VERSION_BYTES,
                     user_key.size() + value.size());
          continue;
        }
        assert(item.second.sequence > 0);
        Status s =
            m->Add(item.second.sequence, kTypeValue, user_key, value, nullptr);
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

    std::list<ImmPromotionCache> tmp;
    {
      auto caches = cfd->promotion_caches().Read();
      auto it = caches->find(target_level_);
      assert((int)it->first == target_level_);
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
size_t MutablePromotionCache::Insert(InternalStats *internal_stats, Slice key,
                                     Slice value) const {
  size_t size;

  ParsedInternalKey ikey;
  Status s = ParseInternalKey(key, &ikey, false);
  assert(s.ok());

  PCHashTable::accessor it;
  if (cache.insert(it, ikey.user_key.ToString())) {
    it->second.sequence = ikey.sequence;
    it->second.value = value.ToString();
    it->second.count = 1;
    size_t add = key.size() + value.size();
    size = size_.fetch_add(add, std::memory_order_relaxed) + add;
  } else {
    size = size_.load(std::memory_order_relaxed);
  }
  return size;
}
size_t MutablePromotionCache::InsertRangeAccessRecords(
    std::vector<std::pair<std::string, std::string>> &&records,
    std::string &&first_user_key, std::string &&last_user_key,
    SequenceNumber sequence, uint64_t num_bytes) {
  auto it = ranges_.lower_bound(first_user_key);
  uint64_t count = 1;
  while (it != ranges_.end() &&
         ucmp_->Compare(it->second.first_user_key, last_user_key) <= 0) {
    // first <= range_last
    std::string range_first = std::move(it->second.first_user_key);
    std::string range_last = it->first;
    uint64_t range_num_bytes = it->second.num_bytes;
    SequenceNumber range_sequence = it->second.sequence;
    count += it->second.count;
    it = ranges_.erase(it);
    if (ucmp_->Compare(range_last, last_user_key) <= 0) {
      // range_last <= last
      if (ucmp_->Compare(range_first, first_user_key) < 0) {
        // range_first < first <= range_last <= last
        first_user_key = std::move(range_first);
        // Overestimation. Precise estimation requires maintaining length for
        // all accessed keys, which can consume much memory.
        num_bytes += range_num_bytes;
        sequence = std::max(sequence, range_sequence);
      }
    } else {
      // range_first <= last < range_last
      if (ucmp_->Compare(range_first, first_user_key) < 0) {
        // range_first < first <= last < range_last
        first_user_key = std::move(range_first);
        num_bytes = std::max(num_bytes, range_num_bytes);
      } else {
        // first <= range_first <= last < range_last
        num_bytes += range_num_bytes;
      }
      last_user_key = std::move(range_last);
      sequence = std::max(sequence, range_sequence);
    }
  }
  MergeRange(std::move(records), sequence, first_user_key);
  ranges_.emplace(std::move(last_user_key),
                  RangeInfo{
                      .first_user_key = std::move(first_user_key),
                      .sequence = sequence,
                      .num_bytes = num_bytes,
                      .count = count,
                  });
  return size_.load(std::memory_order_relaxed);
}

void MutablePromotionCache::MergeRange(
    std::vector<std::pair<std::string, std::string>> &&records,
    SequenceNumber sequence, Slice first_user_key) {
  InternalKey k;
  k.SetMinPossibleForUserKey(first_user_key);
  auto it1 = records_.lower_bound(*k.rep());
  auto icmp1 = records_.key_comp();
  const InternalKeyComparator &icmp = icmp1.icmp();
  ParsedInternalKey k1, k2;
  Slice cur_user_key("");
  for (auto it2 = records.begin(); it2 != records.end(); ++it2) {
    Status s = ParseInternalKey(it2->first, &k2, false);
    assert(s.ok());
    assert(k2.type == ValueType::kTypeValue);
    bool skip = false;
    while (it1 != records_.end()) {
      int res = icmp.Compare(it1->first, it2->first);
      if (res >= 0) {
        if (res == 0) skip = true;
        break;
      }
      s = ParseInternalKey(it1->first, &k1, false);
      assert(s.ok());
      assert(k1.type == ValueType::kTypeValue);
      if (k1.sequence > sequence) {
        ++it1;
        continue;
      }
      if (k1.user_key == cur_user_key) {
        // Stale version
        size_.fetch_sub(it1->first.size() + it1->second.size(),
                        std::memory_order_relaxed);
        it1 = records_.erase(it1);
        continue;
      }
      cur_user_key = k1.user_key;
      ++it1;
    }
    if (skip) continue;
    if (k2.sequence > sequence) {
      Insert(it1, std::move(*it2));
      continue;
    }
    if (k2.user_key == cur_user_key) {
      // Stale version
      continue;
    }
    cur_user_key = k2.user_key;
    Insert(it1, std::move(*it2));
  }
}

void PromotionCache::SwitchMutablePromotionCache(
    DBImpl &db, ColumnFamilyData &cfd, size_t write_buffer_size) const {
  std::unordered_map<std::string, PCData> cache;
  size_t mut_size;
  std::map<std::string, RangeInfo, UserKeyCompare> ranges(
      cfd.ioptions()->user_comparator);
  {
    auto mut = mut_.Write();
    mut_size = mut->size_.load(std::memory_order_relaxed);
    if (mut_size < write_buffer_size) {
      return;
    }
    assert(mut->cache.empty() || mut->records_.empty());
    for (auto &&a : mut->cache) {
      // Don't move keys in case that the hash map still needs it in clear().
      cache.emplace(a.first, std::move(a.second));
    }
    uint64_t size = 0;
    for (auto &&a : mut->records_) {
      size += a.first.size() + a.second.size();
      ParsedInternalKey ikey;
      Status s = ParseInternalKey(a.first, &ikey, false);
      assert(s.ok());
      auto ret = cache.emplace(ikey.user_key.ToString(),
                               PCData{.sequence = ikey.sequence,
                                      .value = std::move(a.second),
                                      .count = 1});
      // FIXME(jiansheng)
      assert(ret.second);
    }
    mut->records_.clear();
    uint64_t stored_size = mut->size_.load(std::memory_order_relaxed);
    assert(size == stored_size);
    mut->cache.clear();
    mut->size_.store(0, std::memory_order_relaxed);
    std::swap(ranges, mut->ranges_);
  }
  db.mutex()->Lock();
  std::list<rocksdb::ImmPromotionCache>::iterator iter;
  {
    auto imm_list = imm_list_.Write();
    imm_list->size += mut_size;
    iter = imm_list->list.emplace(imm_list->list.end(), std::move(cache),
                                  mut_size, std::move(ranges));
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
    });
    queue_len = checker_queue_.size();
  }
  ROCKS_LOG_INFO(db.immutable_db_options().logger, "Checker queue length %zu\n",
                 queue_len);
  signal_check_.notify_one();
}
// [begin, end)
std::vector<std::pair<std::string, std::string>>
MutablePromotionCache::TakeRange(InternalStats *internal_stats,
                                 CompactionRouter *router, Slice smallest,
                                 Slice largest) {
  auto guard =
      internal_stats->hotrap_timers().timer(TimerType::kTakeRange).start();
  std::vector<std::pair<InternalKey, std::pair<std::string, uint64_t>>>
      records_in_range;
  for (const auto &record : cache) {
    if (ucmp_->Compare(record.first, largest) < 0 &&
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
    bool should_promote = count > 1 || router->IsHot(user_key);
    router->Access(user_key, value.size());
    if (should_promote) {
      ret.emplace_back(std::move(*key.rep()), std::move(value));
    }
  }
  if (!ret.empty()) {
    // Mixture of point queries and range scans is not supported yet.
    assert(ranges_.empty());
  }
  auto it = ranges_.lower_bound(smallest.ToString());
  if (it != ranges_.end() &&
      ucmp_->Compare(it->second.first_user_key, smallest) < 0) {
    router->AccessRange(it->second.first_user_key, it->first,
                        it->second.num_bytes, 0);
    it = ranges_.erase(it);
  }
  InternalKey lookup_key;
  lookup_key.SetMinPossibleForUserKey(smallest);
  auto record_it = records_.lower_bound(*lookup_key.rep());
  while (it != ranges_.end()) {
    if (ucmp_->Compare(it->first, largest) > 0) break;
    if (it->second.count == 1 &&
        !router->IsHot(it->second.first_user_key, it->first)) {
      router->AccessRange(it->second.first_user_key, it->first,
                          it->second.num_bytes, 0);
    } else {
      // FIXME(jiansheng): AccessRange after installing results
      router->AccessRange(it->second.first_user_key, it->first,
                          it->second.num_bytes, it->second.sequence);
      while (record_it != records_.end()) {
        if (ucmp_->Compare(ExtractUserKey(record_it->first),
                           it->second.first_user_key) >= 0) {
          break;
        }
        size_.fetch_sub(record_it->first.size() + record_it->second.size(),
                        std::memory_order_relaxed);
        record_it = records_.erase(record_it);
      }
      if (record_it != records_.end()) {
        do {
          if (ucmp_->Compare(ExtractUserKey(record_it->first), it->first) > 0) {
            break;
          }
          // FIXME(jiansheng): Enforce retainment of this range
          ret.push_back(*record_it);
          size_.fetch_sub(record_it->first.size() + record_it->second.size());
          record_it = records_.erase(record_it);
        } while (record_it != records_.end());
      }
    }
    it = ranges_.erase(it);
  }
  if (it != ranges_.end() &&
      ucmp_->Compare(it->second.first_user_key, largest) <= 0) {
    router->AccessRange(it->second.first_user_key, it->first,
                        it->second.num_bytes, 0);
    it = ranges_.erase(it);
  }
  std::sort(ret.begin(), ret.end(), InternalKeyCompare(ucmp_));
  return ret;
}
}  // namespace ROCKSDB_NAMESPACE
