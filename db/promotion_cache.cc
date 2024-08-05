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
    auto it = mut->keys_.find(user_key.ToString());
    if (it != mut->keys_.end()) {
      if (value) value->PinSelf(it->second.seq_value().front().second);
      it->second.set_repeated_accessed(true);
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
    check(elem);
    if (elem.sv->Unref()) {
      elem.db->mutex()->Lock();
      elem.sv->Cleanup();
      elem.db->mutex()->Unlock();
      delete elem.sv;
    }
  }
  printer_should_stop.store(true, std::memory_order_relaxed);
  printer.join();
}
void PromotionCache::check(CheckerQueueElem &elem) {
  SuperVersion *sv = elem.sv;
  CompactionRouter *router = sv->mutable_cf_options.compaction_router;
  if (router == nullptr) return;

  DBImpl *db = elem.db;
  assert(db == &db_);
  auto iter = elem.iter;
  ColumnFamilyData *cfd = sv->cfd;
  const auto &hotrap_timers = cfd->internal_stats()->hotrap_timers();
  ImmPromotionCache &cache = *iter;

  std::vector<std::unordered_map<std::string, ImmPCData>::const_iterator>
      candidates, to_promote;
  const Comparator *ucmp = cfd->ioptions()->user_comparator;
  auto stats = cfd->ioptions()->stats;

  TimerGuard check_stably_hot_start =
      hotrap_timers.timer(TimerType::kCheckStablyHot).start();
  for (auto it = cache.cache.begin(); it != cache.cache.end(); ++it) {
    candidates.push_back(it);
  }
  {
    auto sort_start = hotrap_timers.timer(TimerType::kSort).start();
    std::sort(
        candidates.begin(), candidates.end(),
        [ucmp](std::unordered_map<std::string, ImmPCData>::const_iterator a,
               std::unordered_map<std::string, ImmPCData>::const_iterator b) {
          return ucmp->Compare(a->first, b->first) < 0;
        });
  }
  auto key_it = candidates.begin();
  auto check_key_until = [&to_promote, &key_it, &candidates, ucmp, router,
                          &stats](Slice range_first) {
    while (key_it != candidates.end()) {
      const std::string &user_key = (*key_it)->first;
      if (range_first.data()) {
        if (ucmp->Compare(user_key, range_first) >= 0) break;
      }
      const auto &data = (*key_it)->second;
      assert(data.only_by_point_query);
      const auto &seq_value = data.seq_value;
      assert(seq_value.size() == 1);
      const std::string &value = seq_value[0].second;
      bool should_promote = data.repeated_accessed || router->IsHot(user_key);
      router->Access(user_key, value.size());
      if (should_promote) {
        to_promote.push_back(*key_it);
      } else {
        RecordTick(stats, Tickers::ACCESSED_COLD_BYTES,
                   user_key.size() + value.size());
      }
      ++key_it;
    }
  };
  // We are the only one who accesses cache.ranges, so we can modify it.
  auto range_it = cache.ranges.begin();
  while (range_it != cache.ranges.end()) {
    const std::string &range_first = range_it->second.first_user_key;
    const std::string &range_last = range_it->first;
    uint64_t num_bytes = range_it->second.num_bytes;
    check_key_until(range_first);
    assert(range_it->second.count > 0);
    if (range_it->second.count == 1 &&
        !router->IsHot(range_first, range_last)) {
      router->AccessRange(range_first, range_last, num_bytes, 0);
      RecordTick(stats, Tickers::ACCESSED_COLD_BYTES, num_bytes);
      while (key_it != candidates.end() &&
             ucmp->Compare((*key_it)->first, range_last) <= 0) {
        assert(!(*key_it)->second.only_by_point_query);
        ++key_it;
      }
      range_it = cache.ranges.erase(range_it);
    } else {
      // FIXME(hotrap): AccessRange after flush
      router->AccessRange(range_first, range_last, num_bytes,
                          range_it->second.sequence);
      while (key_it != candidates.end() &&
             ucmp->Compare((*key_it)->first, range_last) <= 0) {
        assert(!(*key_it)->second.only_by_point_query);
        to_promote.push_back(*key_it);
        ++key_it;
      }
      ++range_it;
    }
  }
  check_key_until(Slice(nullptr, 0));
  std::swap(candidates, to_promote);
  to_promote.clear();

  TimerGuard check_newer_version_start =
      hotrap_timers.timer(TimerType::kCheckNewerVersion).start();
  // FIXME(hotrap): Don't promote the corresponding range if a record is not
  // promoted due to a newer version.
  for (auto it : candidates) {
    LookupKey key(it->first, kMaxSequenceNumber);
    Status s;
    MergeContext merge_context;
    SequenceNumber max_covering_tombstone_seq = 0;
    if (sv->imm->Get(key, nullptr, nullptr, &s, &merge_context,
                     &max_covering_tombstone_seq, ReadOptions())) {
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
      continue;
    }
    assert(s.IsNotFound());
    to_promote.push_back(it);
  }
  std::swap(candidates, to_promote);
  to_promote.clear();

  db->mutex()->Lock();

  std::list<ImmPromotionCache> tmp;
  {
    auto caches = cfd->promotion_caches().Read();
    auto caches_it = caches->find(target_level_);
    assert(caches_it != caches->end());
    assert((int)caches_it->first == target_level_);
    auto list = caches_it->second.imm_list().Write();
    list->size -= iter->size;
    tmp.splice(tmp.begin(), list->list, iter);
  }
  // The immutable promotion cache will be freed at last without holding the
  // lock of imm_list

  size_t bytes_to_flush = 0;
  {
    auto updated = cache.updated.Lock();
    for (auto it : candidates) {
      const std::string &user_key = it->first;
      size_t size = 0;
      for (const auto &seq_value : it->second.seq_value) {
        size += user_key.size() + seq_value.second.size();
      }
      if (updated->find(user_key) != updated->end()) {
        RecordTick(stats, Tickers::HAS_NEWER_VERSION_BYTES, size);
        continue;
      }
      bytes_to_flush += size;
      to_promote.push_back(it);
    }
  }
  if (bytes_to_flush * 2 < sv->mutable_cf_options.write_buffer_size) {
    // There are too few data to flush. There will be too much compaction I/O
    // in L1 if we force to flush them to L0. Therefore, we just insert them
    // back to the mutable promotion cache.
    std::vector<std::pair<std::string, ImmPCData>> keys;
    for (auto it : to_promote) {
      keys.push_back(std::move(*it));
    }

    auto mut = elem.mut->Write();
    mut->InsertRanges(std::move(cache.ranges), std::move(keys));
    db->mutex()->Unlock();
  } else {
    RecordTick(stats, Tickers::PROMOTED_FLUSH_BYTES, bytes_to_flush);

    // No need to SetNextLogNumber, because we don't delete any log file
    MemTable *m = cfd->ConstructNewMemtable(sv->mutable_cf_options, 0);
    m->Ref();
    autovector<MemTable *> memtables_to_free;
    SuperVersionContext svc(true);
    for (auto it : to_promote) {
      for (const auto &seq_value : it->second.seq_value) {
        assert(seq_value.first > 0);
        Status s = m->Add(seq_value.first, kTypeValue, it->first,
                          seq_value.second, nullptr);
        if (!s.ok()) {
          ROCKS_LOG_FATAL(cfd->ioptions()->logger,
                          "check_newer_version: Unexpected error: %s",
                          s.ToString().c_str());
        }
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
  }
}

size_t MutablePromotionCache::Insert(Slice user_key, SequenceNumber sequence,
                                     Slice value) {
  auto it = keys_.lower_bound(user_key.ToString());
  if (it == keys_.end() || ucmp_->Compare(it->first, user_key) != 0) {
    // It's definitely not contained by a range.
    auto range_it = ranges_.lower_bound(user_key.ToString());
    assert(range_it == ranges_.end() ||
           ucmp_->Compare(range_it->second.first_user_key, user_key) > 0);

    size_ += user_key.size() + value.size();
    std::deque<std::pair<SequenceNumber, std::string>> seq_value{
        {sequence, value.ToString()}};
    keys_.emplace_hint(it, std::piecewise_construct,
                       std::forward_as_tuple(user_key.ToString()),
                       std::forward_as_tuple(std::move(seq_value),
                                             /*repeated_accessed=*/false,
                                             /*only_by_point_query=*/true));
  } else {
    if (it->second.only_by_point_query()) {
      const auto &seq_value = it->second.seq_value();
      assert(seq_value.size() == 1);
      assert(seq_value[0].first == sequence);
    }
    it->second.set_repeated_accessed(true);
  }
  return size_;
}
size_t MutablePromotionCache::Insert(Slice internal_key, Slice value) {
  ParsedInternalKey ikey;
  Status s = ParseInternalKey(internal_key, &ikey, false);
  assert(s.ok());
  return Insert(ikey.user_key, ikey.sequence, value);
}

size_t MutablePromotionCache::InsertOneRange(
    std::vector<std::pair<std::string, std::string>> &&records,
    std::string &&first_user_key, std::string &&last_user_key,
    SequenceNumber sequence, uint64_t num_bytes) {
  auto it = ranges_.lower_bound(first_user_key);
  RangeInfo range{
      .first_user_key = std::move(first_user_key),
      .sequence = sequence,
      .num_bytes = num_bytes,
      .count = 1,
  };
  MergeRange(it, std::move(last_user_key), std::move(range));
  sequence = it->second.sequence;
  const std::string &new_range_first = it->second.first_user_key;
  const std::string &new_range_last = it->first;
  if (!records.empty()) {
    assert(ucmp_->Compare(ExtractUserKey(records.front().first),
                          new_range_first) >= 0);
    assert(ucmp_->Compare(ExtractUserKey(records.back().first),
                          new_range_last) <= 0);
  }

  auto record_it = records.begin();
  if (record_it == records.end()) return size_;
  ParsedInternalKey ikey;
  Status s = ParseInternalKey(record_it->first, &ikey, false);
  assert(s.ok());
  std::string user_key = ikey.user_key.ToString();
  std::deque<std::pair<SequenceNumber, std::string>> seq_value;
  auto key_it = keys_.lower_bound(user_key);
  for (;;) {
    seq_value.emplace_back(ikey.sequence, std::move(record_it->second));
    ++record_it;
    if (record_it != records.end()) {
      s = ParseInternalKey(record_it->first, &ikey, false);
      assert(s.ok());
      if (ucmp_->Compare(ikey.user_key, user_key) == 0) continue;
    }
    MergeOneKeyInRange(key_it, std::move(user_key), std::move(seq_value),
                       sequence);
    if (record_it == records.end()) break;
    user_key = ikey.user_key.ToString();
    seq_value = std::deque<std::pair<SequenceNumber, std::string>>();
  }
  return size_;
}
void MutablePromotionCache::InsertRanges(
    std::map<std::string, RangeInfo, UserKeyCompare> &&ranges,
    std::vector<std::pair<std::string, ImmPCData>> &&keys) {
  auto key_it = keys_.begin();
  auto key_it1 = keys.begin();
  auto range_it = ranges_.begin();
  auto range1_it = ranges.begin();
  auto insert_keys_until = [this, &key_it, &key_it1, &keys](Slice range_first) {
    while (key_it1 != keys.end()) {
      if (range_first.data()) {
        if (ucmp_->Compare(key_it1->first, range_first) >= 0) break;
      }
      std::string &&user_key = std::move(key_it1->first);
      assert(key_it1->second.only_by_point_query);
      auto seq_value = std::move(key_it1->second.seq_value);
      assert(seq_value.size() == 1);
      while (key_it != keys_.end() &&
             ucmp_->Compare(key_it->first, user_key) < 0) {
        // key_it may still be in the last range, so its only_by_point_query can
        // be false here.
        ++key_it;
      }
      if (key_it == keys_.end() ||
          ucmp_->Compare(key_it->first, user_key) > 0) {
        size_ += user_key.size() + seq_value[0].second.size();
        keys_.emplace_hint(
            key_it, std::piecewise_construct,
            std::forward_as_tuple(std::move(user_key)),
            std::forward_as_tuple(std::move(seq_value),
                                  key_it1->second.repeated_accessed,
                                  /*only_by_point_query=*/true));
      } else {
        assert(key_it->second.only_by_point_query());
        key_it->second.set_repeated_accessed(true);
      }
      ++key_it1;
    }
  };
  while (range1_it != ranges.end()) {
    std::string range1_last = range1_it->first;
    RangeInfo range1 = std::move(range1_it->second);
    range1_it = ranges.erase(range1_it);
    while (range_it != ranges_.end() &&
           ucmp_->Compare(range_it->first, range1.first_user_key) < 0) {
      ++range_it;
    }
    MergeRange(range_it, std::move(range1_last), std::move(range1));
    const std::string &new_range_first = range_it->second.first_user_key;
    const std::string &new_range_last = range_it->first;
    SequenceNumber new_range_sequence = range_it->second.sequence;
    insert_keys_until(new_range_first);
    while (key_it1 != keys.end() &&
           ucmp_->Compare(key_it1->first, new_range_last) <= 0) {
      assert(!key_it1->second.only_by_point_query);
      MergeOneKeyInRange(key_it, std::move(key_it1->first),
                         std::move(key_it1->second.seq_value),
                         new_range_sequence);
      ++key_it1;
    }
  }
  insert_keys_until(Slice(nullptr, 0));
}

void MutablePromotionCache::MergeRange(
    std::map<std::string, RangeInfo, UserKeyCompare>::iterator &it,
    std::string &&range1_last, RangeInfo &&range1) {
  if (it != ranges_.end()) {
    assert(ucmp_->Compare(it->first, range1.first_user_key) >= 0);
    if (it != ranges_.begin()) {
      auto tmp = it;
      --tmp;
      assert(ucmp_->Compare(tmp->first, it->second.first_user_key) < 0);
    }
  }
  while (it != ranges_.end() &&
         ucmp_->Compare(it->second.first_user_key, range1_last) <= 0) {
    std::string &&range_first = std::move(it->second.first_user_key);
    // range_first <= range1_last
    std::string range_last = it->first;
    uint64_t range_num_bytes = it->second.num_bytes;
    SequenceNumber range_sequence = it->second.sequence;
    range1.count += it->second.count;
    if (ucmp_->Compare(range_last, range1_last) <= 0) {
      // range_last <= range1_last
      if (ucmp_->Compare(range_first, range1.first_user_key) < 0) {
        // range_first < range1_first <= range_last <= range1_last
        range1.first_user_key = std::move(range_first);
        // Overestimation. Precise estimation requires maintaining length for
        // all accessed keys, which can consume much memory.
        range1.num_bytes += range_num_bytes;
        range1.sequence = std::max(range1.sequence, range_sequence);
      } else {
        // range1_first <= range_first <= range_last <= range1_last
      }
    } else {
      // range_first <= range1_last < range_last
      if (ucmp_->Compare(range_first, range1.first_user_key) < 0) {
        // range_first < range1_first <= range1_last < range_last
        range1.first_user_key = std::move(range_first);
        range1.num_bytes = std::max(range1.num_bytes, range_num_bytes);
        range1.sequence = range_sequence;
      } else {
        // range1_first <= range_first <= range1_last < range_last
        range1.num_bytes += range_num_bytes;
        range1.sequence = std::max(range1.sequence, range_sequence);
      }
      range1_last = std::move(range_last);
    }
    it = ranges_.erase(it);
  }
  if (it != ranges_.end()) {
    assert(ucmp_->Compare(range1_last, it->second.first_user_key) < 0);
  }
  if (it != ranges_.begin()) {
    auto tmp = it;
    --tmp;
    assert(ucmp_->Compare(tmp->first, range1.first_user_key) < 0);
  }
  it = ranges_.emplace_hint(it, std::move(range1_last), std::move(range1));
}
void MutablePromotionCache::MergeOneKeyInRange(
    std::map<std::string, PCData, UserKeyCompare>::iterator &it,
    std::string &&user_key,
    std::deque<std::pair<SequenceNumber, std::string>> &&seq_values,
    SequenceNumber sequence) {
  while (it != keys_.end() && ucmp_->Compare(it->first, user_key) < 0) {
    ++it;
  }
  if (it == keys_.end() || ucmp_->Compare(it->first, user_key) > 0) {
    for (const auto &seq_value : seq_values) {
      size_ += user_key.size() + seq_value.second.size();
    }
    keys_.emplace_hint(it, std::piecewise_construct,
                       std::forward_as_tuple(std::move(user_key)),
                       std::forward_as_tuple(std::move(seq_values),
                                             /*repeated_accessed=*/false,
                                             /*only_by_point_query=*/false));
  } else {
    auto &seq_value = it->second.seq_value();
    assert(!seq_value.empty());
    auto it1 = seq_values.begin();
    // Insert new versions
    while (it1 != seq_values.end() && it1->first > seq_value.front().first) {
      size_ += user_key.size() + it1->second.size();
      seq_value.emplace_front(it1->first, std::move(it1->second));
      ++it1;
    }
    // Skip versions we already have
    while (it1 != seq_values.end() && it1->first >= seq_value.back().first) {
      ++it1;
    }
    // Drop stale versions
    while (seq_value.size() >= 2 && seq_value.back().first < sequence) {
      size_ -= user_key.size() + seq_value.back().second.size();
      seq_value.pop_back();
    }
    // Insert versions we don't have (possible if this key was inserted by a
    // point query)
    while (it1 != seq_values.end() && it1->first >= sequence) {
      assert(it->second.only_by_point_query());
      size_ += user_key.size() + it1->second.size();
      seq_value.emplace_back(it1->first, std::move(it1->second));
      ++it1;
    }
    it->second.set_repeated_accessed(true);
    it->second.set_only_by_point_query(false);
  }
}

void PromotionCache::SwitchMutablePromotionCache(
    DBImpl &db, ColumnFamilyData &cfd, size_t write_buffer_size) const {
  std::unordered_map<std::string, ImmPCData> cache;
  size_t mut_size;
  std::map<std::string, RangeInfo, UserKeyCompare> ranges(
      cfd.ioptions()->user_comparator);
  {
    auto mut = mut_.Write();
    mut_size = mut->size_;
    if (mut_size < write_buffer_size) {
      return;
    }
    uint64_t size = 0;
    for (auto &&a : mut->keys_) {
      for (const auto &seq_value : a.second.seq_value()) {
        size += a.first.size() + seq_value.second.size();
      }
      auto ret = cache.emplace(
          std::move(a.first),
          ImmPCData{
              .seq_value = std::move(a.second.seq_value()),
              .only_by_point_query = a.second.only_by_point_query(),
              .repeated_accessed = a.second.repeated_accessed(),
          });
      assert(ret.second);
    }
    mut->keys_.clear();
    assert(size == mut->size_);
    mut->size_ = 0;
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
        .mut = &mut_,
    });
    queue_len = checker_queue_.size();
  }
  ROCKS_LOG_INFO(db.immutable_db_options().logger, "Checker queue length %zu\n",
                 queue_len);
  signal_check_.notify_one();
}
std::vector<std::pair<std::string, std::string>>
MutablePromotionCache::TakeRange(InternalStats *internal_stats,
                                 CompactionRouter *router, Slice smallest,
                                 Slice largest) {
  auto guard =
      internal_stats->hotrap_timers().timer(TimerType::kTakeRange).start();
  std::vector<std::pair<std::string, std::string>> ret;
  auto range_it = ranges_.lower_bound(smallest.ToString());
  std::map<std::string, PCData, UserKeyCompare>::iterator key_it;
  auto erase_keys_in_range = [this, &key_it](Slice range_last) {
    while (key_it != keys_.end()) {
      const std::string &user_key = key_it->first;
      if (ucmp_->Compare(user_key, range_last) > 0) break;
      assert(!key_it->second.only_by_point_query());
      for (const auto &seq_value : key_it->second.seq_value()) {
        size_ -= user_key.size() + seq_value.second.size();
      }
      key_it = keys_.erase(key_it);
    }
  };
  auto erase_keys_until = [this, &ret, &key_it, router](Slice end,
                                                        bool included) {
    while (key_it != keys_.end()) {
      const std::string &user_key = key_it->first;
      int res = ucmp_->Compare(user_key, end);
      if (included) {
        if (res > 0) break;
      } else {
        if (res >= 0) break;
      }
      assert(key_it->second.only_by_point_query());
      auto &&seq_values = std::move(key_it->second.seq_value());
      assert(seq_values.size() == 1);
      auto &&seq_value = std::move(seq_values[0]);
      SequenceNumber sequence = seq_value.first;
      std::string &&value = std::move(seq_value.second);
      size_ -= user_key.size() + value.size();
      bool should_promote =
          key_it->second.repeated_accessed() || router->IsHot(user_key);
      router->Access(user_key, value.size());
      if (should_promote) {
        InternalKey key(user_key, sequence, kTypeValue);
        ret.emplace_back(std::move(*key.rep()), std::move(value));
      }
      key_it = keys_.erase(key_it);
    }
  };
  if (range_it != ranges_.end() &&
      ucmp_->Compare(range_it->second.first_user_key, smallest) < 0) {
    // Only a part of this range is promotable.
    // Therefore, we don't promote this range.
    const std::string &range_first = range_it->second.first_user_key;
    const std::string &range_last = range_it->first;
    router->AccessRange(range_first, range_last, range_it->second.num_bytes, 0);
    key_it = keys_.lower_bound(range_first);
    erase_keys_in_range(range_last);
    range_it = ranges_.erase(range_it);
  } else {
    key_it = keys_.lower_bound(smallest.ToString());
  }
  while (range_it != ranges_.end()) {
    const std::string &range_last = range_it->first;
    if (ucmp_->Compare(range_last, largest) > 0) break;
    const std::string &range_first = range_it->second.first_user_key;
    erase_keys_until(range_first, false);
    assert(range_it->second.count > 0);
    if (range_it->second.count == 1 &&
        !router->IsHot(range_first, range_last)) {
      router->AccessRange(range_it->second.first_user_key, range_it->first,
                          range_it->second.num_bytes, 0);
      erase_keys_in_range(range_last);
    } else {
      // FIXME(hotrap): AccessRange after installing results
      router->AccessRange(range_first, range_last, range_it->second.num_bytes,
                          range_it->second.sequence);
      while (key_it != keys_.end()) {
        const std::string &user_key = key_it->first;
        if (ucmp_->Compare(user_key, range_last) > 0) {
          break;
        }
        assert(!key_it->second.only_by_point_query());
        for (auto &&seq_value : key_it->second.seq_value()) {
          std::string &&value = std::move(seq_value.second);
          size_ -= user_key.size() + value.size();
          // FIXME(hotrap): Enforce retainment of this range
          InternalKey key(user_key, seq_value.first, kTypeValue);
          ret.emplace_back(std::move(*key.rep()), std::move(value));
        }
        key_it = keys_.erase(key_it);
      }
    }
    range_it = ranges_.erase(range_it);
  }
  if (range_it != ranges_.end() &&
      ucmp_->Compare(range_it->second.first_user_key, largest) <= 0) {
    // Only a part of this range is promotable.
    // Therefore, we don't promote this range.
    const std::string &range_first = range_it->second.first_user_key;
    const std::string &range_last = range_it->first;
    erase_keys_until(range_first, false);
    erase_keys_in_range(range_last);
    router->AccessRange(range_first, range_last, range_it->second.num_bytes, 0);
    range_it = ranges_.erase(range_it);
  } else {
    erase_keys_until(largest, true);
  }
  std::sort(ret.begin(), ret.end(), InternalKeyCompare(ucmp_));
  return ret;
}
}  // namespace ROCKSDB_NAMESPACE
