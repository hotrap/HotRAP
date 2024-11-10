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

// REQUIRES: new_first <= old_last, old_first <= new_last
template <typename RangeInfo>
void MergeTwoRanges(const Comparator *ucmp, std::string &new_first,
                    std::string &new_last, RangeInfo &new_info,
                    std::string &&old_first, std::string &&old_last,
                    RangeInfo &&old_info,
                    void (*merge)(RangeInfo &new_info, RangeInfo &&old_info),
                    void (*assign)(RangeInfo &new_info, RangeInfo &&old_info)) {
  if (ucmp->Compare(old_last, new_last) < 0) {
    // new_first <= old_last < new_last
    if (ucmp->Compare(old_first, new_first) < 0) {
      // old_first < new_first <= old_last < new_last
      new_first = std::move(old_first);
      merge(new_info, std::move(old_info));
    } else {
      // new_first <= old_first <= old_last < new_last
    }
  } else {
    // new_first <= new_last <= old_last
    new_last = std::move(old_last);
    if (ucmp->Compare(old_first, new_first) <= 0) {
      // old_first <= new_first <= new_last <= old_last
      new_first = std::move(old_first);
      new_info = std::move(old_info);
      assign(new_info, std::move(old_info));
    } else {
      // new_first < old_first <= new_last <= old_last
      merge(new_info, std::move(old_info));
    }
  }
}

static void MergeSeq(SequenceNumber &new_seq, SequenceNumber &&old_seq) {
  new_seq = std::max(new_seq, old_seq);
}
static void AssignSeq(SequenceNumber &new_seq, SequenceNumber &&old_seq) {
  new_seq = old_seq;
}

struct BytesSeq {
  uint64_t num_bytes;
  SequenceNumber seq;
};
static void MergeBytesSeq(BytesSeq &new_info, BytesSeq &&old_info) {
  // Overestimation. Precise estimation requires maintaining length for
  // all accessed keys, which can consume much memory.
  new_info.num_bytes += old_info.num_bytes;
  new_info.seq = std::max(new_info.seq, old_info.seq);
}
static void AssignBytesSeq(BytesSeq &new_info, BytesSeq &&old_info) {
  new_info.num_bytes = std::max(new_info.num_bytes, old_info.num_bytes);
  new_info.seq = old_info.seq;
}

struct SeqVer {
  SequenceNumber seq;
  uint64_t version_number;
};
static void MergeSeqVer(SeqVer &new_info, SeqVer &&old_info) {
  new_info.seq = std::max(new_info.seq, old_info.seq);
  new_info.version_number =
      std::max(new_info.version_number, old_info.version_number);
}
static void AssignSeqVer(SeqVer &new_info, SeqVer &&old_info) {
  new_info = old_info;
}

void InsertRanges(std::map<std::string, RangeFirstSeq, UserKeyCompare> &ranges,
                  const Comparator *ucmp, std::vector<RangeSeq> &&new_ranges) {
  for (RangeSeq &range : new_ranges) {
    std::string &&new_first = std::move(range.first_user_key);
    std::string &&new_last = std::move(range.last_user_key);
    SequenceNumber &new_seq = range.sequence;
    auto it = ranges.lower_bound(new_first);
    // new_first <= old_last
    while (it != ranges.end() &&
           ucmp->Compare(it->second.first_user_key, new_last) <= 0) {
      // old_first <= new_last
      MergeTwoRanges(ucmp, new_first, new_last, new_seq,
                     std::move(it->second.first_user_key),
                     std::string(it->first),
                     SequenceNumber(it->second.sequence), MergeSeq, AssignSeq);
      it = ranges.erase(it);
    }
    ranges.emplace_hint(it, std::piecewise_construct,
                        std::forward_as_tuple(std::move(new_last)),
                        std::forward_as_tuple(std::move(new_first), new_seq));
  }
}

PromotionCache::PromotionCache(DBImpl &db, ColumnFamilyData &cfd,
                               int target_level, const Comparator *ucmp)
    : db_(db),
      cfd_(cfd),
      target_level_(target_level),
      ucmp_(ucmp),
      mut_(ucmp),
      should_switch_(false),
      switcher_should_stop_(false),
      switcher_([this] { this->switcher(); }),
      checker_should_stop_(false),
      checker_([this] { this->checker(); }),
      max_size_(0) {}

PromotionCache::~PromotionCache() {
  assert(switcher_should_stop_);
  if (switcher_.joinable()) switcher_.join();
  assert(checker_should_stop_);
  if (checker_.joinable()) checker_.join();
}

void PromotionCache::stop_checker_no_wait() {
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
void PromotionCache::check(CheckerQueueElem &elem) {
  SuperVersion *sv = elem.sv;
  RALT *ralt = sv->mutable_cf_options.ralt;
  if (ralt == nullptr) return;

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
  auto check_key_until = [&to_promote, &key_it, &candidates, ucmp, ralt,
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
      bool should_promote = data.repeated_accessed || ralt->IsHot(user_key);
      ralt->Access(user_key, value.size());
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
    bool should_promote =
        range_it->second.count > 1 || ralt->IsHot(range_first, range_last);
    // Promoted ranges are stored in SSTables. So RALT is only responsible for
    // tracking hotness of ranges.
    ralt->AccessRange(range_first, range_last, num_bytes, 0);
    if (!should_promote) {
      RecordTick(stats, Tickers::ACCESSED_COLD_BYTES, num_bytes);
      while (key_it != candidates.end() &&
             ucmp->Compare((*key_it)->first, range_last) <= 0) {
        assert(!(*key_it)->second.only_by_point_query);
        ++key_it;
      }
      range_it = cache.ranges.erase(range_it);
    } else {
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
  auto key_has_newer_version = [this, sv, cfd](Slice user_key) {
    LookupKey key(user_key, kMaxSequenceNumber);
    Status s;
    MergeContext merge_context;
    SequenceNumber max_covering_tombstone_seq = 0;
    if (sv->imm->Get(key, nullptr, nullptr, &s, &merge_context,
                     &max_covering_tombstone_seq, ReadOptions())) {
      return true;
    }
    if (!s.ok()) {
      ROCKS_LOG_FATAL(cfd->ioptions()->logger, "Unexpected error: %s\n",
                      s.ToString().c_str());
    }
    if (sv->current->Get(nullptr, ReadOptions(), key, nullptr, nullptr, &s,
                         &merge_context, &max_covering_tombstone_seq, nullptr,
                         nullptr, nullptr, nullptr, nullptr, false,
                         target_level_)) {
      return true;
    }
    assert(s.IsNotFound());
    return false;
  };
  key_it = candidates.begin();
  auto check_newer_version_until = [&to_promote, &key_it,
                                    &key_has_newer_version, &candidates,
                                    ucmp](Slice range_first) {
    for (; key_it != candidates.end(); ++key_it) {
      const std::string &user_key = (*key_it)->first;
      if (range_first.data()) {
        if (ucmp->Compare(user_key, range_first) >= 0) break;
      }
      const auto &data = (*key_it)->second;
      assert(data.only_by_point_query);
      if (!key_has_newer_version((*key_it)->first)) {
        to_promote.push_back(*key_it);
      }
    }
  };
  range_it = cache.ranges.begin();
  while (range_it != cache.ranges.end()) {
    const std::string &range_first = range_it->second.first_user_key;
    const std::string &range_last = range_it->first;
    check_newer_version_until(range_first);
    bool has_newer_version = false;
    for (size_t pending = 0; key_it != candidates.end(); ++key_it) {
      const std::string &user_key = (*key_it)->first;
      if (ucmp->Compare(user_key, range_last) > 0) break;
      if (key_has_newer_version(user_key)) {
        has_newer_version = true;
        while (pending) {
          --pending;
          to_promote.pop_back();
        }
        do {
          ++key_it;
        } while (key_it != candidates.end() &&
                 ucmp->Compare((*key_it)->first, range_last) <= 0);
        break;
      }
      ++pending;
      to_promote.push_back(*key_it);
    }
    if (has_newer_version) {
      range_it = cache.ranges.erase(range_it);
    } else {
      ++range_it;
    }
  }
  check_newer_version_until(Slice(nullptr, 0));
  std::swap(candidates, to_promote);
  to_promote.clear();

  db_.mutex()->Lock();

  std::list<ImmPromotionCache> tmp;
  {
    auto list = imm_list().Write();
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
    TimerGuard start =
        hotrap_timers.timer(TimerType::kWriteBackToMutablePromotionCache)
            .start();
    std::vector<std::pair<std::string, ImmPCData>> keys;
    for (auto it : to_promote) {
      keys.push_back(std::move(*it));
    }

    auto mut = mut_.Write();
    mut->InsertRanges(std::move(cache.ranges), std::move(keys));
    db_.mutex()->Unlock();
    ConsumeBuffer(mut);
  } else {
    RecordTick(stats, Tickers::PROMOTED_FLUSH_BYTES, bytes_to_flush);

    // No need to SetNextLogNumber, because we don't delete any log file
    // The new immutable memtable is not written to WAL, so we can't mark the
    // ranges as promoted before the records are flushed.
    MemTable *m = cfd->ConstructNewMemtable(
        sv->mutable_cf_options, 0, std::move(cache.ranges), target_level_);
    m->Ref();
    autovector<MemTable *> memtables_to_free;
    SuperVersionContext svc(true);
    for (auto it : to_promote) {
      for (const auto &seq_value : it->second.seq_value) {
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
    db_.InstallSuperVersionAndScheduleWork(cfd, &svc, sv->mutable_cf_options);
    DBImpl::FlushRequest flush_req;
    db_.GenerateFlushRequest(autovector<ColumnFamilyData *>({cfd}), &flush_req);
    db_.SchedulePendingFlush(flush_req, FlushReason::kPromotionCacheFull);
    db_.MaybeScheduleFlushOrCompaction();

    db_.mutex()->Unlock();

    for (MemTable *table : memtables_to_free) delete table;
    svc.Clean();
  }
}

size_t PromotionCache::Mutable::Insert(std::string &&user_key,
                                       SequenceNumber sequence,
                                       std::string &&value) {
  auto range_it = ranges_.lower_bound(user_key);
  if (range_it != ranges_.end() &&
      ucmp_->Compare(range_it->second.first_user_key, user_key) <= 0) {
    range_it->second.count += 1;
    return size_;
  }
  auto it = keys_.lower_bound(user_key);
  if (it == keys_.end() || ucmp_->Compare(it->first, user_key) != 0) {
    size_ += user_key.size() + value.size();
    std::deque<std::pair<SequenceNumber, std::string>> seq_value{
        {sequence, std::move(value)}};
    keys_.emplace_hint(it, std::piecewise_construct,
                       std::forward_as_tuple(std::move(user_key)),
                       std::forward_as_tuple(std::move(seq_value),
                                             /*repeated_accessed=*/false,
                                             /*only_by_point_query=*/true));
  } else {
    assert(it->second.only_by_point_query());
    const auto &seq_value = it->second.seq_value();
    assert(seq_value.size() == 1);
    assert(seq_value[0].first == sequence);
    it->second.set_repeated_accessed(true);
  }
  return size_;
}

void PromotionCache::Mutable::InsertOneRange(
    std::vector<std::pair<std::string, std::string>> &&records,
    std::string &&first_user_key, std::string &&last_user_key,
    SequenceNumber sequence, uint64_t num_bytes) {
  assert(sequence > 0);
  if (!records.empty()) {
    assert(ucmp_->Compare(ExtractUserKey(records.front().first),
                          first_user_key) >= 0);
    assert(ucmp_->Compare(ExtractUserKey(records.back().first),
                          last_user_key) <= 0);
  }
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
  auto key_it = keys_.lower_bound(new_range_first);
  if (record_it != records.end()) {
    ParsedInternalKey ikey;
    Status s = ParseInternalKey(record_it->first, &ikey, false);
    assert(s.ok());
    std::string user_key = ikey.user_key.ToString();
    std::deque<std::pair<SequenceNumber, std::string>> seq_values;
    for (;;) {
      seq_values.emplace_back(ikey.sequence, std::move(record_it->second));
      ++record_it;
      if (record_it != records.end()) {
        s = ParseInternalKey(record_it->first, &ikey, false);
        assert(s.ok());
        if (ucmp_->Compare(ikey.user_key, user_key) == 0) continue;
      }
      MergeOneKeyInRange(key_it, std::move(user_key), std::move(seq_values),
                         sequence);
      if (record_it == records.end()) break;
      user_key = ikey.user_key.ToString();
      seq_values = std::deque<std::pair<SequenceNumber, std::string>>();
    }
  }
  MarkNotOnlyByPointQuery(key_it, new_range_last);
}
void PromotionCache::Mutable::InsertRanges(
    std::map<std::string, RangeInfo, UserKeyCompare> &&ranges,
    std::vector<std::pair<std::string, ImmPCData>> &&keys) {
  auto key_it = keys_.begin();
  auto new_key_it = keys.begin();
  auto range_it = ranges_.begin();
  auto new_range_it = ranges.begin();
  auto insert_keys_until = [this, &key_it, &new_key_it,
                            &keys](Slice range_first) {
    while (new_key_it != keys.end()) {
      if (range_first.data()) {
        if (ucmp_->Compare(new_key_it->first, range_first) >= 0) break;
      }
      std::string &&user_key = std::move(new_key_it->first);
      assert(new_key_it->second.only_by_point_query);
      auto seq_value = std::move(new_key_it->second.seq_value);
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
                                  new_key_it->second.repeated_accessed,
                                  /*only_by_point_query=*/true));
      } else {
        // key_it may be in a range
        key_it->second.set_repeated_accessed(true);
      }
      ++new_key_it;
    }
  };
  while (new_range_it != ranges.end()) {
    std::string new_range_last = new_range_it->first;
    RangeInfo &&new_range = std::move(new_range_it->second);
    while (range_it != ranges_.end() &&
           ucmp_->Compare(range_it->first, new_range.first_user_key) < 0) {
      insert_keys_until(range_it->second.first_user_key);
      // Don't insert point query records if it's in a range
      while (new_key_it != keys.end() &&
             ucmp_->Compare(new_key_it->first, range_it->first) <= 0) {
        assert(new_key_it->second.only_by_point_query);
        ++new_key_it;
      }
      ++range_it;
    }
    MergeRange(range_it, std::move(new_range_last), std::move(new_range));
    new_range_it = ranges.erase(new_range_it);
    const std::string &merged_range_first = range_it->second.first_user_key;
    const std::string &merged_range_last = range_it->first;
    SequenceNumber merged_range_sequence = range_it->second.sequence;
    insert_keys_until(merged_range_first);
    while (key_it != keys_.end() &&
           ucmp_->Compare(key_it->first, merged_range_first) < 0) {
      // key_it may be in a range
      ++key_it;
    }
    while (new_key_it != keys.end() &&
           ucmp_->Compare(new_key_it->first, merged_range_last) <= 0) {
      // It's possible that new_key is only_by_point_query because the merged
      // range is larger than the original one.
      MergeOneKeyInRange(key_it, std::move(new_key_it->first),
                         std::move(new_key_it->second.seq_value),
                         merged_range_sequence);
      ++new_key_it;
    }
    MarkNotOnlyByPointQuery(key_it, merged_range_last);
  }
  insert_keys_until(Slice(nullptr, 0));
}

void PromotionCache::Mutable::MergeRange(
    std::map<std::string, RangeInfo, UserKeyCompare>::iterator &it,
    std::string &&new_range_last, RangeInfo &&new_range) {
  BytesSeq new_info{
      .num_bytes = new_range.num_bytes,
      .seq = new_range.sequence,
  };
  if (it != ranges_.end()) {
    assert(ucmp_->Compare(it->first, new_range.first_user_key) >= 0);
    if (it != ranges_.begin()) {
      auto tmp = it;
      --tmp;
      assert(ucmp_->Compare(tmp->first, it->second.first_user_key) < 0);
    }
  }
  // new first <= old last
  while (it != ranges_.end() &&
         ucmp_->Compare(it->second.first_user_key, new_range_last) <= 0) {
    std::string &&old_first = std::move(it->second.first_user_key);
    // old first <= new last
    std::string old_last = it->first;
    BytesSeq old_info{
        .num_bytes = it->second.num_bytes,
        .seq = it->second.sequence,
    };
    new_range.count += it->second.count;
    MergeTwoRanges(ucmp_, new_range.first_user_key, new_range_last, new_info,
                   std::move(old_first), std::move(old_last),
                   std::move(old_info), MergeBytesSeq, AssignBytesSeq);
    it = ranges_.erase(it);
  }
  if (it != ranges_.end()) {
    assert(ucmp_->Compare(new_range_last, it->second.first_user_key) < 0);
  }
  if (it != ranges_.begin()) {
    auto tmp = it;
    --tmp;
    assert(ucmp_->Compare(tmp->first, new_range.first_user_key) < 0);
  }
  new_range.num_bytes = new_info.num_bytes;
  new_range.sequence = new_info.seq;
  it =
      ranges_.emplace_hint(it, std::move(new_range_last), std::move(new_range));
}
// REQUIRES: it->first is in range
void PromotionCache::Mutable::MergeOneKeyInRange(
    std::map<std::string, PCData, UserKeyCompare>::iterator &it,
    std::string &&user_key,
    std::deque<std::pair<SequenceNumber, std::string>> &&seq_values,
    SequenceNumber sequence) {
  while (it != keys_.end() && ucmp_->Compare(it->first, user_key) < 0) {
    it->second.set_only_by_point_query(false);
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
    auto &saved_seq_values = it->second.seq_value();
    assert(!saved_seq_values.empty());
    auto new_it = seq_values.begin();
    // Insert new versions
    while (new_it != seq_values.end() &&
           new_it->first > saved_seq_values.front().first) {
      size_ += user_key.size() + new_it->second.size();
      saved_seq_values.emplace_front(new_it->first, std::move(new_it->second));
      ++new_it;
    }
    // Skip versions we already have
    while (new_it != seq_values.end() &&
           new_it->first == saved_seq_values.back().first) {
      ++new_it;
    }
    // Drop stale versions
    while (saved_seq_values.size() >= 2 &&
           saved_seq_values[saved_seq_values.size() - 2].first <= sequence) {
      size_ -= user_key.size() + saved_seq_values.back().second.size();
      saved_seq_values.pop_back();
    }
    // Insert versions we don't have (possible if this key was inserted by a
    // point query)
    while (new_it != seq_values.end()) {
      if (new_it->first <= sequence && !saved_seq_values.empty() &&
          saved_seq_values.back().first <= sequence)
        break;
      assert(it->second.only_by_point_query());
      size_ += user_key.size() + new_it->second.size();
      saved_seq_values.emplace_back(new_it->first, std::move(new_it->second));
      ++new_it;
    }
    // Now there should be a visible version
    assert(!saved_seq_values.empty());
    assert(saved_seq_values.back().first <= sequence);
    // There shouldn't be more than one visible version
    assert(saved_seq_values.size() == 1 ||
           saved_seq_values[saved_seq_values.size() - 2].first > sequence);
    it->second.set_repeated_accessed(true);
    it->second.set_only_by_point_query(false);
  }
}
void PromotionCache::Mutable::MarkNotOnlyByPointQuery(
    std::map<std::string, PCData, UserKeyCompare>::iterator &it,
    Slice range_last) {
  while (it != keys_.end() && ucmp_->Compare(it->first, range_last) <= 0) {
    it->second.set_only_by_point_query(false);
    ++it;
  }
}
std::pair<std::vector<std::pair<std::string, std::string>>,
          std::vector<RangeSeq>>
PromotionCache::Mutable::TakeRange(InternalStats *internal_stats, RALT *ralt,
                                   Slice smallest, Slice largest) {
  std::vector<RangeSeq> ranges;
  auto guard =
      internal_stats->hotrap_timers().timer(TimerType::kTakeRange).start();
  std::vector<std::pair<std::string, std::string>> records;
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
  auto erase_point_query_keys = [this, &records, &key_it, ralt](Slice end,
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
          key_it->second.repeated_accessed() || ralt->IsHot(user_key);
      ralt->Access(user_key, value.size());
      if (should_promote) {
        InternalKey key(user_key, sequence, kTypeValue);
        records.emplace_back(std::move(*key.rep()), std::move(value));
      }
      key_it = keys_.erase(key_it);
    }
  };
  if (range_it != ranges_.end() &&
      ucmp_->Compare(range_it->second.first_user_key, smallest) < 0) {
    // Only a part of this range is promotable.
    // Therefore, we don't promote this range.
    const std::string &range_first = range_it->second.first_user_key;
    key_it = keys_.lower_bound(range_first);
    const std::string &range_last = range_it->first;
    ralt->AccessRange(range_first, range_last, range_it->second.num_bytes, 0);
    erase_point_query_keys(range_first, false);
    erase_keys_in_range(range_last);
    range_it = ranges_.erase(range_it);
  } else {
    key_it = keys_.lower_bound(smallest.ToString());
  }
  while (range_it != ranges_.end()) {
    const std::string &range_last = range_it->first;
    if (ucmp_->Compare(range_last, largest) > 0) break;
    const std::string &range_first = range_it->second.first_user_key;
    erase_point_query_keys(range_first, false);
    assert(range_it->second.count > 0);
    if (range_it->second.count == 1 && !ralt->IsHot(range_first, range_last)) {
      ralt->AccessRange(range_it->second.first_user_key, range_it->first,
                        range_it->second.num_bytes, 0);
      erase_keys_in_range(range_last);
    } else {
      ralt->AccessRange(range_first, range_last, range_it->second.num_bytes, 0);
      ranges.emplace_back(range_first, range_last, range_it->second.sequence);
      while (key_it != keys_.end()) {
        const std::string &user_key = key_it->first;
        if (ucmp_->Compare(user_key, range_last) > 0) {
          break;
        }
        assert(!key_it->second.only_by_point_query());
        for (auto &&seq_value : key_it->second.seq_value()) {
          std::string &&value = std::move(seq_value.second);
          size_ -= user_key.size() + value.size();
          InternalKey key(user_key, seq_value.first, kTypeValue);
          records.emplace_back(std::move(*key.rep()), std::move(value));
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
    erase_point_query_keys(range_first, false);
    erase_keys_in_range(range_last);
    ralt->AccessRange(range_first, range_last, range_it->second.num_bytes, 0);
    range_it = ranges_.erase(range_it);
  } else {
    erase_point_query_keys(largest, true);
  }
  std::sort(records.begin(), records.end(), InternalKeyCompare(ucmp_));
  return std::make_pair(std::move(records), std::move(ranges));
}

// Return the mutable promotion size, or 0 if inserted to buffer.
void PromotionCache::Insert(const MutableCFOptions &mutable_cf_options,
                            std::string &&user_key, SequenceNumber sequence,
                            std::string &&value) const {
  size_t mut_size;
  {
    auto mut = mut_.TryWrite();
    if (!mut.has_value()) {
      mut_buffer_.Lock()->emplace_back(std::move(user_key), sequence,
                                       std::move(value));
      mut_size = 0;
    } else {
      mut_size =
          mut.value()->Insert(std::move(user_key), sequence, std::move(value));
    }
  }
  size_t tot = mut_size + imm_list_.Read()->size;
  rusty::intrinsics::atomic_max_relaxed(max_size_, tot);
  if (mut_size < mutable_cf_options.write_buffer_size) return;
  ScheduleSwitchMut();
}
void PromotionCache::InsertOneRange(
    const MutableCFOptions &mutable_cf_options,
    std::vector<std::pair<std::string, std::string>> &&records,
    std::string &&first_user_key, std::string &&last_user_key,
    SequenceNumber sequence, uint64_t num_bytes) const {
  size_t mut_size;
  {
    auto mut = mut_.Write();
    mut->InsertOneRange(std::move(records), std::move(first_user_key),
                        std::move(last_user_key), sequence, num_bytes);
    ConsumeBuffer(mut);
    mut_size = mut->size_;
  }
  size_t tot = mut_size + imm_list_.Read()->size;
  rusty::intrinsics::atomic_max_relaxed(max_size_, tot);
  if (mut_size < mutable_cf_options.write_buffer_size) return;
  ScheduleSwitchMut();
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

void PromotionCache::ScheduleSwitchMut() const {
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

void PromotionCache::SwitchMutablePromotionCache() {
  SuperVersion *sv;
  std::list<rocksdb::ImmPromotionCache>::iterator iter;
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
                     .timer(TimerType::kSwitchMutablePromotionCache)
                     .start();

    db_.mutex()->Lock();
    auto imm_list = imm_list_.Write();
    sv = cfd_.GetSuperVersion();
    // Checker is responsible to unref it
    sv->Ref();
    db_.mutex()->Unlock();

    auto mut = mut_.Write();
    mut_size = mut->size_;
    if (mut_size < sv->mutable_cf_options.write_buffer_size) {
      sv->Unref();
      std::unique_lock<std::mutex> lock(switcher_lock_);
      assert(should_switch_);
      should_switch_ = false;
      return;
    }

    std::unordered_map<std::string, ImmPCData> cache;
    uint64_t size = 0;
    for (auto &&a : mut->keys_) {
      for (const auto &seq_value : a.second.seq_value()) {
        size += a.first.size() + seq_value.second.size();
      }
      auto ret = cache.emplace(
          std::piecewise_construct, std::forward_as_tuple(std::move(a.first)),
          std::forward_as_tuple(std::move(a.second.seq_value()),
                                a.second.only_by_point_query(),
                                a.second.repeated_accessed()));
      assert(ret.second);
    }
    mut->keys_.clear();
    assert(size == mut->size_);
    mut->size_ = 0;
    std::map<std::string, RangeInfo, UserKeyCompare> ranges(
        cfd_.ioptions()->user_comparator);
    std::swap(ranges, mut->ranges_);

    imm_list->size += mut_size;
    iter = imm_list->list.emplace(imm_list->list.end(), std::move(cache),
                                  mut_size, std::move(ranges));
    imm_list.drop();

    {
      std::unique_lock<std::mutex> lock(switcher_lock_);
      assert(should_switch_);
      should_switch_ = false;
    }

    ConsumeBuffer(mut);
    mut_size = mut->size_;
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

void PromotionCache::switcher() {
  pthread_setname_np(pthread_self(), "switcher");
  for (;;) {
    {
      std::unique_lock<std::mutex> lock(switcher_lock_);
      switcher_signal_.wait(
          lock, [&] { return switcher_should_stop_ || should_switch_; });
      if (switcher_should_stop_) break;
    }
    SwitchMutablePromotionCache();
  }
}

void PromotionCache::MarkRangesPromoted(std::vector<RangeSeq> &&ranges,
                                        uint64_t version_number) const {
  auto promoted_ranges = promoted_ranges_.Write();
  for (RangeSeq &range : ranges) {
    std::string &&new_first = std::move(range.first_user_key);
    std::string &&new_last = std::move(range.last_user_key);
    SeqVer new_info{
        .seq = range.sequence,
        .version_number = version_number,
    };
    auto it = promoted_ranges->lower_bound(new_first);
    // new_first <= old_last
    while (it != promoted_ranges->end() &&
           ucmp_->Compare(it->second.first_user_key, new_last) <= 0) {
      // old_first <= new_last
      SeqVer old_info{
          .seq = it->second.sequence,
          .version_number = it->second.version_number,
      };
      MergeTwoRanges(ucmp_, new_first, new_last, new_info,
                     std::move(it->second.first_user_key),
                     std::string(it->first), std::move(old_info), MergeSeqVer,
                     AssignSeqVer);
      it = promoted_ranges->erase(it);
    }
    promoted_ranges->emplace_hint(
        it, std::piecewise_construct,
        std::forward_as_tuple(std::move(new_last)),
        std::forward_as_tuple(std::move(new_first), new_info.seq,
                              new_info.version_number));
  }
}
void PromotionCache::LastPromoted(const ReadOptions &read_options,
                                  uint64_t version_number, Slice seek_user_key,
                                  std::string &last_promoted) const {
  auto res = promoted_ranges_.TryRead();
  if (!res.has_value()) return;
  auto &mut = res.value();
  auto it = mut->lower_bound(seek_user_key.ToString());
  if (it == mut->end()) return;
  if (read_options.snapshot &&
      it->second.sequence > read_options.snapshot->GetSequenceNumber())
    return;
  if (it->second.version_number > version_number) return;
  if (ucmp_->Compare(it->second.first_user_key, seek_user_key) > 0) return;
  if (last_promoted.empty() || ucmp_->Compare(it->first, last_promoted) > 0) {
    last_promoted.assign(it->first);
  }
}

std::pair<std::vector<std::pair<std::string, std::string>>,
          std::vector<RangeSeq>>
PromotionCache::TakeRange(InternalStats *internal_stats, RALT *ralt,
                          Slice smallest, Slice largest) const {
  std::pair<std::vector<std::pair<std::string, std::string>>,
            std::vector<RangeSeq>>
      ret;
  {
    auto mut = mut_.Write();
    ConsumeBuffer(mut);
    ret = mut->TakeRange(internal_stats, ralt, smallest, largest);
  }
  auto records = std::move(ret.first);
  auto ranges = std::move(ret.second);

  std::vector<RangeSeq> promoted;
  {
    auto promoted_ranges = promoted_ranges_.Write();
    auto it = promoted_ranges->lower_bound(smallest.ToString());
    while (it != promoted_ranges->end() &&
           ucmp_->Compare(it->first, largest) <= 0) {
      promoted.emplace_back(std::move(it->second.first_user_key),
                            std::string(it->first), it->second.sequence);
      it = promoted_ranges->erase(it);
    }
  }

  std::vector<RangeSeq> merged_ranges;
  auto promoted_it = promoted.begin();
  for (RangeSeq &range : ranges) {
    std::string &&new_first = std::move(range.first_user_key);
    std::string &&new_last = std::move(range.last_user_key);
    SequenceNumber new_seq = range.sequence;
    while (promoted_it != promoted.end() &&
           ucmp_->Compare(promoted_it->last_user_key, new_first) < 0) {
      merged_ranges.emplace_back(std::move(*promoted_it));
      ++promoted_it;
    }
    while (promoted_it != promoted.end() &&
           ucmp_->Compare(promoted_it->first_user_key, new_last) <= 0) {
      MergeTwoRanges(ucmp_, new_first, new_last, new_seq,
                     std::move(promoted_it->first_user_key),
                     std::move(promoted_it->last_user_key),
                     std::move(promoted_it->sequence), MergeSeq, AssignSeq);
      ++promoted_it;
    }
    merged_ranges.emplace_back(std::move(new_first), std::move(new_last),
                               new_seq);
  }

  return std::make_pair(std::move(records), std::move(merged_ranges));
}

}  // namespace ROCKSDB_NAMESPACE
