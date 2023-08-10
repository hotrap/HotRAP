#include "promotion_cache.h"

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

PromotionCache::PromotionCache(int target_level, const Comparator *ucmp)
    : target_level_(target_level),
      ucmp_(ucmp),
      mut_{MutableCache{std::map<std::string, std::string, UserKeyCompare>{
                            UserKeyCompare(ucmp)},
                        0}},
      max_size_(0) {}

bool PromotionCache::Get(Slice key, PinnableSlice *value) const {
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

static void mark_updated(ImmPromotionCache &cache,
                         const std::string &user_key) {
  auto updated = cache.updated.Lock();
  updated->insert(user_key);
}
// Will unref sv
void check_newer_version(DBImpl *db, SuperVersion *sv, int target_level,
                         std::list<ImmPromotionCache>::iterator iter) {
  ColumnFamilyData *cfd = sv->cfd;
  InternalStats &internal_stats = *cfd->internal_stats();
  TimerGuard timer_guard = internal_stats.hotrap_timers()
                               .timer(TimerType::kCheckNewerVersion)
                               .start();
  ImmPromotionCache &cache = *iter;
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
                     static_cast<unsigned int>(-1), target_level);
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
    // TODO: Avoid copying here by flushing immutable promotion cache directly.
    for (const auto &item : cache.cache) {
      const std::string &user_key = item.first;
      const std::string &value = item.second;
      if (updated->find(user_key) != updated->end()) continue;
      // It seems that sequence number 0 is treated specially. So we use 1 here.
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
  Statistics *stats = cfd->ioptions()->stats;
  RecordTick(stats, Tickers::PROMOTED_FLUSH_BYTES, flushed_bytes);

  {
    auto caches = cfd->promotion_caches().Read();
    auto it = caches->find(target_level);
    assert(it->first == target_level);
    auto list = it->second.imm_list().Write();
    list->size -= iter->size;
    list->list.erase(iter);
  }

  if (sv->Unref()) {
    db->mutex()->Lock();
    sv->Cleanup();
    db->mutex()->Unlock();
  }
}
void PromotionCache::Promote(DBImpl &db, ColumnFamilyData &cfd,
                             size_t write_buffer_size, std::string key,
                             Slice value) {
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
  db.mutex()->Lock();
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
  // TODO: Use thread pool of RocksDB
  std::thread checker(check_newer_version, &db, sv, target_level_, iter);
  checker.detach();
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
