#include "promotion_cache.h"

#include "column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/job_context.h"
#include "db/lookup_key.h"
#include "db/version_set.h"
#include "logging/logging.h"
#include "rocksdb/options.h"

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
    auto guard = mut_.Read();
    const MutableCache &mut = guard.deref();
    // TODO: Avoid the copy here after upgrading to C++14
    auto it = mut.cache.find(key.ToString());
    if (it != mut.cache.end()) {
      if (value) value->PinSelf(it->second);
      return true;
    }
  }
  auto guard = imm_list_.Read();
  const ImmPromotionCacheList &imm_list = guard.deref();
  for (const ImmPromotionCache &imm : imm_list.list) {
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
  auto guard = cache.updated.Lock();
  std::unordered_set<std::string> &updated = guard.deref_mut();
  updated.insert(user_key);
}
// Will unref sv
static void check_newer_version(DBImpl *db, SuperVersion *sv, int target_level,
                                std::list<ImmPromotionCache>::iterator iter) {
  ColumnFamilyData *cfd = sv->cfd;
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
  const auto &updated = cache.updated.Lock().deref_mut();
  // TODO: Avoid copying here by flushing immutable promotion cache directly.
  for (const auto &item : cache.cache) {
    const std::string &user_key = item.first;
    const std::string &value = item.second;
    if (updated.find(user_key) != updated.end()) continue;
    Status s = m->Add(0, ValueType::kTypeValue, user_key, value, nullptr);
    if (!s.ok()) {
      ROCKS_LOG_FATAL(cfd->ioptions()->logger,
                      "check_newer_version: Unexpected error: %s",
                      s.ToString().c_str());
    }
  }
  cfd->imm()->Add(m, &memtables_to_free);
  db->InstallSuperVersionAndScheduleWork(cfd, &svc, sv->mutable_cf_options);
  db->mutex()->Unlock();

  for (MemTable *table : memtables_to_free) delete table;
  svc.Clean();

  {
    auto guard = cfd->promotion_caches().Read();
    const std::map<int, PromotionCache> &caches = guard.deref();
    auto it = caches.find(target_level);
    assert(it->first == target_level);
    auto list_guard = it->second.imm_list().Write();
    auto &list = list_guard.deref_mut();
    list.size -= iter->size;
    list.list.erase(iter);
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
  auto guard = mut_.Write();
  MutableCache &mut = guard.deref_mut();
  // TODO: Avoid requiring the ownership of key here after upgrading to C++14
  auto it = mut.cache.find(key);
  if (it != mut.cache.end()) return;
  mut.size += key.size() + value.size();
  size_t tot = mut.size + imm_list_.Read().deref().size;
  atomic_max_relaxed(max_size_, tot);
  auto ret = mut.cache.insert(std::make_pair(std::move(key), value.ToString()));
  (void)ret;
  assert(ret.second == true);
  if (mut.size < write_buffer_size) return;
  db.mutex()->Lock();
  SuperVersion *sv = cfd.GetSuperVersion();
  // check_newer_version is responsible to unref it
  sv->Ref();
  auto imm_guard = imm_list_.Write();
  ImmPromotionCacheList &imm_list = imm_guard.deref_mut();
  imm_list.size += mut.size;
  auto iter = imm_list.list.emplace(imm_list.list.end(), std::move(mut.cache),
                                    mut.size);
  mut.cache = std::map<std::string, std::string, UserKeyCompare>(
      cfd.ioptions()->user_comparator);
  mut.size = 0;
  db.mutex()->Unlock();
  // TODO: Use thread pool of RocksDB
  std::thread checker(check_newer_version, &db, sv, target_level_, iter);
  checker.detach();
}
// [begin, end)
std::vector<std::pair<std::string, std::string>> PromotionCache::TakeRange(
    Slice smallest, Slice largest) {
  auto guard = mut_.Write();
  MutableCache &mut = guard.deref_mut();
  std::vector<std::pair<std::string, std::string>> ret;
  auto begin_it = mut.cache.lower_bound(smallest.ToString());
  auto it = begin_it;
  while (it != mut.cache.end() && ucmp_->Compare(it->first, largest) <= 0) {
    // TODO: Is it possible to avoid copying here?
    ret.emplace_back(it->first, it->second);
    mut.size -= it->first.size() + it->second.size();
    ++it;
  }
  mut.cache.erase(begin_it, it);
  return ret;
}
}  // namespace ROCKSDB_NAMESPACE
