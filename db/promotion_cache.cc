#include "promotion_cache.h"

#include <functional>

#include "column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/job_context.h"
#include "db/lookup_key.h"
#include "db/version_set.h"
#include "logging/logging.h"
#include "monitoring/statistics.h"
#include "rocksdb/compaction_router.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

bool PromotionCacheMemtable::Get(const Slice& key, PinnableSlice* value) {
  if (auto it = del_data_.find(key.ToString()); it != del_data_.end()) {
    return false;
  }

  if (auto it = data_.find(key.ToString()); it != data_.end()) {
    value->PinSelf(it->second);
    return true;
  }

  return false;
}

void PromotionCacheMemtable::Put(const Slice& key, const Slice& value) {
  if (auto it = del_data_.find(key.ToString()); it != del_data_.end()) {
    del_data_.erase(it);
  }
  data_.insert_or_assign(key.ToString(), value.ToString());
}

void PromotionCacheMemtable::Remove(const Slice& key) {
  del_data_.insert(key.ToString());
}

bool PromotionCacheImmList::Get(const Slice& key, PinnableSlice* value) {
  for (auto& t : imm_list_) {
    if (t->Get(key, value)) {
      return true;
    }
  }
  return false;
}

void PromotionCacheImmList::Remove(const Slice& key) {
  buffer_size_ = 0;
  for (auto& t : imm_list_) {
    t->Remove(key);
    buffer_size_ += t->Size();
  }
}

void PromotionCacheImmList::AddMemtable(std::unique_ptr<PromotionCacheMemtable> mem) {
  buffer_size_ += mem->Size();
  imm_list_.push_back(std::move(mem));
}

PromotionCache::PromotionCache(const Comparator* ucmp, size_t table_size, ColumnFamilyData* cfd, DBImpl* db)
  : ucmp_(ucmp), table_size_(table_size), cfd_(cfd), db_(db) {
  mut_ = std::make_unique<PromotionCacheMemtable>(ucmp);
  imms_ = std::make_unique<PromotionCacheImmList>();
}

bool PromotionCache::Get(InternalStats *internal_stats, const Slice& key, PinnableSlice* value) {
  auto timer_guard = internal_stats->hotrap_timers()
                         .timer(TimerType::kPromotionCacheGet)
                         .start();
  io_m_.ReadLock();
  bool s = false;
  s = mut_->Get(key, value);
  if (!s) {
    s = imms_->Get(key, value);
  }
  io_m_.ReadUnlock();
  return s;
}

void PromotionCache::Put(const Slice& key, const Slice& value) {
  io_m_.WriteLock();
  reg_st_m_.ReadLock();
  bool is_obsolete = in_process_.find(key.ToString()) == in_process_.end();
  reg_st_m_.ReadUnlock();
  if (!is_obsolete) {
    mut_->Put(key, value);
    if (mut_->Size() > table_size_) {
      flush_m_.Lock();
      imms_->AddMemtable(std::move(mut_));
      mut_ = std::make_unique<PromotionCacheMemtable>(ucmp_);
      signal_flush_.Signal();
      flush_m_.Unlock();
    }
  }
  io_m_.WriteUnlock();
}

void PromotionCache::RegisterInProcessReadKey(const Slice& key) {
  reg_st_m_.WriteLock();
  in_process_.insert(key.ToString());
  reg_st_m_.WriteUnlock();
}

void PromotionCache::UnregisterInProcessReadKey(const Slice& key) {
  reg_st_m_.WriteLock();
  in_process_.erase(key.ToString());
  reg_st_m_.WriteUnlock();
}

void PromotionCache::RemoveObsolete(MemTable* table) {
  Arena arena;
  auto new_iter = table->NewIterator(ReadOptions(), &arena);
  reg_st_m_.WriteLock();
  for (auto it = in_process_.begin(); it != in_process_.end(); ) {
    new_iter->Seek(*it);
    auto nit = std::next(it);
    if (!new_iter->Valid() || !ucmp_->Equal(*it, new_iter->user_key())) {
      in_process_.erase(it);
    }
    it = nit;
  }
  reg_st_m_.WriteUnlock();
  io_m_.ReadLock();
  new_iter->SeekToFirst();
  while(new_iter->Valid()) {
    auto key = new_iter->user_key();
    mut_->Remove(key);
    imms_->Remove(key);
    new_iter->Next();
  }
  io_m_.ReadUnlock();
  delete new_iter;
}

void PromotionCache::FlushThread() {
  while (!signal_terminate_) {
    flush_m_.Lock();
    signal_flush_.Wait();
    // Store the pointers we are going to process.
    std::vector<PromotionCacheMemtable*> Q;
    for (auto& t : imms_->Imms()) if (!t->IsChecked()) {
      Q.push_back(t.get());
    }
    flush_m_.Unlock();
    auto sv = db_->GetAndRefSuperVersion(cfd_);
    auto router = sv->mutable_cf_options.compaction_router;
    
    // Check if it is stably hot.
    std::vector<std::pair<const std::string&, const std::string&>> keys;
    if (router) {
      TimerGuard check_stably_hot_start = cfd_->internal_stats()->hotrap_timers()
                                              .timer(TimerType::kCheckStablyHot)
                                              .start();
      for (auto& t : Q) {
        for (auto& [key, value] : t->Data()) {
          if(router->IsStablyHot(key)) {
            keys.emplace_back(key, value);
          }
        }
      }
    }

    
    
  }
}



}  // namespace ROCKSDB_NAMESPACE
