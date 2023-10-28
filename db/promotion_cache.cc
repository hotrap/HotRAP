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
#include "db/memtable.h"
#include <memory>

namespace std {

template<typename T, typename... Args>
std::unique_ptr<T> make_unique(Args&&... args) {
  return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
}

}

namespace ROCKSDB_NAMESPACE {

bool PromotionCacheMemtable::Get(const Slice& key, PinnableSlice* value) {
  auto it = data_.find(key.ToString());
  if (it != data_.end()) {
    value->PinSelf(it->second);
    return true;
  }

  return false;
}

void PromotionCacheMemtable::Put(const Slice& key, const Slice& value) {
  if(data_.emplace(key.ToString(), value.ToString()).second) {
    buffer_size_ += value.size();
  }
}


void PromotionCacheMemtable::Update(const Slice& key, const Slice& value) {
  auto it = data_.find(key.ToString());
  if (it != data_.end()) {
    buffer_size_ += value.size() - it->second.size();
    it->second = value.ToString();
  }
}

bool PromotionCacheImmList::Get(const Slice& key, PinnableSlice* value) {
  for (auto& t : imm_list_) {
    if (t->Get(key, value)) {
      return true;
    }
  }
  return false;
}


void PromotionCacheImmList::Update(const Slice& key, const Slice& value) {
  for (auto& t : imm_list_) {
    t->Update(key, value);
  }
}

void PromotionCacheImmList::AddMemtable(std::unique_ptr<PromotionCacheMemtable> mem) {
  buffer_size_ += mem->Size();
  imm_list_.push_back(std::move(mem));
}

PromotionCache::PromotionCache(const Comparator* ucmp, size_t table_size, ColumnFamilyData* cfd)
  : ucmp_(ucmp), table_size_(table_size), cfd_(cfd), signal_flush_(&flush_m_) {
  mut_ = std::make_unique<PromotionCacheMemtable>(ucmp);
  imms_ = std::make_unique<PromotionCacheImmList>();
  checked_ = std::make_unique<PromotionCacheMemtable>(ucmp);
  flush_thread_ = std::thread([&](){ this->FlushThread(); });
}

PromotionCache::~PromotionCache() {
  signal_terminate_ = true;
  signal_flush_.Signal();
  flush_thread_.join();
}

bool PromotionCache::Get(const Slice& key, PinnableSlice* value) {
  auto timer_guard = cfd_->internal_stats()->hotrap_timers()
                         .timer(TimerType::kPromotionCacheGet)
                         .start();
  io_m_.ReadLock();
  bool s = false;
  s = mut_->Get(key, value);
  if (!s) {
    s = imms_->Get(key, value);
  }
  if (!s) {
    s = checked_->Get(key, value);
  }
  io_m_.ReadUnlock();
  return s;
}

void PromotionCache::Put(const Slice& key, const Slice& value, DBImpl* db) {
  db_ = db;
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
  
  // The inserted keys will not be accessed by GET() because they will access the memtable first.
  // Use read lock because the structure of mut_ may be modified by Put.
  new_iter->SeekToFirst();
  io_m_.ReadLock();
  while(new_iter->Valid()) {
    auto key = new_iter->user_key();
    auto value = new_iter->value();
    mut_->Update(key, value);
    new_iter->Next();
  }
  io_m_.ReadUnlock();

  flush_m_.Lock();
  if (imms_->Size()) {
    new_iter->SeekToFirst();
    while(new_iter->Valid()) {
      auto key = new_iter->user_key();
      auto value = new_iter->value();
      imms_->Update(key, value);
      new_iter->Next();
    }
  }
  flush_m_.Unlock();

  // These can only be modified by FlushThread. 
  // We are modifying the immutable parts so we use imm_m_.
  imm_m_.Lock();
  new_iter->SeekToFirst();
  while(new_iter->Valid()) {
    auto key = new_iter->user_key();
    auto value = new_iter->value();
    checked_->Update(key, value);
    new_iter->Next();
  }
  imm_m_.Unlock();
  delete new_iter;
}

void PromotionCache::FlushThread() {
  while (!signal_terminate_) {
    flush_m_.Lock();
    while (!imms_->Size() || signal_terminate_) {
      signal_flush_.Wait();
    }
    if (signal_terminate_) {
      return;
    }
    // Store the pointers we are going to process.
    std::vector<PromotionCacheMemtable*> Q;
    for (auto& t : imms_->Imms()) {
      Q.push_back(t.get());
    }
    flush_m_.Unlock();

    // Check if it is stably hot.
    // Store the references to them. References are immutable.
    std::vector<std::pair<const std::string&, const std::string&>> kv;
    
    auto sv = db_->GetAndRefSuperVersion(cfd_);
    auto router = sv->mutable_cf_options.compaction_router;
    size_t buffer_size = checked_->Size();
    assert(router != nullptr);
    {
      TimerGuard check_stably_hot_start = cfd_->internal_stats()->hotrap_timers()
                                              .timer(TimerType::kCheckStablyHot)
                                              .start();
      for (auto& t : Q) {
        for (auto& p : t->Data()) {
          auto& key = p.first;
          auto& value = p.second;
          if(router->IsStablyHot(key)) {
            kv.emplace_back(key, value);
            buffer_size += key.size() + value.size();
          }
        }
      }
    }
    // This buffer size may be over estimated because some concurrency effects.
    if (buffer_size >= table_size_) {
      // Since RemoveObsolete only works under DBMutex. We can only acquire DBMutex.
      db_->mutex()->Lock();
      auto new_checked = std::make_unique<PromotionCacheMemtable>(*checked_);
      for (auto& p : kv) {
        auto& key = p.first;
        auto& value = p.second;
        new_checked->Put(key, value);
      }
      // Reset checked_;
      io_m_.WriteLock();
      checked_ = std::make_unique<PromotionCacheMemtable>(ucmp_);
      io_m_.WriteUnlock();    
      // Flush new_checked.
      MemTable *m = cfd_->ConstructNewMemtable(sv->mutable_cf_options, 0);
      m->Ref();
      autovector<MemTable *> memtables_to_free;
      size_t flushed_bytes = 0;
      for (auto& p : new_checked->Data()) {
        auto& key = p.first;
        auto& value = p.second;
        Status s = m->Add(1, ValueType::kTypeValue, key, value, nullptr);
        if (!s.ok()) {
          ROCKS_LOG_FATAL(cfd_->ioptions()->logger,
                          "FlushThread: Unexpected error: %s",
                          s.ToString().c_str());
        }
        flushed_bytes += key.size() + value.size();
      }
      cfd_->imm()->Add(m, &memtables_to_free);
      SuperVersionContext svc(true);
      db_->InstallSuperVersionAndScheduleWork(cfd_, &svc, sv->mutable_cf_options);
      DBImpl::FlushRequest flush_req;
      db_->GenerateFlushRequest(autovector<ColumnFamilyData *>({cfd_}), &flush_req);
      db_->SchedulePendingFlush(flush_req, FlushReason::kPromotionCacheFull);
      db_->MaybeScheduleFlushOrCompaction();
      db_->mutex()->Unlock();
      // Clean up.
      for (MemTable *table : memtables_to_free) delete table;
      svc.Clean();
      Statistics *stats = cfd_->ioptions()->stats;
      RecordTick(stats, Tickers::PROMOTED_FLUSH_BYTES, flushed_bytes);
    } else {
      imm_m_.Lock();
      // Create new memtable to avoid blocking GET.
      auto new_checked = std::make_unique<PromotionCacheMemtable>(*checked_);
      for (auto& p : kv) {
        auto& key = p.first;
        auto& value = p.second;
        new_checked->Put(key, value);
      }
      io_m_.WriteLock();
      checked_ = std::move(new_checked);
      io_m_.WriteUnlock();    
      imm_m_.Unlock();
    }
    
    if (sv->Unref()) {
      db_->mutex()->Lock();
      sv->Cleanup();
      delete sv;
      db_->mutex()->Unlock();
    }

    // Remove the processed pointers.
    io_m_.WriteLock();
    flush_m_.Lock();
    imms_->Imms().erase(imms_->Imms().begin(), imms_->Imms().begin() + Q.size());
    flush_m_.Unlock();
    io_m_.WriteUnlock();    
  }
}



}  // namespace ROCKSDB_NAMESPACE
