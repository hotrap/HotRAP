#pragma once

#include <atomic>
#include <map>
#include <unordered_set>

#include "monitoring/instrumented_mutex.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "util/mutexlock.h"
#include "memtable/inlineskiplist.h"

namespace ROCKSDB_NAMESPACE {

class ColumnFamilyData;
class DBImpl;
class InternalStats;
class PromotionCache;

class UserKeyCompare {
 public:
  UserKeyCompare(const Comparator *ucmp) : ucmp_(ucmp) {}
  bool operator()(const std::string &lhs, const std::string &rhs) const {
    return ucmp_->Compare(lhs, rhs) < 0;
  }

 private:
  const Comparator *ucmp_;
};

class PromotionCacheMemtable {
 public:
  PromotionCacheMemtable(const Comparator* ucmp)
    : data_(ucmp), del_data_(ucmp), ucmp_(ucmp) {}

  bool Get(const Slice& key, PinnableSlice* value);

  // Require external mutex.
  void Put(const Slice& key, const Slice& value);

  // Ensure that there is only one writer.
  void Remove(const Slice& key);

  size_t Size() const { return buffer_size_; }

  void MarkChecked() {
    checked_ = true;
  }

  bool IsChecked() const {
    return checked_;
  }

  std::map<std::string, std::string, UserKeyCompare>& Data() {
    return data_;
  }
  
  std::map<std::string, std::string, UserKeyCompare>& DelData() {
    return data_;
  }
 private:
  std::map<std::string, std::string, UserKeyCompare> data_;
  std::set<std::string, UserKeyCompare> del_data_;
  const Comparator* ucmp_;
  size_t buffer_size_{0};
  bool checked_{false};
  
  friend class PromotionCache;
};

class PromotionCacheImmList {
 public:
  PromotionCacheImmList() = default;

  bool Get(const Slice& key, PinnableSlice* value);

  void Remove(const Slice& key);

  void AddMemtable(std::unique_ptr<PromotionCacheMemtable> mem);

  size_t Size() const { return buffer_size_; }

  std::vector<std::unique_ptr<PromotionCacheMemtable>>& Imms() {
    return imm_list_;
  }

 private:
  std::vector<std::unique_ptr<PromotionCacheMemtable>> imm_list_;
  size_t buffer_size_{0};

  friend class PromotionCache;
};

class PromotionCache {
 public:
  PromotionCache(const Comparator* ucmp, size_t table_size, ColumnFamilyData* cfd, DBImpl* db);
  PromotionCache(const PromotionCache &) = delete;
  PromotionCache &operator=(const PromotionCache &) = delete;
  PromotionCache(PromotionCache &&) = delete;
  PromotionCache &operator=(PromotionCache &&) = delete;

  ~PromotionCache() = default;

  void RegisterInProcessReadKey(const Slice& key);

  void UnregisterInProcessReadKey(const Slice& key);

  void Put(const Slice& key, const Slice& value);

  void RemoveObsolete(MemTable* table);

  bool Get(const Slice& key, PinnableSlice* value);

 private:
  void FlushThread();

  const Comparator* ucmp_;
  size_t table_size_;
  ColumnFamilyData* cfd_;
  DBImpl* db_;

  std::unique_ptr<PromotionCacheMemtable> mut_;
  std::unique_ptr<PromotionCacheImmList> imms_;
  port::RWMutex io_m_;
  port::Mutex flush_m_;
  port::CondVar signal_flush_;
  bool signal_terminate_{false};
  std::thread flush_thread_;

  std::set<std::string> in_process_;
  port::RWMutex reg_st_m_;

};

}  // namespace ROCKSDB_NAMESPACE
