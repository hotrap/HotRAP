#pragma once

#include "rocksdb/customizable.h"
#include "rocksdb/rocksdb_namespace.h"

#include <algorithm>
#include <ostream>

namespace ROCKSDB_NAMESPACE {

struct HotRecInfo {
  rocksdb::Slice slice;
  double count;
  size_t vlen;
};

enum class TimerType {
  kUpdateFilesByCompactionPri,
  kEnd,
};
constexpr size_t timer_num = static_cast<size_t>(TimerType::kEnd);

extern const char* timer_names[];

enum class PerLevelTimerType {
  kPickSST,
  kEnd,
};
constexpr size_t per_level_timer_num =
  static_cast<size_t>(PerLevelTimerType::kEnd);
extern const char *per_level_timer_names[];

struct TimerStatus {
  const char* name;
  uint64_t count;
  uint64_t nsec;
};

class CompactionRouter : public Customizable {
 public:
  enum class Decision {
    kUndetermined,
    kNextLevel,
    kCurrentLevel,
  };
  CompactionRouter() : num_used_levels_(0) {}
  virtual ~CompactionRouter() {}
  static const char *Type() { return "CompactionRouter"; }
  static Status CreateFromString(const ConfigOptions &config_options, const std::string &name, const CompactionRouter **result);
  const char *Name() const override = 0;
  virtual size_t Tier(int level) = 0;
  virtual void AddHotness(size_t tier, const rocksdb::Slice &key, size_t vlen, double weight) = 0;
  virtual void Access(int level, const Slice &key, size_t vlen) = 0;
  virtual void *NewIter(size_t tier) = 0;
  virtual const rocksdb::HotRecInfo *Seek(void *iter, const rocksdb::Slice &key) = 0;
  virtual const HotRecInfo *NextHot(void *iter) = 0;
  virtual void DelIter(void *iter) = 0;
  virtual void DelRange(size_t tier, const rocksdb::Slice &smallest, const rocksdb::Slice &largest) = 0;
  virtual size_t RangeHotSize(size_t tier, const rocksdb::Slice &smallest, const rocksdb::Slice &largest) = 0;

  void AddTimer(TimerType type, uint64_t nsec) {
    timers_[static_cast<size_t>(type)].add(nsec);
  }
  std::vector<TimerStatus> CollectTimers() {
    std::vector<TimerStatus> ret;
    for (size_t id = 0; id < timer_num; id += 1) {
      const auto& timer = timers_[id];
      ret.push_back(TimerStatus{timer_names[id], timer.count(), timer.nsec()});
    }
    return ret;
  }
  std::vector<std::vector<TimerStatus>> CollectTimersInAllLevels() {
    std::vector<std::vector<TimerStatus>> ret;
    size_t num_used_levels = num_used_levels_.load(std::memory_order_relaxed);
    for (size_t level = 0; level < num_used_levels; ++level) {
      std::vector<TimerStatus> timers;
      for (size_t id = 0; id < per_level_timer_num; id += 1) {
        const auto& timer = per_level_timers_[level][id];
        timers.push_back(TimerStatus{per_level_timer_names[id], timer.count(),
            timer.nsec()});
      }
      ret.push_back(std::move(timers));
    }
    return ret;
  }
  void AddTimerInLevel(int level, PerLevelTimerType type, uint64_t nsec) {
    level = std::min((size_t)level, MAX_LEVEL_NUM - 1);
    int num_used_levels = num_used_levels_.load(std::memory_order_relaxed);
    while (num_used_levels <= level) {
      num_used_levels_.compare_exchange_weak(num_used_levels, level + 1,
        std::memory_order_relaxed);
    }
    per_level_timers_[level][static_cast<size_t>(type)].add(nsec);
  }
 private:
  class Timer {
   public:
    Timer() : count_(0), nsec_(0) {}
    uint64_t count() const { return count_.load(); }
    uint64_t nsec() const { return nsec_.load(); }
    void add(uint64_t nsec) {
      count_.fetch_add(1, std::memory_order_relaxed);
      nsec_.fetch_add(nsec, std::memory_order_relaxed);
    }
   private:
    std::atomic<uint64_t> count_;
    std::atomic<uint64_t> nsec_;
  };
  Timer timers_[timer_num];
  static constexpr size_t MAX_LEVEL_NUM = 10;
  Timer per_level_timers_[MAX_LEVEL_NUM][per_level_timer_num];
  std::atomic<int> num_used_levels_;
};

}  // namespace ROCKSDB_NAMESPACE
