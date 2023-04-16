#pragma once

#include <algorithm>
#include <chrono>
#include <memory>
#include <ostream>

#include "rocksdb/customizable.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

struct HotRecInfo {
  rocksdb::Slice slice;
  double count;
  size_t vlen;
};

enum class TimerType : size_t {
  kUpdateFilesByCompactionPri = 0,
  kGetKeyValueFromLevelsBelow,
  kEnd,
};
constexpr size_t timer_num = static_cast<size_t>(TimerType::kEnd);

extern const char *timer_names[];

enum class PerLevelTimerType {
  kPickSST,
  kEnd,
};
constexpr size_t per_level_timer_num =
    static_cast<size_t>(PerLevelTimerType::kEnd);
extern const char *per_level_timer_names[];

struct TimerStatus {
  const char *name;
  uint64_t count;
  uint64_t nsec;
};

template <typename T>
class PointerIter {
 public:
  using value_type = T;
  PointerIter() {}
  PointerIter(const PointerIter &) = delete;
  PointerIter &operator=(const PointerIter &) = delete;
  virtual ~PointerIter() = default;
  // The returned reference should stay valid until the next call to it.
  virtual T *next() = 0;
};

template <typename Iter>
class PeekablePointerIter {
 public:
  using value_type = typename Iter::value_type;
  PeekablePointerIter() : cur_(NULL) {}
  PeekablePointerIter(std::unique_ptr<PointerIter<value_type>> &&iter)
      : iter_(std::move(iter)), cur_(iter_->next()) {}
  PeekablePointerIter(const PeekablePointerIter &) = delete;
  PeekablePointerIter &operator=(const PeekablePointerIter &) = delete;
  PeekablePointerIter(PeekablePointerIter &&rhs)
      : iter_(std::move(rhs.iter_)), cur_(rhs.cur_) {}
  PeekablePointerIter &operator=(PeekablePointerIter &&rhs) {
    iter_ = std::move(rhs.iter_);
    cur_ = rhs.cur_;
    return *this;
  }
  bool has_iter() const { return iter_ != nullptr; }
  const value_type *peek() const { return cur_; }
  value_type *next() {
    value_type *ret = cur_;
    cur_ = iter_->next();
    return ret;
  }

 private:
  std::unique_ptr<PointerIter<value_type>> iter_;
  value_type *cur_;
};

class CompactionRouter : public Customizable {
 public:
  using Iter = PointerIter<const HotRecInfo>;
  enum class Decision {
    kUndetermined,
    kNextLevel,
    kCurrentLevel,
  };
  CompactionRouter() : num_used_levels_(0) {}
  virtual ~CompactionRouter() {}
  static const char *Type() { return "CompactionRouter"; }
  static Status CreateFromString(const ConfigOptions &config_options,
                                 const std::string &name,
                                 const CompactionRouter **result);
  const char *Name() const override = 0;
  virtual size_t Tier(int level) = 0;
  virtual void AddHotness(size_t tier, const rocksdb::Slice &key, size_t vlen,
                          double weight) = 0;
  virtual void Access(int level, const Slice &key, size_t vlen) = 0;
  virtual std::unique_ptr<Iter> LowerBound(size_t tier,
                                           const rocksdb::Slice &key) = 0;
  virtual void DelRange(size_t tier, const rocksdb::Slice &smallest,
                        const rocksdb::Slice &largest) = 0;
  virtual size_t RangeHotSize(size_t tier, const rocksdb::Slice &smallest,
                              const rocksdb::Slice &largest) = 0;

  static std::chrono::steady_clock::time_point Start() {
    return std::chrono::steady_clock::now();
  }
  void Stop(TimerType type, std::chrono::steady_clock::time_point start_time) {
    auto end_time = std::chrono::steady_clock::now();
    auto nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time -
                                                                     start_time)
                    .count();
    AddTimer(type, nsec);
  }
  void AddTimer(TimerType type, uint64_t nsec) {
    timers_[static_cast<size_t>(type)].add(nsec);
  }
  std::vector<TimerStatus> CollectTimers() {
    std::vector<TimerStatus> ret;
    for (size_t id = 0; id < timer_num; id += 1) {
      const auto &timer = timers_[id];
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
        const auto &timer = per_level_timers_[level][id];
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
  void Stop(int level, PerLevelTimerType type,
            std::chrono::steady_clock::time_point start_time) {
    auto end_time = std::chrono::steady_clock::now();
    auto nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time -
                                                                     start_time)
                    .count();
    AddTimerInLevel(level, type, nsec);
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
