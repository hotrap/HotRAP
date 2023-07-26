#pragma once

#include <algorithm>
#include <chrono>
#include <memory>
#include <ostream>

#include "rocksdb/comparator.h"
#include "rocksdb/customizable.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

enum class TimerType : size_t {
  kUpdateFilesByCompactionPri = 0,
  kGetKeyValueFromLevelsBelow,
  kEnd,
};
constexpr size_t timer_num = static_cast<size_t>(TimerType::kEnd);

extern const char *timer_names[];

enum class PerLevelTimerType {
  kPickSST,
  kProcessKeyValueCompaction,  // start_level
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

class SingleTimer {
 public:
  SingleTimer() : count_(0), nsec_(0) {}
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

class TimerGuard {
 public:
  TimerGuard() : timer_(nullptr) {}
  TimerGuard(SingleTimer &timer)
      : timer_(&timer), start_time_(std::chrono::steady_clock::now()) {}
  ~TimerGuard() {
    if (timer_ == nullptr) return;
    auto end_time = std::chrono::steady_clock::now();
    auto nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    end_time - start_time_)
                    .count();
    timer_->add(nsec);
  }

 private:
  SingleTimer *timer_;
  std::chrono::steady_clock::time_point start_time_;
};

template <typename T>
class TraitIterator {
 public:
  using Item = T;
  TraitIterator() {}
  TraitIterator(const TraitIterator &) = delete;
  TraitIterator &operator=(const TraitIterator &) = delete;
  virtual ~TraitIterator() = default;
  // TODO: Return std::optional<T> if upgrade to C++17
  virtual std::unique_ptr<T> next() = 0;
};
template <typename T>
class TraitObjIterator : public TraitIterator<T> {
 public:
  using Item = T;
  TraitObjIterator(const TraitObjIterator<T> &) = delete;
  TraitObjIterator<T> &operator=(const TraitObjIterator<T> &) = delete;
  TraitObjIterator(TraitObjIterator<T> &&rhs) : iter_(std::move(rhs.iter_)) {}
  TraitObjIterator<T> &operator=(TraitObjIterator<T> &&rhs) {
    iter_ = std::move(rhs.iter_);
    return *this;
  }
  TraitObjIterator(std::unique_ptr<TraitIterator<Item>> &&iter)
      : iter_(std::move(iter)) {}
  std::unique_ptr<T> next() override { return iter_->next(); }

 private:
  std::unique_ptr<TraitIterator<Item>> iter_;
};

template <typename Iter>
class Peekable : TraitIterator<typename Iter::Item> {
 public:
  using Item = typename Iter::Item;
  Peekable(Iter &&iter) : iter_(std::move(iter)), cur_(iter_.next()) {}
  Peekable(const Peekable &) = delete;
  Peekable &operator=(const Peekable &) = delete;
  Peekable(Peekable &&rhs)
      : iter_(std::move(rhs.iter_)), cur_(std::move(rhs.cur_)) {}
  Peekable &operator=(Peekable &&rhs) {
    iter_ = std::move(rhs.iter_);
    cur_ = std::move(rhs.cur_);
    return *this;
  }
  ~Peekable() override = default;
  const Item *peek() const { return cur_.get(); }
  std::unique_ptr<Item> next() override {
    std::unique_ptr<Item> ret = std::move(cur_);
    cur_ = iter_.next();
    return ret;
  }

 private:
  Iter iter_;
  std::unique_ptr<Item> cur_;
};

template <typename T>
class VecIter {
 public:
  VecIter(const std::vector<T> &v) : v_(v), it_(v_.cbegin()) {}
  const T *next() {
    if (it_ == v_.end()) return nullptr;
    const T *ret = &*it_;
    ++it_;
    return ret;
  }
  const T *peek() {
    if (it_ == v_.end())
      return nullptr;
    else
      return &*it_;
  }

 private:
  const std::vector<T> &v_;
  typename std::vector<T>::const_iterator it_;
};

struct Bound {
  Slice user_key;
  bool excluded;
};

struct RangeBounds {
  Bound start, end;
  bool contains(Slice user_key, const Comparator *ucmp) {
    int res = ucmp->Compare(user_key, start.user_key);
    if (start.excluded) {
      if (res <= 0) return false;
    } else {
      if (res < 0) return false;
    }
    res = ucmp->Compare(user_key, end.user_key);
    if (end.excluded) {
      if (res >= 0) return false;
    } else {
      if (res > 0) return false;
    }
    return true;
  }
};

class CompactionRouter : public Customizable {
 public:
  using Iter = TraitObjIterator<Slice>;
  CompactionRouter() : num_used_levels_(0) {}
  virtual ~CompactionRouter() {}
  static const char *Type() { return "CompactionRouter"; }
  static Status CreateFromString(const ConfigOptions &config_options,
                                 const std::string &name,
                                 const CompactionRouter **result);
  const char *Name() const override = 0;
  virtual size_t Tier(int level) = 0;
  virtual void Access(int level, Slice key, size_t vlen) = 0;
  virtual Iter LowerBound(size_t tier, Slice key) = 0;
  virtual void TransferRange(size_t target_tier, size_t source_tier,
                             RangeBounds range) = 0;
  virtual size_t RangeHotSize(size_t tier, Slice smallest, Slice largest) = 0;

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
    MarkLevelUsed(level);
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
  TimerGuard GetTimerGuard(TimerType type) {
    return TimerGuard(timers_[static_cast<size_t>(type)]);
  }
  TimerGuard GetTimerGuard(int level, PerLevelTimerType type) {
    level = std::min((size_t)level, MAX_LEVEL_NUM - 1);
    MarkLevelUsed(level);
    return TimerGuard(per_level_timers_[level][static_cast<size_t>(type)]);
  }

 private:
  void MarkLevelUsed(int level) {
    int num_used_levels = num_used_levels_.load(std::memory_order_relaxed);
    while (num_used_levels <= level) {
      num_used_levels_.compare_exchange_weak(num_used_levels, level + 1,
                                             std::memory_order_relaxed);
    }
  }
  SingleTimer timers_[timer_num];
  static constexpr size_t MAX_LEVEL_NUM = 10;
  SingleTimer per_level_timers_[MAX_LEVEL_NUM][per_level_timer_num];
  std::atomic<int> num_used_levels_;
};

}  // namespace ROCKSDB_NAMESPACE
