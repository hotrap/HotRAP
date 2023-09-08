#pragma once

#include <atomic>
#include <deque>

#include "util/mutexlock.h"
#include "util/rusty.h"

namespace ROCKSDB_NAMESPACE {

class TimerGuard;
class AtomicTimer {
 public:
  AtomicTimer() : count_(0), nsec_(0) {}
  uint64_t count() const { return count_.load(); }
  rusty::time::Duration time() const {
    return rusty::time::Duration::from_nanos(nsec_.load());
  }
  void add(rusty::time::Duration time) const {
    count_.fetch_add(1, std::memory_order_relaxed);
    nsec_.fetch_add(time.as_nanos(), std::memory_order_relaxed);
  }
  TimerGuard start() const;

 private:
  mutable std::atomic<uint64_t> count_;
  mutable std::atomic<uint64_t> nsec_;
};

class TimerGuard {
 public:
  ~TimerGuard() {
    if (timer_ == nullptr) return;
    timer_->add(rusty::time::Instant::now() - start_time_);
  }

 private:
  friend class AtomicTimer;
  TimerGuard(const AtomicTimer &timer)
      : timer_(&timer), start_time_(rusty::time::Instant::now()) {}
  const AtomicTimer *timer_;
  rusty::time::Instant start_time_;
};

inline TimerGuard AtomicTimer::start() const { return TimerGuard(*this); }

class Timers {
 public:
  Timers(size_t num) : timers_(num) {}
  size_t num() const { return timers_.size(); }
  const AtomicTimer &timer(size_t type) const { return timers_[type]; }

 private:
  std::vector<AtomicTimer> timers_;
};

class TimersPerLevel {
 public:
  TimersPerLevel(size_t num_timers_in_each_level)
      : num_timers_in_each_level_(num_timers_in_each_level) {}
  TimersPerLevel(const TimersPerLevel &) = delete;
  TimersPerLevel &operator=(const TimersPerLevel &) = delete;
  TimersPerLevel(TimersPerLevel &&) = delete;
  TimersPerLevel &operator=(TimersPerLevel &&) = delete;
  size_t num_levels() const { return v_.Read()->size(); }
  const Timers &timers_in_level(size_t level) const {
    {
      auto v = v_.Read();
      if (level < v->size()) return (*v)[level];
    }
    auto v = v_.Write();
    while (v->size() <= level)
      v->emplace_back(num_timers_in_each_level_);
    return (*v)[level];
  }
  const AtomicTimer &timer(size_t level, size_t type) const {
    return timers_in_level(level).timer(type);
  }

 private:
  rocksdb::RWMutexProtected<std::deque<Timers>> v_;
  const size_t num_timers_in_each_level_;
};

template <typename Type>
class TypedTimers {
 public:
  TypedTimers() : timers_(NUM) {}
  const AtomicTimer &timer(Type type) const {
    return timers_.timer(static_cast<size_t>(type));
  }
  const Timers &timers() const { return timers_; }

 private:
  static constexpr size_t NUM = static_cast<size_t>(Type::kEnd);
  Timers timers_;
};

template <typename Type>
class TypedTimersPerLevel {
 public:
  TypedTimersPerLevel() : v_(static_cast<size_t>(Type::kEnd)) {}
  const AtomicTimer &timer(size_t level, Type type) const {
    return v_.timer(level, static_cast<size_t>(type));
  }
  const TimersPerLevel &timers_per_level() const { return v_; }

 private:
  TimersPerLevel v_;
};
}  // namespace ROCKSDB_NAMESPACE
