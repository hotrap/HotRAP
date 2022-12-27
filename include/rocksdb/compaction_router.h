#pragma once

#include "rocksdb/customizable.h"
#include "rocksdb/rocksdb_namespace.h"

#include <ostream>

namespace ROCKSDB_NAMESPACE {

struct HotRecInfo {
  rocksdb::Slice slice;
  double count;
  size_t vlen;
};

enum class TimerType {
  kPickSST,
  kEnd,
};
constexpr size_t timer_num = static_cast<size_t>(TimerType::kEnd);

extern const char* timer_names[];

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

  void TimerAdd(TimerType type, uint64_t nsec) {
    timers_[static_cast<size_t>(type)].add(nsec);
  }
  std::vector<TimerStatus> TimerCollect() {
    std::vector<TimerStatus> timers;
    for (size_t id = 0; id < timer_num; id += 1) {
      const auto& timer = timers_[id];
      timers.push_back(TimerStatus{timer_names[id], timer.count(),
          timer.nsec()});
    }
    return timers;
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
  Timer timers_[static_cast<size_t>(TimerType::kEnd)];
};

}  // namespace ROCKSDB_NAMESPACE
