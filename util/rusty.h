#pragma once

#include <chrono>
#include <cstdint>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
namespace rusty {
namespace time {
class Duration {
 public:
  static Duration from_nanos(uint64_t nsec) { return Duration(nsec); }
  uint64_t as_nanos() const { return nsec_; }

 private:
  Duration(uint64_t nsec) : nsec_(nsec) {}
  // uint128_t is not in standard C++ yet.
  uint64_t nsec_;
};
class Instant {
 public:
  static Instant now() { return std::chrono::steady_clock::now(); }
  Duration elapsed() const { return now() - *this; }
  Duration operator-(const Instant& earlier) {
    return Duration::from_nanos(
        std::chrono::duration_cast<std::chrono::nanoseconds>(time_ -
                                                             earlier.time_)
            .count());
  }

 private:
  Instant(std::chrono::steady_clock::time_point time) : time_(time) {}
  std::chrono::steady_clock::time_point time_;
};
}  // namespace time
}  // namespace rusty
}  // namespace ROCKSDB_NAMESPACE
