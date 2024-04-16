#pragma once

#include <utility>
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

struct in_place_t {};
constexpr in_place_t in_place{};

struct nullopt_t {};
constexpr nullopt_t nullopt{};

template <typename T>
class optional {
 public:
  optional() : has_value_(false) {}
  optional(nullopt_t) : has_value_(false) {}
  optional(T &&x) : has_value_(true), x_(std::move(x)) {}
  template <typename... Args>
  optional(in_place_t, Args &&...args)
      : has_value_(true), x_(std::forward<Args>(args)...) {}
  optional<T> &operator=(T &&x) {
    if (has_value_) x_.~T();
    has_value_ = true;
    x_ = std::move(x);
    return *this;
  }
  optional(const optional<T> &rhs) : has_value_(rhs.has_value_) {
    if (has_value_) {
      x_ = rhs.x_;
    }
  }
  optional<T> &operator=(const optional<T> &rhs) {
    this->~optional();
    has_value_ = rhs.has_value_;
    if (has_value_) {
      x_ = rhs.x_;
    }
    return *this;
  }
  optional(optional<T> &&rhs) : has_value_(rhs.has_value_) {
    if (has_value_) {
      x_ = std::move(rhs.x_);
      rhs.has_value_ = false;
    }
  }
  optional<T> &operator=(optional<T> &&rhs) {
    this->~optional();
    has_value_ = rhs.has_value_;
    if (has_value_) {
      x_ = std::move(rhs.x_);
      rhs.has_value_ = false;
    }
    return *this;
  }
  ~optional() {
    if (has_value_) {
      x_.~T();
    }
  }
  bool has_value() const { return has_value_; }
  T &value() { return x_; }
  const T &value() const { return x_; }
  void reset() {
    if (has_value_) {
      x_.~T();
      has_value_ = false;
    }
  }

 private:
  bool has_value_;
  union {
    T x_;
  };
};
template <typename T, typename... Args>
optional<T> make_optional(Args &&...args) {
  return optional<T>(in_place, std::forward<Args>(args)...);
}

}  // namespace ROCKSDB_NAMESPACE
