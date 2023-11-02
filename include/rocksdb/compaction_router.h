#pragma once

#include <algorithm>
#include <chrono>
#include <memory>
#include <ostream>

#include "rocksdb/comparator.h"
#include "rocksdb/customizable.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

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

struct HotRecInfo {
  Slice key;
  bool stable;
};

class CompactionRouter : public Customizable {
 public:
  using Iter = std::unique_ptr<TraitIterator<HotRecInfo>>;
  virtual ~CompactionRouter() {}
  static const char *Type() { return "CompactionRouter"; }
  static Status CreateFromString(const ConfigOptions &config_options,
                                 const std::string &name,
                                 const CompactionRouter **result);
  const char *Name() const override = 0;
  virtual size_t Tier(int level) = 0;
  virtual void Access(int level, Slice key, size_t vlen) = 0;
  virtual Iter LowerBound(Slice key) = 0;
  virtual size_t RangeHotSize(Slice smallest, Slice largest) = 0;
  virtual bool IsStablyHot(Slice key) = 0;
};

}  // namespace ROCKSDB_NAMESPACE
