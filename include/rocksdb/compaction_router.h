#pragma once

#include <algorithm>
#include <chrono>
#include <memory>
#include <ostream>

#include "rocksdb/comparator.h"
#include "rocksdb/customizable.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/utilities/backports.h"

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
  virtual optional<T> next() = 0;
};

template <typename T>
class TraitPeekable : public TraitIterator<T> {
 public:
  virtual const T *peek() = 0;
};

template <typename Iter>
class Peekable : public TraitPeekable<typename Iter::Item> {
 public:
  using Item = typename Iter::Item;
  Peekable(Iter &&iter) : iter_(std::move(iter)) {}
  Peekable(const Peekable &) = delete;
  Peekable &operator=(const Peekable &) = delete;
  Peekable(Peekable &&rhs)
      : iter_(std::move(rhs.iter_)), peeked_(std::move(rhs.peeked_)) {}
  Peekable &operator=(Peekable &&rhs) {
    iter_ = std::move(rhs.iter_);
    peeked_ = std::move(rhs.peeked_);
    return *this;
  }
  ~Peekable() override = default;
  const Item *peek() override {
    if (peeked_.has_value()) return &peeked_.value();
    peeked_ = next();
    if (peeked_.has_value()) return &peeked_.value();
    return nullptr;
  }
  optional<Item> next() override {
    if (!peeked_.has_value()) return iter_.next();
    optional<Item> ret(std::move(peeked_.value()));
    peeked_.reset();
    return ret;
  }

 private:
  Iter iter_;
  optional<Item> peeked_;
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
  virtual void Access(Slice key, size_t vlen) = 0;
  virtual Iter LowerBound(Slice key) = 0;
  virtual size_t RangeHotSize(Slice smallest, Slice largest) = 0;
  virtual bool IsStablyHot(Slice key) = 0;

  // For statistics
  virtual void HitLevel(int level, rocksdb::Slice key) = 0;
};

}  // namespace ROCKSDB_NAMESPACE
