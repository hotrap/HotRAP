#pragma once

#include <algorithm>
#include <chrono>
#include <memory>
#include <ostream>

#include "rocksdb/comparator.h"
#include "rocksdb/customizable.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/types.h"
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

template <typename Item>
class Peekable<std::unique_ptr<TraitIterator<Item>>>
    : public TraitPeekable<Item> {
 public:
  Peekable(std::unique_ptr<TraitIterator<Item>> &&iter)
      : iter_(std::move(iter)) {}

  ~Peekable() override = default;

  optional<Item> next() final override {
    if (!peeked_.has_value()) return iter_->next();
    optional<Item> ret(std::move(peeked_.value()));
    peeked_.reset();
    return ret;
  }
  const Item *peek() final override {
    if (peeked_.has_value()) return &peeked_.value();
    peeked_ = next();
    if (peeked_.has_value()) return &peeked_.value();
    return nullptr;
  }

 private:
  std::unique_ptr<TraitIterator<Item>> iter_;
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
  Slice first;  // If empty, then the range is [last, last]
  Slice last;
};

class RALT : public Customizable {
 public:
  using Iter = std::unique_ptr<TraitIterator<HotRecInfo>>;
  virtual ~RALT() {}
  static Status CreateFromString(const ConfigOptions &config_options,
                                 const std::string &name, const RALT **result);
  const char *Name() const override = 0;
  virtual void Access(Slice key, size_t vlen) = 0;
  // sequence > 0 means the range is promoted.
  // sequence = 0 means the range is not promoted.
  virtual void AccessRange(Slice first, Slice last, uint64_t num_bytes,
                           SequenceNumber sequence) = 0;
  virtual Iter LowerBound(Slice key) = 0;
  virtual uint64_t RangeHotSize(Slice smallest, Slice largest) = 0;
  virtual bool IsHot(Slice key) = 0;
  virtual bool IsHot(Slice first, Slice last) = 0;

  // If "key" is in a promoted range, return the last promoted key in that
  // promoted range. If "key" is not in a promoted range, then return an empty
  // string.
  virtual std::string LastPromoted(Slice key, SequenceNumber seq) = 0;

  // For statistics
  virtual void HitLevel(int, rocksdb::Slice){};
  virtual void ScanResult(bool){};
};

}  // namespace ROCKSDB_NAMESPACE
