#pragma once

#include "db/compaction/compaction_iterator.h"

namespace ROCKSDB_NAMESPACE {

struct IKeyValueLevel {
  Slice key;
  ParsedInternalKey ikey;
  Slice value;
  int level;

  IKeyValueLevel(Slice arg_key, ParsedInternalKey arg_ikey, Slice arg_value,
                 int arg_level)
      : key(arg_key), ikey(arg_ikey), value(arg_value), level(arg_level) {}

  IKeyValueLevel(CompactionIterator& c_iter);
};

struct IKeyValue {
  Slice key;
  ParsedInternalKey ikey;
  Slice value;

  IKeyValue(const IKeyValueLevel& rhs)
      : IKeyValue(rhs.key, rhs.ikey, rhs.value) {}
  IKeyValue(Slice arg_key, ParsedInternalKey arg_ikey, Slice arg_value)
      : key(arg_key), ikey(arg_ikey), value(arg_value) {}
};

enum class Decision {
  kUndetermined,
  kNextLevel,
  kStartLevel,
};

struct Elem {
  Decision decision;
  IKeyValue kv;
  Elem(Decision arg_decision, const IKeyValueLevel& arg_kv)
      : decision(arg_decision), kv(arg_kv) {}
};

// Future work(hotrap): The caller should ZeroOutSequenceIfPossible if the final
// decision is kNextLevel
class CompactionIterWrapper : public TraitIterator<IKeyValueLevel> {
 public:
  CompactionIterWrapper(CompactionIterator& c_iter)
      : c_iter_(c_iter), first_(true) {}
  CompactionIterWrapper(const CompactionIterWrapper& rhs) = delete;
  CompactionIterWrapper& operator=(const CompactionIterWrapper& rhs) = delete;
  CompactionIterWrapper(CompactionIterWrapper&& rhs)
      : c_iter_(rhs.c_iter_), first_(rhs.first_) {}
  CompactionIterWrapper& operator=(const CompactionIterWrapper&& rhs) = delete;
  optional<IKeyValueLevel> next() override {
    if (first_) {
      first_ = false;
    } else {
      c_iter_.Next();
    }
    if (c_iter_.Valid())
      return make_optional<IKeyValueLevel>(c_iter_);
    else
      return nullopt;
  }

 private:
  CompactionIterator& c_iter_;
  bool first_;
};

class IteratorWithoutRouter : public TraitIterator<Elem> {
 public:
  IteratorWithoutRouter(const Compaction& c, CompactionIterator& c_iter)
      : c_iter_(c_iter) {}
  optional<Elem> next() override;

 private:
  CompactionIterWrapper c_iter_;
};

class RouterIteratorIntraTier : public TraitIterator<Elem> {
 public:
  RouterIteratorIntraTier(const Compaction& c, CompactionIterator& c_iter,
                          Slice start, Bound end, Tickers promotion_type)
      : c_(c),
        promotion_type_(promotion_type),
        promoted_bytes_(0),
        iter_(c_iter) {}
  ~RouterIteratorIntraTier() override {
    auto stats = c_.immutable_options()->stats;
    RecordTick(stats, promotion_type_, promoted_bytes_);
  }
  optional<Elem> next() override;

 private:
  const Compaction& c_;
  Tickers promotion_type_;
  size_t promoted_bytes_;
  CompactionIterWrapper iter_;
};

class RouterIteratorFD2SD : public TraitIterator<Elem> {
 public:
  RouterIteratorFD2SD(RALT& ralt, const Compaction& c,
                      CompactionIterator& c_iter, Slice start, Bound end)
      : c_(c),
        start_(start),
        end_(end),
        ucmp_(c.column_family_data()->user_comparator()),
        iter_(c_iter),
        hot_iter_(ralt.LowerBound(start_)),
        kvsize_promoted_(0),
        kvsize_retained_(0) {}
  ~RouterIteratorFD2SD();
  optional<Elem> next() override;

 private:
  Decision route(const IKeyValueLevel& kv);

  const Compaction& c_;
  const Slice start_;
  const Bound end_;

  const Comparator* ucmp_;
  CompactionIterWrapper iter_;
  Peekable<RALT::Iter> hot_iter_;

  size_t kvsize_promoted_;
  size_t kvsize_retained_;
};
class RouterIterator {
 public:
  RouterIterator(RALT* ralt, const Compaction& c, CompactionIterator& c_iter,
                 Slice start, Bound end);

	const CompactionIterator &c_iter() const { return c_iter_; }

  bool Valid() const { return cur_.has_value(); }
  void Next() {
    assert(Valid());
    cur_ = iter_->next();
  }
  Decision decision() const { return cur_.value().decision; }
  const Slice& key() const { return cur_.value().kv.key; }
  const ParsedInternalKey& ikey() const { return cur_.value().kv.ikey; }
  const Slice& user_key() const { return cur_.value().kv.ikey.user_key; }
  const Slice& value() const { return cur_.value().kv.value; }

 private:
  CompactionIterator &c_iter_;
  std::unique_ptr<TraitIterator<Elem>> iter_;
  optional<Elem> cur_;
};

}  // namespace ROCKSDB_NAMESPACE
