#pragma once

#include "db/compaction/compaction_iterator.h"

namespace ROCKSDB_NAMESPACE {

class SubcompactionState;

struct IKeyValue {
  Slice key;
  ParsedInternalKey ikey;
  Slice value;

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
  Elem(Decision arg_decision, const IKeyValue& arg_kv)
      : decision(arg_decision), kv(arg_kv) {}
};

class RouterIterator {
 public:
  RouterIterator(SubcompactionState& sub_compact, CompactionIterator& c_iter);

  const CompactionIterator& c_iter() const { return c_iter_; }

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
  CompactionIterator& c_iter_;
  std::unique_ptr<TraitIterator<Elem>> iter_;
  std::optional<Elem> cur_;
};

}  // namespace ROCKSDB_NAMESPACE
