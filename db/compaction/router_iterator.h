#pragma once

#include "db/compaction/compaction_iterator.h"

namespace ROCKSDB_NAMESPACE {

class SubcompactionState;

enum class Decision {
  kUndetermined,
  kNextLevel,
  kStartLevel,
};

class RouterIterator {
 public:
  virtual ~RouterIterator() {}

  virtual const CompactionIterator& c_iter() const = 0;

  virtual bool Valid() const = 0;
  virtual void Next() = 0;
  virtual Decision decision() const = 0;
  virtual Slice key() const = 0;
  virtual const ParsedInternalKey& ikey() const = 0;
  virtual Slice user_key() const = 0;
  virtual Slice value() const = 0;
};

std::unique_ptr<RouterIterator> NewRouterIterator(
    SubcompactionState& sub_compact, CompactionIterator& c_iter);

}  // namespace ROCKSDB_NAMESPACE
