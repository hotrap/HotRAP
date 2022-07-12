#pragma once

#include "rocksdb/customizable.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

class CompactionRouter : public Customizable {
public:
  enum class Decision {
    kUndetermined,
    kNextLevel,
    kCurrentLevel,
  };
  virtual ~CompactionRouter() {}
  static const char* Type() { return "CompactionRouter"; }
  static Status CreateFromString(const ConfigOptions& config_options,
                                 const std::string& name,
                                 const CompactionRouter** result);
  const char* Name() const override = 0;
  virtual void Access(int level, const Slice *key, size_t vlen) = 0;
	virtual void *NewIter(int level) = 0;
  virtual void DelIter(void *iter) = 0;
  virtual Decision Route(void *iter, const Slice *key) = 0;
};

}  // namespace ROCKSDB_NAMESPACE
