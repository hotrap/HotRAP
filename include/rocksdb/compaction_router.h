#pragma once

#include "rocksdb/customizable.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

struct HotRecInfo {
  rocksdb::Slice slice;
  double count;
  size_t vlen;
};

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
  virtual void AddHotness(int level, const rocksdb::Slice *key, size_t vlen,
			double weight) = 0;
  virtual void Access(int level, const Slice *key, size_t vlen) = 0;
	virtual bool MightRetain(int level) = 0;
  virtual void *NewIter(int level) = 0;
  virtual const rocksdb::HotRecInfo *Seek(void *iter, const rocksdb::Slice *key)
      = 0;
  virtual const HotRecInfo *NextHot(void *iter) = 0;
  virtual void DelIter(void *iter) = 0;
  virtual void DelRange(int level, const rocksdb::Slice *smallest,
			const rocksdb::Slice *largest) = 0;
};

}  // namespace ROCKSDB_NAMESPACE
