#include "rocksdb/compaction_router.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/customizable_util.h"

namespace ROCKSDB_NAMESPACE {
Status CompactionRouter::CreateFromString(const ConfigOptions& config_options,
                                          const std::string& value,
                                          const CompactionRouter** result) {
  CompactionRouter* router = const_cast<CompactionRouter*>(*result);
  Status status = LoadStaticObject<CompactionRouter>(config_options, value,
                                                     nullptr, &router);
  if (status.ok()) {
    *result = const_cast<CompactionRouter*>(router);
  }
  return status;
}
}  // namespace ROCKSDB_NAMESPACE
