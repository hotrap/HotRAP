#include "rocksdb/compaction_router.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/customizable_util.h"

namespace ROCKSDB_NAMESPACE {
Status CompactionRouter::CreateFromString(const ConfigOptions &config_options,
                                          const std::string &value,
                                          const CompactionRouter **result) {
  *result = (const CompactionRouter *)std::stoul(value, nullptr, 16);
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
