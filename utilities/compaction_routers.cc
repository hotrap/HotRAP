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

const char *timer_names[] = {
    "UpdateFilesByCompactionPri",
    "GetKeyValueFromLevelsBelow",
};
static_assert(sizeof(timer_names) / sizeof(const char *) == timer_num);

const char *per_level_timer_names[] = {
    "kPickSST",
    "ProcessKeyValueCompaction",
};
static_assert(sizeof(per_level_timer_names) / sizeof(const char *) ==
              per_level_timer_num);

}  // namespace ROCKSDB_NAMESPACE
