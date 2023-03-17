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

const char* timer_names[] = {
  "UpdateFilesByCompactionPri",
};
static_assert(sizeof(timer_names) / sizeof(const char*) == timer_num);

const char *per_level_timer_names[] = {
  "kPickSST",
};
static_assert(sizeof(per_level_timer_names) / sizeof(const char *) ==
  per_level_timer_num);

}  // namespace ROCKSDB_NAMESPACE
