#include "rocksdb/ralt.h"

#include "rocksdb/options.h"
#include "rocksdb/utilities/customizable_util.h"

namespace ROCKSDB_NAMESPACE {
Status RALT::CreateFromString(const ConfigOptions &config_options,
                              const std::string &value, const RALT **result) {
  *result = (const RALT *)std::stoul(value, nullptr, 16);
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
