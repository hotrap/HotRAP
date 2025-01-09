#include "router_iterator.h"

#include "db/compaction/subcompaction_state.h"

namespace ROCKSDB_NAMESPACE {

static std::pair<int, Slice> parse_level_value(Slice input_value) {
  assert(input_value.size() ==
         sizeof(int) + sizeof(const char*) + sizeof(size_t));
  const char* buf = input_value.data();
  int level = *(int*)buf;
  buf += sizeof(int);
  const char* data = *(const char**)buf;
  buf += sizeof(const char*);
  size_t len = *(size_t*)buf;
  return std::make_pair(level, Slice(data, len));
}

class IteratorWithoutRouter : public RouterIterator {
 public:
  IteratorWithoutRouter(CompactionIterator& c_iter) : c_iter_(c_iter) {}

  const CompactionIterator& c_iter() const override { return c_iter_; }

  bool Valid() const override { return c_iter_.Valid(); }
  void Next() override { return c_iter_.Next(); }
  Decision decision() const override { return Decision::kNextLevel; }
  Slice key() const override { return c_iter_.key(); }
  const ParsedInternalKey& ikey() const override { return c_iter_.ikey(); }
  Slice user_key() const override { return c_iter_.user_key(); }
  Slice value() const override {
    return parse_level_value(c_iter_.value()).second;
  }

 private:
  CompactionIterator& c_iter_;
};

std::unique_ptr<RouterIterator> NewRouterIterator(
    SubcompactionState& sub_compact, CompactionIterator& c_iter) {
  return std::make_unique<IteratorWithoutRouter>(c_iter);
}

}  // namespace ROCKSDB_NAMESPACE
