#include "router_iterator.h"

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

IKeyValueLevel::IKeyValueLevel(CompactionIterator& c_iter)
    : key(c_iter.key()), ikey(c_iter.ikey()) {
  auto ret = parse_level_value(c_iter.value());
  value = ret.second;
  level = ret.first;
}

// Future work(hotrap): The caller should ZeroOutSequenceIfPossible if the final
// decision is kNextLevel
class CompactionIterWrapper : public TraitIterator<IKeyValueLevel> {
 public:
  CompactionIterWrapper(CompactionIterator& c_iter)
      : c_iter_(c_iter), first_(true) {}
  CompactionIterWrapper(const CompactionIterWrapper& rhs) = delete;
  CompactionIterWrapper& operator=(const CompactionIterWrapper& rhs) = delete;
  CompactionIterWrapper(CompactionIterWrapper&& rhs)
      : c_iter_(rhs.c_iter_), first_(rhs.first_) {}
  CompactionIterWrapper& operator=(const CompactionIterWrapper&& rhs) = delete;
  std::optional<IKeyValueLevel> next() override {
    if (first_) {
      first_ = false;
    } else {
      c_iter_.Next();
    }
    if (c_iter_.Valid())
      return std::make_optional<IKeyValueLevel>(c_iter_);
    else
      return std::nullopt;
  }

 private:
  CompactionIterator& c_iter_;
  bool first_;
};

class IteratorWithoutRouter : public TraitIterator<Elem> {
 public:
  IteratorWithoutRouter(const Compaction& c, CompactionIterator& c_iter)
      : c_iter_(c_iter) {}
  std::optional<Elem> next() override {
    std::optional<IKeyValueLevel> ret = c_iter_.next();
    if (ret.has_value())
      return std::make_optional<Elem>(Decision::kNextLevel, ret.value());
    else
      return std::nullopt;
  }

 private:
  CompactionIterWrapper c_iter_;
};

RouterIterator::RouterIterator(const Compaction& c, CompactionIterator& c_iter,
                               Slice start_level_smallest_user_key)
    : c_iter_(c_iter) {
  RALT* ralt = c.mutable_cf_options()->ralt.get();
  iter_ = std::unique_ptr<IteratorWithoutRouter>(
      new IteratorWithoutRouter(c, c_iter));
  cur_ = iter_->next();
}

}  // namespace ROCKSDB_NAMESPACE
