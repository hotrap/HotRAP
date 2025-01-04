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

class RouterIterator2SD : public TraitIterator<Elem> {
 public:
  RouterIterator2SD(RALT& ralt, const Compaction& c,
                      CompactionIterator& c_iter,
                      Slice start_level_smallest_user_key)
      : c_(c),
        ucmp_(c.column_family_data()->user_comparator()),
        iter_(c_iter),
        hot_iter_(ralt.LowerBound(start_level_smallest_user_key)),
        kvsize_promoted_(0),
        kvsize_retained_(0) {}
  ~RouterIterator2SD() {
    auto stats = c_.immutable_options()->stats;
    if (c_.start_level_path_id() == 0) {
      RecordTick(stats, Tickers::PROMOTED_2FDLAST_BYTES, kvsize_promoted_);
      RecordTick(stats, Tickers::RETAINED_FD_BYTES, kvsize_retained_);
    } else {
      RecordTick(stats, Tickers::PROMOTED_2SDFRONT_BYTES, kvsize_promoted_);
      RecordTick(stats, Tickers::RETAINED_SD_BYTES, kvsize_retained_);
    }
  }
  std::optional<Elem> next() override {
    std::optional<IKeyValueLevel> kv_ret = iter_.next();
    if (!kv_ret.has_value()) {
      return std::nullopt;
    }
    const IKeyValueLevel& kv = kv_ret.value();
    if (kv.level == c_.output_level())
      return std::make_optional<Elem>(Decision::kNextLevel, kv);
    assert(kv.level == c_.start_level());
    Decision decision = route(kv);
    if (decision == Decision::kStartLevel) {
      size_t kvsize = kv.key.size() + kv.value.size();
      kvsize_retained_ += kvsize;
    } else {
      assert(decision == Decision::kNextLevel);
    }
    return std::make_optional<Elem>(decision, kv);
  }

 private:
  Decision route(const IKeyValueLevel& kv) {
    // It is guaranteed that all versions of the same user key share the same
    // decision.
    const rocksdb::Slice* hot = hot_iter_.peek();
    while (hot != nullptr) {
      if (ucmp_->Compare(*hot, kv.ikey.user_key) >= 0) break;
      hot_iter_.next();
      hot = hot_iter_.peek();
    }
    Decision decision;
    if (hot && ucmp_->Compare(*hot, kv.ikey.user_key) == 0) {
      return Decision::kStartLevel;
    } else {
      return Decision::kNextLevel;
    }
  }

  const Compaction& c_;

  const Comparator* ucmp_;
  CompactionIterWrapper iter_;
  Peekable<RALT::Iter> hot_iter_;

  size_t kvsize_promoted_;
  size_t kvsize_retained_;
};

RouterIterator::RouterIterator(const Compaction& c, CompactionIterator& c_iter,
                               Slice start_level_smallest_user_key)
    : c_iter_(c_iter) {
  RALT* ralt = c.mutable_cf_options()->ralt.get();
  if (ralt == NULL) {
    iter_ = std::unique_ptr<IteratorWithoutRouter>(
        new IteratorWithoutRouter(c, c_iter));
  } else {
    const Version& version = *c.input_version();
    uint32_t start_tier = version.path_id(c.level());
    uint32_t latter_tier = version.path_id(c.output_level());
    if (start_tier != latter_tier) {
      assert(c.SupportsPerKeyPlacement());
      iter_ = std::unique_ptr<RouterIterator2SD>(new RouterIterator2SD(
          *ralt, c, c_iter, start_level_smallest_user_key));
    } else {
      iter_ = std::unique_ptr<IteratorWithoutRouter>(
          new IteratorWithoutRouter(c, c_iter));
    }
  }
  cur_ = iter_->next();
}

}  // namespace ROCKSDB_NAMESPACE
