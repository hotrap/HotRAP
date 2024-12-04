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
  optional<IKeyValueLevel> next() override {
    if (first_) {
      first_ = false;
    } else {
      c_iter_.Next();
    }
    if (c_iter_.Valid())
      return make_optional<IKeyValueLevel>(c_iter_);
    else
      return nullopt;
  }

 private:
  CompactionIterator& c_iter_;
  bool first_;
};

class IteratorWithoutRouter : public TraitIterator<Elem> {
 public:
  IteratorWithoutRouter(const Compaction& c, CompactionIterator& c_iter)
      : c_iter_(c_iter) {}
  optional<Elem> next() override {
    optional<IKeyValueLevel> ret = c_iter_.next();
    if (ret.has_value())
      return make_optional<Elem>(Decision::kNextLevel, ret.value());
    else
      return nullopt;
  }

 private:
  CompactionIterWrapper c_iter_;
};

class RouterIteratorIntraTier : public TraitIterator<Elem> {
 public:
  RouterIteratorIntraTier(const Compaction& c, CompactionIterator& c_iter,
                          Slice start, Bound end, Tickers promotion_type)
      : c_(c),
        promotion_type_(promotion_type),
        promoted_bytes_(0),
        iter_(c_iter) {}
  ~RouterIteratorIntraTier() override {
    auto stats = c_.immutable_options()->stats;
    RecordTick(stats, promotion_type_, promoted_bytes_);
  }
  optional<Elem> next() override {
    optional<IKeyValueLevel> ret = iter_.next();
    if (!ret.has_value()) {
      return nullopt;
    }
    IKeyValueLevel& kv = ret.value();
    if (kv.level != -1) {
      return make_optional<Elem>(Decision::kNextLevel, kv);
    }
    promoted_bytes_ += kv.key.size() + kv.value.size();
    return make_optional<Elem>(Decision::kNextLevel, kv);
  }

 private:
  const Compaction& c_;
  Tickers promotion_type_;
  size_t promoted_bytes_;
  CompactionIterWrapper iter_;
};

class RouterIteratorFD2SD : public TraitIterator<Elem> {
 public:
  RouterIteratorFD2SD(RALT& ralt, const Compaction& c,
                      CompactionIterator& c_iter, Slice start, Bound end)
      : c_(c),
        start_(start),
        end_(end),
        ucmp_(c.column_family_data()->user_comparator()),
        iter_(c_iter),
        hot_iter_(ralt.LowerBound(start_)),
        kvsize_promoted_(0),
        kvsize_retained_(0) {}
  ~RouterIteratorFD2SD() {
    auto stats = c_.immutable_options()->stats;
    RecordTick(stats, Tickers::PROMOTED_2FDLAST_BYTES, kvsize_promoted_);
    RecordTick(stats, Tickers::RETAINED_BYTES, kvsize_retained_);
  }
  optional<Elem> next() override {
    optional<IKeyValueLevel> kv_ret = iter_.next();
    if (!kv_ret.has_value()) {
      return nullopt;
    }
    const IKeyValueLevel& kv = kv_ret.value();
    RangeBounds range{
        .start =
            Bound{
                .user_key = start_,
                .excluded = false,
            },
        .end = end_,
    };
    if (!range.contains(kv.ikey.user_key, ucmp_)) {
      return make_optional<Elem>(Decision::kNextLevel, kv);
    }
    Decision decision = route(kv);
    if (decision == Decision::kStartLevel) {
      size_t kvsize = kv.key.size() + kv.value.size();
      if (kv.level == -1 || kv.level == c_.output_level()) {
        kvsize_promoted_ += kvsize;
      } else {
        assert(kv.level == c_.start_level());
        kvsize_retained_ += kvsize;
      }
    } else {
      assert(decision == Decision::kNextLevel);
    }
    return make_optional<Elem>(decision, kv);
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
  const Slice start_;
  const Bound end_;

  const Comparator* ucmp_;
  CompactionIterWrapper iter_;
  Peekable<RALT::Iter> hot_iter_;

  size_t kvsize_promoted_;
  size_t kvsize_retained_;
};

RouterIterator::RouterIterator(const Compaction& c, CompactionIterator& c_iter,
                               Slice start, Bound end)
    : c_iter_(c_iter) {
  int start_level = c.level();
  int latter_level = c.output_level();
  RALT* ralt = c.mutable_cf_options()->ralt.get();
  if (ralt == NULL) {
    // Future work(hotrap): Handle the case that it's not empty, which is
    // possible when ralt was not NULL but then is set to NULL.
    assert(c.cached_records_to_promote().empty());
    iter_ = std::unique_ptr<IteratorWithoutRouter>(
        new IteratorWithoutRouter(c, c_iter));
  } else {
    const Version& version = *c.input_version();
    uint32_t start_tier = version.path_id(start_level);
    uint32_t latter_tier = version.path_id(latter_level);
    if (start_tier != latter_tier) {
      assert(c.SupportsPerKeyPlacement());
      iter_ = std::unique_ptr<RouterIteratorFD2SD>(
          new RouterIteratorFD2SD(*ralt, c, c_iter, start, end));
    } else if (version.path_id(latter_level + 1) != latter_tier) {
      iter_ =
          std::unique_ptr<RouterIteratorIntraTier>(new RouterIteratorIntraTier(
              c, c_iter, start, end, Tickers::PROMOTED_2FDLAST_BYTES));
    } else {
      iter_ =
          std::unique_ptr<RouterIteratorIntraTier>(new RouterIteratorIntraTier(
              c, c_iter, start, end, Tickers::PROMOTED_2SDFRONT_BYTES));
    }
  }
  cur_ = iter_->next();
}

}  // namespace ROCKSDB_NAMESPACE
