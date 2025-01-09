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

struct IKeyValueLevel {
  Slice key;
  ParsedInternalKey ikey;
  Slice value;
  int level;

  IKeyValueLevel(Slice arg_key, ParsedInternalKey arg_ikey, Slice arg_value,
                 int arg_level)
      : key(arg_key), ikey(arg_ikey), value(arg_value), level(arg_level) {}

  IKeyValueLevel(CompactionIterator& c_iter)
      : key(c_iter.key()), ikey(c_iter.ikey()) {
    auto ret = parse_level_value(c_iter.value());
    value = ret.second;
    level = ret.first;
  }
};

struct IKeyValue {
  Slice key;
  ParsedInternalKey ikey;
  Slice value;

  IKeyValue(const IKeyValueLevel& rhs)
      : IKeyValue(rhs.key, rhs.ikey, rhs.value) {}
  IKeyValue(Slice arg_key, ParsedInternalKey arg_ikey, Slice arg_value)
      : key(arg_key), ikey(arg_ikey), value(arg_value) {}
};

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

struct Elem {
  Decision decision;
  IKeyValue kv;
  Elem(Decision arg_decision, const IKeyValueLevel& arg_kv)
      : decision(arg_decision), kv(arg_kv) {}
};

class RouterIteratorIntraFD : public TraitIterator<Elem> {
 public:
  RouterIteratorIntraFD(const Compaction& c, CompactionIterator& c_iter)
      : c_(c), promoted_bytes_(0), iter_(c_iter) {}
  ~RouterIteratorIntraFD() override {
    auto stats = c_.immutable_options()->stats;
    RecordTick(stats, Tickers::PROMOTED_2FDLAST_BYTES, promoted_bytes_);
  }
  std::optional<Elem> next() override {
    std::optional<IKeyValueLevel> ret = iter_.next();
    if (!ret.has_value()) {
      return std::nullopt;
    }
    IKeyValueLevel& kv = ret.value();
    if (kv.level != -1) {
      return std::make_optional<Elem>(Decision::kNextLevel, kv);
    }
    promoted_bytes_ += kv.key.size() + kv.value.size();
    return std::make_optional<Elem>(Decision::kNextLevel, kv);
  }

 private:
  const Compaction& c_;
  size_t promoted_bytes_;
  CompactionIterWrapper iter_;
};

class RouterIterator2SD : public TraitIterator<Elem> {
 public:
  RouterIterator2SD(RALT& ralt, const Compaction& c, CompactionIterator& c_iter,
                    Slice start, Bound end)
      : c_(c),
        promotable_range_{
            .start =
                Bound{
                    .user_key = start,
                    .excluded = false,
                },
            .end = end,
        },
        ucmp_(c.immutable_options()->user_comparator),
        iter_(c_iter),
        hot_iter_(ralt.LowerBound(start)),
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
    if (!promotable_range_.contains(kv.ikey.user_key, ucmp_)) {
      return std::make_optional<Elem>(Decision::kNextLevel, kv);
    }
    Decision decision = route(kv);
    if (decision == Decision::kStartLevel) {
      size_t kvsize = kv.key.size() + kv.value.size();
      if (kv.level == -1 || kv.level == c_.output_level()) {
        if (kv.level == -1) {
          assert(c_.start_level_path_id() == 0);
        }
        kvsize_promoted_ += kvsize;
      } else {
        assert(kv.level == c_.start_level());
        kvsize_retained_ += kvsize;
      }
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
  RangeBounds promotable_range_;

  const Comparator* ucmp_;
  CompactionIterWrapper iter_;
  Peekable<RALT::Iter> hot_iter_;

  size_t kvsize_promoted_;
  size_t kvsize_retained_;
};

class RouterIteratorImpl : public RouterIterator {
 public:
  RouterIteratorImpl(SubcompactionState& sub_compact,
                     CompactionIterator& c_iter)
      : c_iter_(c_iter) {
    const Compaction& c = *sub_compact.compaction;
    RALT& ralt = *c.mutable_cf_options()->ralt.get();
    if (c.latter_level_path_id() == 0) {
      iter_ = std::unique_ptr<RouterIteratorIntraFD>(
          new RouterIteratorIntraFD(c, c_iter));
    } else {
      assert(c.SupportsPerKeyPlacement());
      const Comparator* ucmp = c.immutable_options()->user_comparator;
      // Future work(hotrap): How to handle other cases?
      assert(c.num_input_levels() <= 2);
      const CompactionInputFiles& start_level_inputs = (*c.inputs())[0];
      assert(start_level_inputs.level == c.start_level());
      Slice start_level_smallest_user_key, start_level_largest_user_key;
      start_level_inputs.GetBoundaryKeys(ucmp, &start_level_smallest_user_key,
                                         &start_level_largest_user_key);
      std::optional<Slice> start = sub_compact.start;
      std::optional<Slice> end = sub_compact.end;
      Slice promotable_start =
          !start.has_value()
              ? start_level_smallest_user_key
              : (ucmp->Compare(*start, start_level_smallest_user_key) < 0
                     ? start_level_smallest_user_key
                     : *start);
      Bound promotable_end =
          !end.has_value()
              ? Bound{.user_key = start_level_largest_user_key,
                      .excluded = false}
              : (ucmp->Compare(*end, start_level_largest_user_key) <= 0
                     ? Bound{.user_key = *end, .excluded = true}
                     : Bound{
                           .user_key = start_level_largest_user_key,
                           .excluded = false,
                       });
      iter_ = std::unique_ptr<RouterIterator2SD>(new RouterIterator2SD(
          ralt, c, c_iter, promotable_start, promotable_end));
    }
    cur_ = iter_->next();
  }

  const CompactionIterator& c_iter() const override { return c_iter_; }

  bool Valid() const override { return cur_.has_value(); }
  void Next() override {
    assert(Valid());
    cur_ = iter_->next();
  }
  Decision decision() const override { return cur_.value().decision; }
  Slice key() const override { return cur_.value().kv.key; }
  const ParsedInternalKey& ikey() const override {
    return cur_.value().kv.ikey;
  }
  Slice user_key() const override {
    return cur_.value().kv.ikey.user_key;
  }
  Slice value() const override { return cur_.value().kv.value; }

 private:
  CompactionIterator& c_iter_;
  std::unique_ptr<TraitIterator<Elem>> iter_;
  std::optional<Elem> cur_;
};

std::unique_ptr<RouterIterator> NewRouterIterator(
    SubcompactionState& sub_compact, CompactionIterator& c_iter) {
  const Compaction& c = *sub_compact.compaction;
  RALT* ralt = c.mutable_cf_options()->ralt.get();
  if (ralt == NULL) {
    return std::make_unique<IteratorWithoutRouter>(c_iter);
  } else {
    return std::make_unique<RouterIteratorImpl>(sub_compact, c_iter);
  }
}

}  // namespace ROCKSDB_NAMESPACE
