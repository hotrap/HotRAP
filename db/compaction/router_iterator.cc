#include "router_iterator.h"

#include "db/compaction/subcompaction_state.h"

namespace ROCKSDB_NAMESPACE {

// Future work(hotrap): The caller should ZeroOutSequenceIfPossible if the final
// decision is kNextLevel
class CompactionIterWrapper : public TraitIterator<IKeyValue> {
 public:
  CompactionIterWrapper(CompactionIterator& c_iter)
      : c_iter_(c_iter), first_(true) {}
  CompactionIterWrapper(const CompactionIterWrapper& rhs) = delete;
  CompactionIterWrapper& operator=(const CompactionIterWrapper& rhs) = delete;
  CompactionIterWrapper(CompactionIterWrapper&& rhs)
      : c_iter_(rhs.c_iter_), first_(rhs.first_) {}
  CompactionIterWrapper& operator=(const CompactionIterWrapper&& rhs) = delete;
  std::optional<IKeyValue> next() override {
    if (first_) {
      first_ = false;
    } else {
      c_iter_.Next();
    }
    if (c_iter_.Valid())
      return std::make_optional<IKeyValue>(c_iter_.key(), c_iter_.ikey(),
                                           c_iter_.value());
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
    std::optional<IKeyValue> ret = c_iter_.next();
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
        ucmp_(c.column_family_data()->user_comparator()),
        iter_(c_iter),
        hot_iter_(ralt.LowerBound(start)),
        kvsize_to_start_level_(0) {}
  ~RouterIterator2SD() {
    auto stats = c_.immutable_options()->stats;
    if (c_.start_level_path_id() == 0) {
      RecordTick(stats, Tickers::TO_FD_LAST_BYTES, kvsize_to_start_level_);
    } else {
      RecordTick(stats, Tickers::TO_SD_FRONT_BYTES, kvsize_to_start_level_);
    }
  }
  std::optional<Elem> next() override {
    std::optional<IKeyValue> kv_ret = iter_.next();
    if (!kv_ret.has_value()) {
      return std::nullopt;
    }
    const IKeyValue& kv = kv_ret.value();
    if (!promotable_range_.contains(kv.ikey.user_key, ucmp_)) {
      return std::make_optional<Elem>(Decision::kNextLevel, kv);
    }
    Decision decision = route(kv);
    if (decision == Decision::kStartLevel) {
      kvsize_to_start_level_ += kv.key.size() + kv.value.size();
    }
    return std::make_optional<Elem>(decision, kv);
  }

 private:
  Decision route(const IKeyValue& kv) {
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

  uint64_t kvsize_to_start_level_;
};

RouterIterator::RouterIterator(SubcompactionState& sub_compact,
                               CompactionIterator& c_iter)
    : c_iter_(c_iter) {
  const Compaction& c = *sub_compact.compaction;
  RALT* ralt = c.mutable_cf_options()->ralt.get();
  if (ralt == NULL || c.latter_level_path_id() == 0) {
    iter_ = std::unique_ptr<IteratorWithoutRouter>(
        new IteratorWithoutRouter(c, c_iter));
  } else {
    assert(c.SupportsPerKeyPlacement());
    const Comparator* ucmp =
        c.column_family_data()->ioptions()->user_comparator;
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
            ? Bound{.user_key = start_level_largest_user_key, .excluded = false}
            : (ucmp->Compare(*end, start_level_largest_user_key) <= 0
                   ? Bound{.user_key = *end, .excluded = true}
                   : Bound{
                         .user_key = start_level_largest_user_key,
                         .excluded = false,
                     });
    iter_ = std::unique_ptr<RouterIterator2SD>(new RouterIterator2SD(
        *ralt, c, c_iter, promotable_start, promotable_end));
  }
  cur_ = iter_->next();
}

}  // namespace ROCKSDB_NAMESPACE
