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

optional<Elem> IteratorWithoutRouter::next() {
	optional<IKeyValueLevel> ret = c_iter_.next();
	if (ret.has_value())
		return make_optional<Elem>(Decision::kNextLevel, ret.value());
	else
		return nullopt;
}

optional<Elem> RouterIteratorIntraTier::next() {
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

RouterIteratorFD2SD::~RouterIteratorFD2SD() {
	auto stats = c_.immutable_options()->stats;
	RecordTick(stats, Tickers::RETAINED_BYTES, kvsize_retained_);
}

Decision RouterIteratorFD2SD::route(const IKeyValueLevel& kv) {
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

optional<Elem> RouterIteratorFD2SD::next() {
	optional<IKeyValueLevel> kv_ret = iter_.next();
	if (!kv_ret.has_value()) {
		return nullopt;
	}
	const IKeyValueLevel& kv = kv_ret.value();
    if (kv.level == c_.output_level())
      return make_optional<Elem>(Decision::kNextLevel, kv);
    assert(kv.level == c_.start_level());
	Decision decision = route(kv);
	if (decision == Decision::kStartLevel) {
      kvsize_retained_ += kv.key.size() + kv.value.size();
	} else {
		assert(decision == Decision::kNextLevel);
	}
	return make_optional<Elem>(decision, kv);
}

RouterIterator::RouterIterator(RALT* ralt, const Compaction& c,
                               CompactionIterator& c_iter,
                               Slice start_level_smallest_user_key)
    : c_iter_(c_iter) {
  int start_level = c.level();
  int latter_level = c.output_level();
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
      iter_ = std::unique_ptr<RouterIteratorFD2SD>(new RouterIteratorFD2SD(
          *ralt, c, c_iter, start_level_smallest_user_key));
    } else {
      iter_ = std::unique_ptr<IteratorWithoutRouter>(
          new IteratorWithoutRouter(c, c_iter));
    }
  }
  cur_ = iter_->next();
}

}  // namespace ROCKSDB_NAMESPACE
