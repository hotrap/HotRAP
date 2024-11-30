//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include "rocksdb/comparator.h"

#include <queue>
#include <string>
#include <vector>

#include "memory/arena.h"
#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "table/internal_iterator.h"

namespace ROCKSDB_NAMESPACE {

class DBImpl;
class Env;
struct SuperVersion;
class ColumnFamilyData;
class VersionStorageInfo;
struct FileMetaData;

class MinIterComparator {
 public:
  explicit MinIterComparator(const CompareInterface* comparator)
      : comparator_(comparator) {}

  bool operator()(InternalIterator* a, InternalIterator* b) {
    return comparator_->Compare(a->key(), b->key()) > 0;
  }

 private:
  const CompareInterface* comparator_;
};

using MinIterHeap =
    std::priority_queue<InternalIterator*, std::vector<InternalIterator*>,
                        MinIterComparator>;

// Usage:
//     ForwardLevelIterator iter;
//     iter.SetFileIndex(file_index);
//     iter.Seek(target); // or iter.SeekToFirst();
//     iter.Next()
class ForwardLevelIterator : public InternalIterator {
 public:
  ForwardLevelIterator(
      const ColumnFamilyData* const cfd, const ReadOptions& read_options,
      const std::vector<FileMetaData*>& files,
      const std::shared_ptr<const SliceTransform>& prefix_extractor,
      bool allow_unprepared_value, uint8_t block_protection_bytes_per_key)
      : cfd_(cfd),
        read_options_(read_options),
        files_(files),
        valid_(false),
        file_index_(std::numeric_limits<uint32_t>::max()),
        file_iter_(nullptr),
        pinned_iters_mgr_(nullptr),
        prefix_extractor_(prefix_extractor),
        allow_unprepared_value_(allow_unprepared_value),
        block_protection_bytes_per_key_(block_protection_bytes_per_key) {
    status_.PermitUncheckedError();  // Allow uninitialized status through
  }

  ~ForwardLevelIterator() override;

  void SetFileIndex(uint32_t file_index) {
    assert(file_index < files_.size());
    status_ = Status::OK();
    if (file_index != file_index_) {
      file_index_ = file_index;
      Reset();
    }
  }
  void Reset();
  void SeekToLast() override {
    status_ = Status::NotSupported("ForwardLevelIterator::SeekToLast()");
    valid_ = false;
  }
  void Prev() override {
    status_ = Status::NotSupported("ForwardLevelIterator::Prev()");
    valid_ = false;
  }
  bool Valid() const override { return valid_; }
  void SeekToFirst() override {
    assert(file_iter_ != nullptr);
    if (!status_.ok()) {
      assert(!valid_);
      return;
    }
    file_iter_->SeekToFirst();
    valid_ = file_iter_->Valid();
  }
  void Seek(const Slice& internal_key) override {
    assert(file_iter_ != nullptr);

    // This deviates from the usual convention for InternalIterator::Seek() in
    // that it doesn't discard pre-existing error status. That's because this
    // Seek() is only supposed to be called immediately after SetFileIndex()
    // (which discards pre-existing error status), and SetFileIndex() may set
    // an error status, which we shouldn't discard.
    if (!status_.ok()) {
      assert(!valid_);
      return;
    }

    file_iter_->Seek(internal_key);
    valid_ = file_iter_->Valid();
  }
  void SeekForPrev(const Slice& /*internal_key*/) override {
    status_ = Status::NotSupported("ForwardLevelIterator::SeekForPrev()");
    valid_ = false;
  }
  void Next() override {
    assert(valid_);
    file_iter_->Next();
    for (;;) {
      valid_ = file_iter_->Valid();
      if (!file_iter_->status().ok()) {
        assert(!valid_);
        return;
      }
      if (valid_) {
        return;
      }
      if (file_index_ + 1 >= files_.size()) {
        valid_ = false;
        return;
      }
      SetFileIndex(file_index_ + 1);
      if (!status_.ok()) {
        assert(!valid_);
        return;
      }
      file_iter_->SeekToFirst();
    }
  }
  Slice key() const override {
    assert(valid_);
    return file_iter_->key();
  }
  Slice value() const override {
    assert(valid_);
    return file_iter_->value();
  }
  Status status() const override {
    if (!status_.ok()) {
      return status_;
    } else if (file_iter_) {
      return file_iter_->status();
    }
    return Status::OK();
  }
  bool PrepareValue() override {
    assert(valid_);
    if (file_iter_->PrepareValue()) {
      return true;
    }

    assert(!file_iter_->Valid());
    valid_ = false;
    return false;
  }
  bool IsKeyPinned() const override;
  bool IsValuePinned() const override;
  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
    if (file_iter_) {
      file_iter_->SetPinnedItersMgr(pinned_iters_mgr_);
    }
  }

 private:
  const ColumnFamilyData* const cfd_;
  const ReadOptions& read_options_;
  const std::vector<FileMetaData*>& files_;

  bool valid_;
  uint32_t file_index_;
  Status status_;
  InternalIterator* file_iter_;
  PinnedIteratorsManager* pinned_iters_mgr_;
  // Kept alive by ForwardIterator::sv_->mutable_cf_options
  const std::shared_ptr<const SliceTransform>& prefix_extractor_;
  const bool allow_unprepared_value_;
  const uint8_t block_protection_bytes_per_key_;
};

/**
 * ForwardIterator is a special type of iterator that only supports Seek()
 * and Next(). It is expected to perform better than TailingIterator by
 * removing the encapsulation and making all information accessible within
 * the iterator. At the current implementation, snapshot is taken at the
 * time Seek() is called. The Next() followed do not see new values after.
 */
class ForwardIterator : public InternalIterator {
 public:
  ForwardIterator(DBImpl* db, const ReadOptions& read_options,
                  ColumnFamilyData* cfd, SuperVersion* current_sv = nullptr,
                  bool allow_unprepared_value = false);
  virtual ~ForwardIterator();

  void SeekForPrev(const Slice& /*target*/) override {
    status_ = Status::NotSupported("ForwardIterator::SeekForPrev()");
    valid_ = false;
  }
  void SeekToLast() override {
    status_ = Status::NotSupported("ForwardIterator::SeekToLast()");
    valid_ = false;
  }
  void Prev() override {
    status_ = Status::NotSupported("ForwardIterator::Prev");
    valid_ = false;
  }

  virtual bool Valid() const override;
  void SeekToFirst() override;
  virtual void Seek(const Slice& target) override;
  virtual void Next() override;
  virtual Slice key() const override;
  virtual Slice value() const override;
  virtual Status status() const override;
  virtual bool PrepareValue() override;
  virtual Status GetProperty(std::string prop_name, std::string* prop) override;
  virtual void SetPinnedItersMgr(
      PinnedIteratorsManager* pinned_iters_mgr) override;
  virtual bool IsKeyPinned() const override;
  virtual bool IsValuePinned() const override;

  bool TEST_CheckDeletedIters(int* deleted_iters, int* num_iters);

 private:
  void Cleanup(bool release_sv);
  // Unreference and, if needed, clean up the current SuperVersion. This is
  // either done immediately or deferred until this iterator is unpinned by
  // PinnedIteratorsManager.
  void SVCleanup();
  static void SVCleanup(DBImpl* db, SuperVersion* sv,
                        bool background_purge_on_iterator_cleanup);
  static void DeferredSVCleanup(void* arg);

  void RebuildIterators(bool refresh_sv);
  void RenewIterators();
  void BuildLevelIterators(const VersionStorageInfo* vstorage,
                           SuperVersion* sv);
  void ResetIncompleteIterators();
  void SeekInternal(const Slice& internal_key, bool seek_to_first,
                    bool seek_after_async_io);

  void UpdateCurrent();
  bool NeedToSeekImmutable(const Slice& internal_key);
  void DeleteCurrentIter();
  uint32_t FindFileInRange(const std::vector<FileMetaData*>& files,
                           const Slice& internal_key, uint32_t left,
                           uint32_t right);

  bool IsOverUpperBound(const Slice& internal_key) const;

  // Set PinnedIteratorsManager for all children Iterators, this function should
  // be called whenever we update children Iterators or pinned_iters_mgr_.
  void UpdateChildrenPinnedItersMgr();

  // A helper function that will release iter in the proper manner, or pass it
  // to pinned_iters_mgr_ to release it later if pinning is enabled.
  void DeleteIterator(InternalIterator* iter, bool is_arena = false);

  DBImpl* const db_;
  ReadOptions read_options_;
  ColumnFamilyData* const cfd_;
  const SliceTransform* const prefix_extractor_;
  const Comparator* user_comparator_;
  const bool allow_unprepared_value_;
  MinIterHeap immutable_min_heap_;

  SuperVersion* sv_;
  InternalIterator* mutable_iter_;
  std::vector<InternalIterator*> imm_iters_;
  std::vector<InternalIterator*> l0_iters_;
  std::vector<ForwardLevelIterator*> level_iters_;
  InternalIterator* current_;
  bool valid_;

  // Internal iterator status; set only by one of the unsupported methods.
  Status status_;
  // Status of immutable iterators, maintained here to avoid iterating over
  // all of them in status().
  Status immutable_status_;
  // Indicates that at least one of the immutable iterators pointed to a key
  // larger than iterate_upper_bound and was therefore destroyed. Seek() may
  // need to rebuild such iterators.
  bool has_iter_trimmed_for_upper_bound_;
  // Is current key larger than iterate_upper_bound? If so, makes Valid()
  // return false.
  bool current_over_upper_bound_;

  // Left endpoint of the range of keys that immutable iterators currently
  // cover. When Seek() is called with a key that's within that range, immutable
  // iterators don't need to be moved; see NeedToSeekImmutable(). This key is
  // included in the range after a Seek(), but excluded when advancing the
  // iterator using Next().
  IterKey prev_key_;
  bool is_prev_set_;
  bool is_prev_inclusive_;

  PinnedIteratorsManager* pinned_iters_mgr_;
  Arena arena_;
};

}  // namespace ROCKSDB_NAMESPACE
