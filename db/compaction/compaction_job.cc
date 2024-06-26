//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction/compaction_job.h"

#include <algorithm>
#include <cinttypes>
#include <functional>
#include <list>
#include <memory>
#include <optional>
#include <set>
#include <thread>
#include <utility>
#include <vector>

#include "db/blob/blob_counting_iterator.h"
#include "db/blob/blob_file_addition.h"
#include "db/blob/blob_file_builder.h"
#include "db/builder.h"
#include "db/compaction/clipping_iterator.h"
#include "db/db_impl/db_impl.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/error_handler.h"
#include "db/event_helpers.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/memtable_list.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "db/range_del_aggregator.h"
#include "db/version_set.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/sst_file_manager_impl.h"
#include "file/writable_file_writer.h"
#include "logging/log_buffer.h"
#include "logging/logging.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics.h"
#include "monitoring/thread_status_util.h"
#include "options/configurable_helper.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "rocksdb/compaction_router.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "rocksdb/sst_partitioner.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/options_type.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/merging_iterator.h"
#include "table/table_builder.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/hash.h"
#include "util/mutexlock.h"
#include "util/rusty.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "util/timers.h"

namespace ROCKSDB_NAMESPACE {

const char* GetCompactionReasonString(CompactionReason compaction_reason) {
  switch (compaction_reason) {
    case CompactionReason::kUnknown:
      return "Unknown";
    case CompactionReason::kLevelL0FilesNum:
      return "LevelL0FilesNum";
    case CompactionReason::kLevelMaxLevelSize:
      return "LevelMaxLevelSize";
    case CompactionReason::kUniversalSizeAmplification:
      return "UniversalSizeAmplification";
    case CompactionReason::kUniversalSizeRatio:
      return "UniversalSizeRatio";
    case CompactionReason::kUniversalSortedRunNum:
      return "UniversalSortedRunNum";
    case CompactionReason::kFIFOMaxSize:
      return "FIFOMaxSize";
    case CompactionReason::kFIFOReduceNumFiles:
      return "FIFOReduceNumFiles";
    case CompactionReason::kFIFOTtl:
      return "FIFOTtl";
    case CompactionReason::kManualCompaction:
      return "ManualCompaction";
    case CompactionReason::kFilesMarkedForCompaction:
      return "FilesMarkedForCompaction";
    case CompactionReason::kBottommostFiles:
      return "BottommostFiles";
    case CompactionReason::kTtl:
      return "Ttl";
    case CompactionReason::kFlush:
      return "Flush";
    case CompactionReason::kExternalSstIngestion:
      return "ExternalSstIngestion";
    case CompactionReason::kPeriodicCompaction:
      return "PeriodicCompaction";
    case CompactionReason::kChangeTemperature:
      return "ChangeTemperature";
    case CompactionReason::kForcedBlobGC:
      return "ForcedBlobGC";
    case CompactionReason::kNumOfReasons:
      // fall through
    default:
      assert(false);
      return "Invalid";
  }
}

const CompactionJob::SubcompactionState::Output*
CompactionJob::SubcompactionState::LevelOutput::current_output_const() const {
  if (outputs.empty()) {
    // This subcompaction's output could be empty if compaction was aborted
    // before this subcompaction had a chance to generate any output files.
    // When subcompactions are executed sequentially this is more likely and
    // will be particularly likely for the later subcompactions to be empty.
    // Once they are run in parallel however it should be much rarer.
    return nullptr;
  } else {
    return &outputs.back();
  }
}

CompactionJob::SubcompactionState::Output*
CompactionJob::SubcompactionState::LevelOutput::current_output() {
  if (outputs.empty()) {
    // This subcompaction's output could be empty if compaction was aborted
    // before this subcompaction had a chance to generate any output files.
    // When subcompactions are executed sequentially this is more likely and
    // will be particularly likely for the later subcompactions to be empty.
    // Once they are run in parallel however it should be much rarer.
    return nullptr;
  } else {
    return &outputs.back();
  }
}

// Adds the key and value to the builder
// If paranoid is true, adds the key-value to the paranoid hash
Status CompactionJob::SubcompactionState::LevelOutput::AddToBuilder(
    const Slice& key, const Slice& value) {
  auto curr = current_output();
  assert(builder != nullptr);
  assert(curr != nullptr);
  Status s = curr->validator.Add(key, value);
  if (!s.ok()) {
    return s;
  }
  builder->Add(key, value);
  return Status::OK();
}

Slice CompactionJob::SubcompactionState::LevelOutput::SmallestUserKey() const {
  if (!outputs.empty() && outputs[0].finished) {
    return outputs[0].meta.smallest.user_key();
  }
  // If there is no finished output, return an empty slice.
  return Slice(nullptr, 0);
}

Slice CompactionJob::SubcompactionState::LevelOutput::LargestUserKey() const {
  if (!outputs.empty() && current_output_const()->finished) {
    return current_output_const()->meta.largest.user_key();
  }
  // If there is no finished output, return an empty slice.
  return Slice(nullptr, 0);
}

void CompactionJob::SubcompactionState::LevelOutput::CleanupCompaction(
    const Status& sub_status, std::shared_ptr<rocksdb::Cache> table_cache) {
  if (builder != nullptr) {
    // May happen if we get a shutdown call in the middle of compaction
    builder->Abandon();
    builder.reset();
  } else {
    assert(!sub_status.ok() || outfile == nullptr);
  }
  for (const auto& out : outputs) {
    // If this file was inserted into the table cache then remove
    // them here because this compaction was not committed.
    if (!sub_status.ok()) {
      TableCache::Evict(table_cache.get(), out.meta.fd.GetNumber());
    }
  }
}

std::string CompactionJob::SubcompactionState::LevelOutput::GetTableFileName(
    const ImmutableOptions* ioptions, uint64_t file_number) {
  return TableFileName(ioptions->cf_paths, file_number, path_id());
}

CompactionJob::SubcompactionState::SubcompactionState(Compaction* c,
                                                      Slice* _start,
                                                      Slice* _end,
                                                      uint64_t size,
                                                      uint32_t _sub_job_id)
    : compaction(c),
      start(_start),
      end(_end),
      start_level_output(c->start_level_path_id(), c->start_level(), 1),
      latter_level_output(c->latter_level_path_id(), c->output_level(),
                          c->latter_level_hot_per_byte()),
      approx_size(size),
      sub_job_id(_sub_job_id) {
  assert(compaction != nullptr);
}

bool CompactionJob::SubcompactionState::ShouldStopBefore(
    const Slice& internal_key, uint64_t curr_file_size) {
  const InternalKeyComparator* icmp =
      &compaction->column_family_data()->internal_comparator();
  const std::vector<FileMetaData*>& grandparents = compaction->grandparents();

  bool grandparant_file_switched = false;
  // Scan to find earliest grandparent file that contains key.
  while (grandparent_index < grandparents.size() &&
         icmp->Compare(internal_key,
                       grandparents[grandparent_index]->largest.Encode()) > 0) {
    if (seen_key) {
      overlapped_bytes += grandparents[grandparent_index]->fd.GetFileSize();
      grandparant_file_switched = true;
    }
    assert(grandparent_index + 1 >= grandparents.size() ||
           icmp->Compare(
               grandparents[grandparent_index]->largest.Encode(),
               grandparents[grandparent_index + 1]->smallest.Encode()) <= 0);
    grandparent_index++;
  }
  seen_key = true;

  if (grandparant_file_switched &&
      overlapped_bytes + curr_file_size > compaction->max_compaction_bytes()) {
    // Too much overlap for current output; start new output
    overlapped_bytes = 0;
    return true;
  }

  if (!files_to_cut_for_ttl.empty()) {
    if (cur_files_to_cut_for_ttl != -1) {
      // Previous key is inside the range of a file
      if (icmp->Compare(internal_key,
                        files_to_cut_for_ttl[cur_files_to_cut_for_ttl]
                            ->largest.Encode()) > 0) {
        next_files_to_cut_for_ttl = cur_files_to_cut_for_ttl + 1;
        cur_files_to_cut_for_ttl = -1;
        return true;
      }
    } else {
      // Look for the key position
      while (next_files_to_cut_for_ttl <
             static_cast<int>(files_to_cut_for_ttl.size())) {
        if (icmp->Compare(internal_key,
                          files_to_cut_for_ttl[next_files_to_cut_for_ttl]
                              ->smallest.Encode()) >= 0) {
          if (icmp->Compare(internal_key,
                            files_to_cut_for_ttl[next_files_to_cut_for_ttl]
                                ->largest.Encode()) <= 0) {
            // With in the current file
            cur_files_to_cut_for_ttl = next_files_to_cut_for_ttl;
            return true;
          }
          // Beyond the current file
          next_files_to_cut_for_ttl++;
        } else {
          // Still fall into the gap
          break;
        }
      }
    }
  }

  return false;
}

Status CompactionJob::SubcompactionState::ProcessOutFlowIfNeeded(
    const Slice& key, const Slice& value) {
  if (!blob_garbage_meter) {
    return Status::OK();
  }

  return blob_garbage_meter->ProcessOutFlow(key, value);
}

void CompactionJob::SubcompactionState::FillFilesToCutForTtl() {
  if (compaction->immutable_options()->compaction_style !=
          CompactionStyle::kCompactionStyleLevel ||
      compaction->immutable_options()->compaction_pri !=
          CompactionPri::kMinOverlappingRatio ||
      compaction->mutable_cf_options()->ttl == 0 ||
      compaction->num_input_levels() < 2 || compaction->bottommost_level()) {
    return;
  }

  // We define new file with oldest ancestor time to be younger than 1/4 TTL,
  // and an old one to be older than 1/2 TTL time.
  int64_t temp_current_time;
  auto get_time_status = compaction->immutable_options()->clock->GetCurrentTime(
      &temp_current_time);
  if (!get_time_status.ok()) {
    return;
  }
  uint64_t current_time = static_cast<uint64_t>(temp_current_time);
  if (current_time < compaction->mutable_cf_options()->ttl) {
    return;
  }
  uint64_t old_age_thres =
      current_time - compaction->mutable_cf_options()->ttl / 2;

  const std::vector<FileMetaData*>& olevel =
      *(compaction->inputs(compaction->num_input_levels() - 1));
  for (FileMetaData* file : olevel) {
    // Worth filtering out by start and end?
    uint64_t oldest_ancester_time = file->TryGetOldestAncesterTime();
    // We put old files if they are not too small to prevent a flood
    // of small files.
    if (oldest_ancester_time < old_age_thres &&
        file->fd.GetFileSize() >
            compaction->mutable_cf_options()->target_file_size_base / 2) {
      files_to_cut_for_ttl.push_back(file);
    }
  }
}

// Maintains state for the entire compaction
struct CompactionJob::CompactionState {
  Compaction* const compaction;

  // REQUIRED: subcompaction states are stored in order of increasing
  // key-range
  std::vector<CompactionJob::SubcompactionState> sub_compact_states;
  Status status;

  size_t num_output_files = 0;
  uint64_t total_bytes = 0;
  uint64_t retained_or_promoted_bytes = 0;
  size_t num_blob_output_files = 0;
  uint64_t total_blob_bytes = 0;
  uint64_t num_output_records = 0;

  explicit CompactionState(Compaction* c) : compaction(c) {}

  Slice SmallestUserKey() {
    Slice start_level_ret, latter_level_ret;
    for (const auto& sub_compact_state : sub_compact_states) {
      start_level_ret = sub_compact_state.start_level_output.SmallestUserKey();
      if (!start_level_ret.empty()) {
        break;
      }
    }
    for (const auto& sub_compact_state : sub_compact_states) {
      latter_level_ret =
          sub_compact_state.latter_level_output.SmallestUserKey();
      if (!latter_level_ret.empty()) {
        break;
      }
    }
    if (start_level_ret.empty()) {
      return latter_level_ret;
    } else if (latter_level_ret.empty()) {
      return start_level_ret;
    } else {
      auto* c = compaction;
      auto* cfd = c->column_family_data();
      const Comparator* cfd_comparator = cfd->user_comparator();
      int res = cfd_comparator->Compare(start_level_ret, latter_level_ret);
      if (res < 0) {
        return start_level_ret;
      } else {
        return latter_level_ret;
      }
    }
  }

  Slice LargestUserKey() {
    Slice start_level_ret, latter_level_ret;
    for (auto it = sub_compact_states.rbegin(); it < sub_compact_states.rend();
         ++it) {
      start_level_ret = it->start_level_output.LargestUserKey();
      if (!start_level_ret.empty()) {
        break;
      }
    }
    for (auto it = sub_compact_states.rbegin(); it < sub_compact_states.rend();
         ++it) {
      latter_level_ret = it->latter_level_output.LargestUserKey();
      if (!latter_level_ret.empty()) {
        break;
      }
    }
    if (start_level_ret.empty()) {
      return latter_level_ret;
    } else if (latter_level_ret.empty()) {
      return start_level_ret;
    } else {
      auto* c = compaction;
      auto* cfd = c->column_family_data();
      const Comparator* cfd_comparator = cfd->user_comparator();
      int res = cfd_comparator->Compare(start_level_ret, latter_level_ret);
      if (res > 0) {
        return start_level_ret;
      } else {
        return latter_level_ret;
      }
    }
  }
};

void CompactionJob::AggregateStatistics() {
  assert(compact_);

  for (SubcompactionState& sc : compact_->sub_compact_states) {
    {
      auto& outputs = sc.start_level_output.outputs;
      if (!outputs.empty() && !outputs.back().meta.fd.file_size) {
        // An error occurred, so ignore the last output.
        outputs.pop_back();
      }
      compact_->num_output_files += outputs.size();
    }

    {
      auto& outputs = sc.latter_level_output.outputs;
      if (!outputs.empty() && !outputs.back().meta.fd.file_size) {
        // An error occurred, so ignore the last output.
        outputs.pop_back();
      }
      compact_->num_output_files += outputs.size();
    }

    compact_->total_bytes += sc.total_bytes;
    compact_->retained_or_promoted_bytes += sc.retained_or_promoted_bytes;

    const auto& blobs = sc.blob_file_additions;

    compact_->num_blob_output_files += blobs.size();

    for (const auto& blob : blobs) {
      compact_->total_blob_bytes += blob.GetTotalBlobBytes();
    }

    compact_->num_output_records += sc.num_output_records;

    compaction_job_stats_->Add(sc.compaction_job_stats);
  }
}

CompactionJob::CompactionJob(
    int job_id, Compaction* compaction, const ImmutableDBOptions& db_options,
    const MutableDBOptions& mutable_db_options, const FileOptions& file_options,
    VersionSet* versions, const std::atomic<bool>* shutting_down,
    const SequenceNumber preserve_deletes_seqnum, LogBuffer* log_buffer,
    FSDirectory* db_directory, FSDirectory* start_level_output_directory,
    FSDirectory* latter_level_output_directory,
    FSDirectory* blob_output_directory, Statistics* stats,
    InstrumentedMutex* db_mutex, ErrorHandler* db_error_handler,
    std::vector<SequenceNumber> existing_snapshots,
    SequenceNumber earliest_write_conflict_snapshot,
    const SnapshotChecker* snapshot_checker, std::shared_ptr<Cache> table_cache,
    EventLogger* event_logger, bool paranoid_file_checks, bool measure_io_stats,
    const std::string& dbname, CompactionJobStats* compaction_job_stats,
    Env::Priority thread_pri, const std::shared_ptr<IOTracer>& io_tracer,
    const std::atomic<int>* manual_compaction_paused,
    const std::atomic<bool>* manual_compaction_canceled,
    const std::string& db_id, const std::string& db_session_id,
    std::string full_history_ts_low, BlobFileCompletionCallback* blob_callback)
    : compact_(new CompactionState(compaction)),
      compaction_stats_(compaction->compaction_reason(), 1),
      db_options_(db_options),
      mutable_db_options_copy_(mutable_db_options),
      log_buffer_(log_buffer),
      start_level_output_directory_(start_level_output_directory),
      latter_level_output_directory_(latter_level_output_directory),
      stats_(stats),
      bottommost_level_(false),
      write_hint_(Env::WLTH_NOT_SET),
      job_id_(job_id),
      compaction_job_stats_(compaction_job_stats),
      dbname_(dbname),
      db_id_(db_id),
      db_session_id_(db_session_id),
      file_options_(file_options),
      env_(db_options.env),
      io_tracer_(io_tracer),
      fs_(db_options.fs, io_tracer),
      file_options_for_read_(
          fs_->OptimizeForCompactionTableRead(file_options, db_options_)),
      versions_(versions),
      shutting_down_(shutting_down),
      manual_compaction_paused_(manual_compaction_paused),
      manual_compaction_canceled_(manual_compaction_canceled),
      preserve_deletes_seqnum_(preserve_deletes_seqnum),
      db_directory_(db_directory),
      blob_output_directory_(blob_output_directory),
      db_mutex_(db_mutex),
      db_error_handler_(db_error_handler),
      existing_snapshots_(std::move(existing_snapshots)),
      earliest_write_conflict_snapshot_(earliest_write_conflict_snapshot),
      snapshot_checker_(snapshot_checker),
      table_cache_(std::move(table_cache)),
      event_logger_(event_logger),
      paranoid_file_checks_(paranoid_file_checks),
      measure_io_stats_(measure_io_stats),
      thread_pri_(thread_pri),
      full_history_ts_low_(std::move(full_history_ts_low)),
      blob_callback_(blob_callback) {
  assert(compaction_job_stats_ != nullptr);
  assert(log_buffer_ != nullptr);
  const auto* cfd = compact_->compaction->column_family_data();
  ThreadStatusUtil::SetColumnFamily(cfd, cfd->ioptions()->env,
                                    db_options_.enable_thread_tracking);
  ThreadStatusUtil::SetThreadOperation(ThreadStatus::OP_COMPACTION);
  ReportStartedCompaction(compaction);
}

CompactionJob::~CompactionJob() {
  assert(compact_ == nullptr);
  ThreadStatusUtil::ResetThreadStatus();
}

void CompactionJob::ReportStartedCompaction(Compaction* compaction) {
  const auto* cfd = compact_->compaction->column_family_data();
  ThreadStatusUtil::SetColumnFamily(cfd, cfd->ioptions()->env,
                                    db_options_.enable_thread_tracking);

  ThreadStatusUtil::SetThreadOperationProperty(ThreadStatus::COMPACTION_JOB_ID,
                                               job_id_);

  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_INPUT_OUTPUT_LEVEL,
      (static_cast<uint64_t>(compact_->compaction->start_level()) << 32) +
          compact_->compaction->output_level());

  // In the current design, a CompactionJob is always created
  // for non-trivial compaction.
  assert(compaction->IsTrivialMove() == false ||
         compaction->is_manual_compaction() == true);

  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_PROP_FLAGS,
      compaction->is_manual_compaction() +
          (compaction->deletion_compaction() << 1));

  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_TOTAL_INPUT_BYTES,
      compaction->CalculateTotalInputSize());

  IOSTATS_RESET(bytes_written);
  IOSTATS_RESET(bytes_read);
  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_BYTES_WRITTEN, 0);
  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_BYTES_READ, 0);

  // Set the thread operation after operation properties
  // to ensure GetThreadList() can always show them all together.
  ThreadStatusUtil::SetThreadOperation(ThreadStatus::OP_COMPACTION);

  compaction_job_stats_->is_manual_compaction =
      compaction->is_manual_compaction();
  compaction_job_stats_->is_full_compaction = compaction->is_full_compaction();
}

void CompactionJob::Prepare() {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_PREPARE);

  // Generate file_levels_ for compaction before making Iterator
  auto* c = compact_->compaction;
  assert(c->column_family_data() != nullptr);
  assert(c->column_family_data()->current()->storage_info()->NumLevelFiles(
             compact_->compaction->level()) > 0);

  write_hint_ =
      c->column_family_data()->CalculateSSTWriteHint(c->output_level());
  bottommost_level_ = c->bottommost_level();

  if (c->ShouldFormSubcompactions()) {
    {
      StopWatch sw(db_options_.clock, stats_, SUBCOMPACTION_SETUP_TIME);
      GenSubcompactionBoundaries();
    }
    assert(sizes_.size() == boundaries_.size() + 1);

    for (size_t i = 0; i <= boundaries_.size(); i++) {
      Slice* start = i == 0 ? nullptr : &boundaries_[i - 1];
      Slice* end = i == boundaries_.size() ? nullptr : &boundaries_[i];
      compact_->sub_compact_states.emplace_back(c, start, end, sizes_[i],
                                                static_cast<uint32_t>(i));
    }
    RecordInHistogram(stats_, NUM_SUBCOMPACTIONS_SCHEDULED,
                      compact_->sub_compact_states.size());
  } else {
    constexpr Slice* start = nullptr;
    constexpr Slice* end = nullptr;
    constexpr uint64_t size = 0;

    compact_->sub_compact_states.emplace_back(c, start, end, size,
                                              /*sub_job_id*/ 0);
  }
}

struct RangeWithSize {
  Range range;
  uint64_t size;

  RangeWithSize(const Slice& a, const Slice& b, uint64_t s = 0)
      : range(a, b), size(s) {}
};

void CompactionJob::GenSubcompactionBoundaries() {
  auto* c = compact_->compaction;
  auto* cfd = c->column_family_data();
  const Comparator* cfd_comparator = cfd->user_comparator();
  std::vector<Slice> bounds;
  int start_lvl = c->start_level();
  int out_lvl = c->output_level();

  // Add the starting and/or ending key of certain input files as a potential
  // boundary
  for (size_t lvl_idx = 0; lvl_idx < c->num_input_levels(); lvl_idx++) {
    int lvl = c->level(lvl_idx);
    if (lvl >= start_lvl && lvl <= out_lvl) {
      const LevelFilesBrief* flevel = c->input_levels(lvl_idx);
      size_t num_files = flevel->num_files;

      if (num_files == 0) {
        continue;
      }

      if (lvl == 0) {
        // For level 0 add the starting and ending key of each file since the
        // files may have greatly differing key ranges (not range-partitioned)
        for (size_t i = 0; i < num_files; i++) {
          bounds.emplace_back(flevel->files[i].smallest_key);
          bounds.emplace_back(flevel->files[i].largest_key);
        }
      } else {
        // For all other levels add the smallest/largest key in the level to
        // encompass the range covered by that level
        bounds.emplace_back(flevel->files[0].smallest_key);
        bounds.emplace_back(flevel->files[num_files - 1].largest_key);
        if (lvl == out_lvl) {
          // For the last level include the starting keys of all files since
          // the last level is the largest and probably has the widest key
          // range. Since it's range partitioned, the ending key of one file
          // and the starting key of the next are very close (or identical).
          for (size_t i = 1; i < num_files; i++) {
            bounds.emplace_back(flevel->files[i].smallest_key);
          }
        }
      }
    }
  }

  std::sort(bounds.begin(), bounds.end(),
            [cfd_comparator](const Slice& a, const Slice& b) -> bool {
              return cfd_comparator->Compare(ExtractUserKey(a),
                                             ExtractUserKey(b)) < 0;
            });
  // Remove duplicated entries from bounds
  bounds.erase(
      std::unique(bounds.begin(), bounds.end(),
                  [cfd_comparator](const Slice& a, const Slice& b) -> bool {
                    return cfd_comparator->Compare(ExtractUserKey(a),
                                                   ExtractUserKey(b)) == 0;
                  }),
      bounds.end());

  // Combine consecutive pairs of boundaries into ranges with an approximate
  // size of data covered by keys in that range
  uint64_t sum = 0;
  std::vector<RangeWithSize> ranges;
  // Get input version from CompactionState since it's already referenced
  // earlier in SetInputVersioCompaction::SetInputVersion and will not change
  // when db_mutex_ is released below
  auto* v = compact_->compaction->input_version();
  for (auto it = bounds.begin();;) {
    const Slice a = *it;
    ++it;

    if (it == bounds.end()) {
      break;
    }

    const Slice b = *it;

    // ApproximateSize could potentially create table reader iterator to seek
    // to the index block and may incur I/O cost in the process. Unlock db
    // mutex to reduce contention
    db_mutex_->Unlock();
    uint64_t size = versions_->ApproximateSize(SizeApproximationOptions(), v, a,
                                               b, start_lvl, out_lvl + 1,
                                               TableReaderCaller::kCompaction);
    db_mutex_->Lock();
    ranges.emplace_back(a, b, size);
    sum += size;
  }

  // Group the ranges into subcompactions
  const double min_file_fill_percent = 4.0 / 5;
  int base_level = v->storage_info()->base_level();
  uint64_t max_output_files = static_cast<uint64_t>(std::ceil(
      sum / min_file_fill_percent /
      MaxFileSizeForLevel(
          *(c->mutable_cf_options()), out_lvl,
          c->immutable_options()->compaction_style, base_level,
          c->immutable_options()->level_compaction_dynamic_level_bytes)));
  uint64_t subcompactions = std::min(
      {static_cast<uint64_t>(ranges.size()),
       static_cast<uint64_t>(c->max_subcompactions()), max_output_files});

  if (subcompactions > 1) {
    double mean = sum * 1.0 / subcompactions;
    // Greedily add ranges to the subcompaction until the sum of the ranges'
    // sizes becomes >= the expected mean size of a subcompaction
    sum = 0;
    for (size_t i = 0; i + 1 < ranges.size(); i++) {
      sum += ranges[i].size;
      if (subcompactions == 1) {
        // If there's only one left to schedule then it goes to the end so no
        // need to put an end boundary
        continue;
      }
      if (sum >= mean) {
        boundaries_.emplace_back(ExtractUserKey(ranges[i].range.limit));
        sizes_.emplace_back(sum);
        subcompactions--;
        sum = 0;
      }
    }
    sizes_.emplace_back(sum + ranges.back().size);
  } else {
    // Only one range so its size is the total sum of sizes computed above
    sizes_.emplace_back(sum);
  }
}

Status CompactionJob::Run() {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_RUN);
  TEST_SYNC_POINT("CompactionJob::Run():Start");
  log_buffer_->FlushBufferToLog();
  LogCompaction();

  const size_t num_threads = compact_->sub_compact_states.size();
  assert(num_threads > 0);
  const uint64_t start_micros = db_options_.clock->NowMicros();

  // Launch a thread for each of subcompactions 1...num_threads-1
  std::vector<port::Thread> thread_pool;
  thread_pool.reserve(num_threads - 1);
  for (size_t i = 1; i < compact_->sub_compact_states.size(); i++) {
    thread_pool.emplace_back(&CompactionJob::ProcessKeyValueCompaction, this,
                             &compact_->sub_compact_states[i]);
  }

  // Always schedule the first subcompaction (whether or not there are also
  // others) in the current thread to be efficient with resources
  ProcessKeyValueCompaction(&compact_->sub_compact_states[0]);

  // Wait for all other threads (if there are any) to finish execution
  for (auto& thread : thread_pool) {
    thread.join();
  }

  compaction_stats_.micros = db_options_.clock->NowMicros() - start_micros;
  compaction_stats_.cpu_micros = 0;
  for (size_t i = 0; i < compact_->sub_compact_states.size(); i++) {
    compaction_stats_.cpu_micros +=
        compact_->sub_compact_states[i].compaction_job_stats.cpu_micros;
  }

  RecordTimeToHistogram(stats_, COMPACTION_TIME, compaction_stats_.micros);
  RecordTimeToHistogram(stats_, COMPACTION_CPU_TIME,
                        compaction_stats_.cpu_micros);

  TEST_SYNC_POINT("CompactionJob::Run:BeforeVerify");

  // Check if any thread encountered an error during execution
  Status status;
  IOStatus io_s;
  bool wrote_new_blob_files = false;

  for (const auto& state : compact_->sub_compact_states) {
    if (!state.status.ok()) {
      status = state.status;
      io_s = state.io_status;
      break;
    }

    if (!state.blob_file_additions.empty()) {
      wrote_new_blob_files = true;
    }
  }

  if (io_status_.ok()) {
    io_status_ = io_s;
  }
  if (status.ok()) {
    constexpr IODebugContext* dbg = nullptr;

    if (start_level_output_directory_) {
      io_s = start_level_output_directory_->FsyncWithDirOptions(
          IOOptions(), dbg,
          DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
    }
    if (latter_level_output_directory_) {
      io_s = latter_level_output_directory_->FsyncWithDirOptions(
          IOOptions(), dbg,
          DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
    }

    if (io_s.ok() && wrote_new_blob_files && blob_output_directory_ &&
        blob_output_directory_ != latter_level_output_directory_) {
      io_s = blob_output_directory_->FsyncWithDirOptions(
          IOOptions(), dbg,
          DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
    }
  }
  if (io_status_.ok()) {
    io_status_ = io_s;
  }
  if (status.ok()) {
    status = io_s;
  }
  if (status.ok()) {
    thread_pool.clear();
    Compaction* c = compact_->compaction;
    std::vector<
        std::pair<int, const CompactionJob::SubcompactionState::Output*>>
        files_output;
    for (const auto& state : compact_->sub_compact_states) {
      for (const auto& output : state.start_level_output.outputs) {
        files_output.emplace_back(c->start_level(), &output);
      }
      for (const auto& output : state.latter_level_output.outputs) {
        files_output.emplace_back(c->output_level(), &output);
      }
    }
    ColumnFamilyData* cfd = c->column_family_data();
    auto prefix_extractor = c->mutable_cf_options()->prefix_extractor.get();
    std::atomic<size_t> next_file_idx(0);
    auto verify_table = [&](Status& output_status) {
      while (true) {
        size_t file_idx = next_file_idx.fetch_add(1);
        if (file_idx >= files_output.size()) {
          break;
        }
        int level = files_output[file_idx].first;
        auto output = files_output[file_idx].second;
        // Verify that the table is usable
        // We set for_compaction to false and don't
        // OptimizeForCompactionTableRead here because this is a special case
        // after we finish the table building No matter whether
        // use_direct_io_for_flush_and_compaction is true, we will regard this
        // verification as user reads since the goal is to cache it here for
        // further user reads
        ReadOptions read_options;
        InternalIterator* iter = cfd->table_cache()->NewIterator(
            read_options, file_options_, cfd->internal_comparator(),
            output->meta, /*range_del_agg=*/nullptr, prefix_extractor,
            /*table_reader_ptr=*/nullptr,
            cfd->internal_stats()->GetFileReadHist(level),
            TableReaderCaller::kCompactionRefill, /*arena=*/nullptr,
            /*skip_filters=*/false, level,
            MaxFileSizeForL0MetaPin(
                *compact_->compaction->mutable_cf_options()),
            /*smallest_compaction_key=*/nullptr,
            /*largest_compaction_key=*/nullptr,
            /*allow_unprepared_value=*/false);
        auto s = iter->status();

        if (s.ok() && paranoid_file_checks_) {
          OutputValidator validator(cfd->internal_comparator(),
                                    /*_enable_order_check=*/true,
                                    /*_enable_hash=*/true);
          for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            s = validator.Add(iter->key(), iter->value());
            if (!s.ok()) {
              break;
            }
          }
          if (s.ok()) {
            s = iter->status();
          }
          if (s.ok() && !validator.CompareValidator(output->validator)) {
            s = Status::Corruption("Paranoid checksums do not match");
          }
        }

        delete iter;

        if (!s.ok()) {
          output_status = s;
          break;
        }
      }
    };
    for (size_t i = 1; i < compact_->sub_compact_states.size(); i++) {
      thread_pool.emplace_back(
          verify_table, std::ref(compact_->sub_compact_states[i].status));
    }
    verify_table(compact_->sub_compact_states[0].status);
    for (auto& thread : thread_pool) {
      thread.join();
    }
    for (const auto& state : compact_->sub_compact_states) {
      if (!state.status.ok()) {
        status = state.status;
        break;
      }
    }
  }

  TablePropertiesCollection tp;
  for (const auto& state : compact_->sub_compact_states) {
    for (const auto& output : state.start_level_output.outputs) {
      auto fn =
          TableFileName(state.compaction->immutable_options()->cf_paths,
                        output.meta.fd.GetNumber(), output.meta.fd.GetPathId());
      tp[fn] = output.table_properties;
    }
    for (const auto& output : state.latter_level_output.outputs) {
      auto fn =
          TableFileName(state.compaction->immutable_options()->cf_paths,
                        output.meta.fd.GetNumber(), output.meta.fd.GetPathId());
      tp[fn] = output.table_properties;
    }
  }
  compact_->compaction->SetOutputTableProperties(std::move(tp));

  // Finish up all book-keeping to unify the subcompaction results
  AggregateStatistics();
  UpdateCompactionStats();

  RecordCompactionIOStats();
  LogFlush(db_options_.info_log);
  TEST_SYNC_POINT("CompactionJob::Run():End");

  compact_->status = status;
  return status;
}

Status CompactionJob::Install(const MutableCFOptions& mutable_cf_options) {
  assert(compact_);

  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_INSTALL);
  db_mutex_->AssertHeld();
  Status status = compact_->status;

  ColumnFamilyData* cfd = compact_->compaction->column_family_data();
  assert(cfd);

  cfd->internal_stats()->AddCompactionStats(
      compact_->compaction->output_level(), thread_pri_, compaction_stats_);

  if (status.ok()) {
    status = InstallCompactionResults(mutable_cf_options);
  }
  if (!versions_->io_status().ok()) {
    io_status_ = versions_->io_status();
  }

  VersionStorageInfo::LevelSummaryStorage tmp;
  auto vstorage = cfd->current()->storage_info();
  const auto& stats = compaction_stats_;

  double read_write_amp = 0.0;
  double write_amp = 0.0;
  double bytes_read_per_sec = 0;
  double bytes_written_per_sec = 0;

  const uint64_t bytes_read_non_output_and_blob =
      stats.bytes_read_non_output_levels + stats.bytes_read_blob;
  const uint64_t bytes_read_all =
      stats.bytes_read_output_level + bytes_read_non_output_and_blob;
  const uint64_t bytes_written_all =
      stats.bytes_written + stats.bytes_written_blob;

  if (bytes_read_non_output_and_blob > 0) {
    read_write_amp = (bytes_written_all + bytes_read_all) /
                     static_cast<double>(bytes_read_non_output_and_blob);
    write_amp =
        bytes_written_all / static_cast<double>(bytes_read_non_output_and_blob);
  }
  if (stats.micros > 0) {
    bytes_read_per_sec = bytes_read_all / static_cast<double>(stats.micros);
    bytes_written_per_sec =
        bytes_written_all / static_cast<double>(stats.micros);
  }

  const std::string& column_family_name = cfd->GetName();

  constexpr double kMB = 1048576.0;

  ROCKS_LOG_BUFFER(
      log_buffer_,
      "[%s] compacted to: %s, MB/sec: %.1f rd, %.1f wr, level %d, "
      "files in(%d, %d) out(%d +%d blob) "
      "MB in(%.1f, %.1f +%.1f blob) out(%.1f +%.1f blob), "
      "read-write-amplify(%.1f) write-amplify(%.1f) %s, records in: %" PRIu64
      ", records dropped: %" PRIu64 " output_compression: %s\n",
      column_family_name.c_str(), vstorage->LevelSummary(&tmp),
      bytes_read_per_sec, bytes_written_per_sec,
      compact_->compaction->output_level(),
      stats.num_input_files_in_non_output_levels,
      stats.num_input_files_in_output_level, stats.num_output_files,
      stats.num_output_files_blob, stats.bytes_read_non_output_levels / kMB,
      stats.bytes_read_output_level / kMB, stats.bytes_read_blob / kMB,
      stats.bytes_written / kMB, stats.bytes_written_blob / kMB, read_write_amp,
      write_amp, status.ToString().c_str(), stats.num_input_records,
      stats.num_dropped_records,
      CompressionTypeToString(compact_->compaction->output_compression())
          .c_str());

  const auto& blob_files = vstorage->GetBlobFiles();
  if (!blob_files.empty()) {
    ROCKS_LOG_BUFFER(log_buffer_,
                     "[%s] Blob file summary: head=%" PRIu64 ", tail=%" PRIu64
                     "\n",
                     column_family_name.c_str(), blob_files.begin()->first,
                     blob_files.rbegin()->first);
  }

  UpdateCompactionJobStats(stats);

  auto stream = event_logger_->LogToBuffer(log_buffer_, 8192);
  stream << "job" << job_id_ << "event"
         << "compaction_finished"
         << "compaction_time_micros" << stats.micros
         << "compaction_time_cpu_micros" << stats.cpu_micros << "output_level"
         << compact_->compaction->output_level() << "num_output_files"
         << compact_->num_output_files << "total_output_size"
         << compact_->total_bytes;

  if (compact_->num_blob_output_files > 0) {
    stream << "num_blob_output_files" << compact_->num_blob_output_files
           << "total_blob_output_size" << compact_->total_blob_bytes;
  }

  stream << "num_input_records" << stats.num_input_records
         << "num_output_records" << compact_->num_output_records
         << "num_subcompactions" << compact_->sub_compact_states.size()
         << "output_compression"
         << CompressionTypeToString(compact_->compaction->output_compression());

  stream << "num_single_delete_mismatches"
         << compaction_job_stats_->num_single_del_mismatch;
  stream << "num_single_delete_fallthrough"
         << compaction_job_stats_->num_single_del_fallthru;

  if (measure_io_stats_) {
    stream << "file_write_nanos" << compaction_job_stats_->file_write_nanos;
    stream << "file_range_sync_nanos"
           << compaction_job_stats_->file_range_sync_nanos;
    stream << "file_fsync_nanos" << compaction_job_stats_->file_fsync_nanos;
    stream << "file_prepare_write_nanos"
           << compaction_job_stats_->file_prepare_write_nanos;
  }

  stream << "lsm_state";
  stream.StartArray();
  for (int level = 0; level < vstorage->num_levels(); ++level) {
    stream << vstorage->NumLevelFiles(level);
  }
  stream.EndArray();

  if (!blob_files.empty()) {
    stream << "blob_file_head" << blob_files.begin()->first;
    stream << "blob_file_tail" << blob_files.rbegin()->first;
  }

  CleanupCompaction();
  return status;
}

#ifndef ROCKSDB_LITE
CompactionServiceJobStatus
CompactionJob::ProcessKeyValueCompactionWithCompactionService(
    SubcompactionState* sub_compact) {
  (void)sub_compact;
#if 0
  assert(sub_compact);
  assert(sub_compact->compaction);
  assert(db_options_.compaction_service);

  const Compaction* compaction = sub_compact->compaction;
  CompactionServiceInput compaction_input;
  compaction_input.output_level = compaction->output_level();

  const std::vector<CompactionInputFiles>& inputs =
      *(compact_->compaction->inputs());
  for (const auto& files_per_level : inputs) {
    for (const auto& file : files_per_level.files) {
      compaction_input.input_files.emplace_back(
          MakeTableFileName(file->fd.GetNumber()));
    }
  }
  compaction_input.column_family.name =
      compaction->column_family_data()->GetName();
  compaction_input.column_family.options =
      compaction->column_family_data()->GetLatestCFOptions();
  compaction_input.db_options =
      BuildDBOptions(db_options_, mutable_db_options_copy_);
  compaction_input.snapshots = existing_snapshots_;
  compaction_input.has_begin = sub_compact->start;
  compaction_input.begin =
      compaction_input.has_begin ? sub_compact->start->ToString() : "";
  compaction_input.has_end = sub_compact->end;
  compaction_input.end =
      compaction_input.has_end ? sub_compact->end->ToString() : "";
  compaction_input.approx_size = sub_compact->approx_size;

  std::string compaction_input_binary;
  Status s = compaction_input.Write(&compaction_input_binary);
  if (!s.ok()) {
    sub_compact->status = s;
    return CompactionServiceJobStatus::kFailure;
  }

  std::ostringstream input_files_oss;
  bool is_first_one = true;
  for (const auto& file : compaction_input.input_files) {
    input_files_oss << (is_first_one ? "" : ", ") << file;
    is_first_one = false;
  }

  ROCKS_LOG_INFO(
      db_options_.info_log,
      "[%s] [JOB %d] Starting remote compaction (output level: %d): %s",
      compaction_input.column_family.name.c_str(), job_id_,
      compaction_input.output_level, input_files_oss.str().c_str());
  CompactionServiceJobInfo info(dbname_, db_id_, db_session_id_,
                                GetCompactionId(sub_compact), thread_pri_);
  CompactionServiceJobStatus compaction_status =
      db_options_.compaction_service->StartV2(info, compaction_input_binary);
  switch (compaction_status) {
    case CompactionServiceJobStatus::kSuccess:
      break;
    case CompactionServiceJobStatus::kFailure:
      sub_compact->status = Status::Incomplete(
          "CompactionService failed to start compaction job.");
      ROCKS_LOG_WARN(db_options_.info_log,
                     "[%s] [JOB %d] Remote compaction failed to start.",
                     compaction_input.column_family.name.c_str(), job_id_);
      return compaction_status;
    case CompactionServiceJobStatus::kUseLocal:
      ROCKS_LOG_INFO(
          db_options_.info_log,
          "[%s] [JOB %d] Remote compaction fallback to local by API Start.",
          compaction_input.column_family.name.c_str(), job_id_);
      return compaction_status;
    default:
      assert(false);  // unknown status
      break;
  }

  ROCKS_LOG_INFO(db_options_.info_log,
                 "[%s] [JOB %d] Waiting for remote compaction...",
                 compaction_input.column_family.name.c_str(), job_id_);
  std::string compaction_result_binary;
  compaction_status = db_options_.compaction_service->WaitForCompleteV2(
      info, &compaction_result_binary);

  if (compaction_status == CompactionServiceJobStatus::kUseLocal) {
    ROCKS_LOG_INFO(db_options_.info_log,
                   "[%s] [JOB %d] Remote compaction fallback to local by API "
                   "WaitForComplete.",
                   compaction_input.column_family.name.c_str(), job_id_);
    return compaction_status;
  }

  CompactionServiceResult compaction_result;
  s = CompactionServiceResult::Read(compaction_result_binary,
                                    &compaction_result);

  if (compaction_status == CompactionServiceJobStatus::kFailure) {
    if (s.ok()) {
      if (compaction_result.status.ok()) {
        sub_compact->status = Status::Incomplete(
            "CompactionService failed to run the compaction job (even though "
            "the internal status is okay).");
      } else {
        // set the current sub compaction status with the status returned from
        // remote
        sub_compact->status = compaction_result.status;
      }
    } else {
      sub_compact->status = Status::Incomplete(
          "CompactionService failed to run the compaction job (and no valid "
          "result is returned).");
      compaction_result.status.PermitUncheckedError();
    }
    ROCKS_LOG_WARN(db_options_.info_log,
                   "[%s] [JOB %d] Remote compaction failed.",
                   compaction_input.column_family.name.c_str(), job_id_);
    return compaction_status;
  }

  if (!s.ok()) {
    sub_compact->status = s;
    compaction_result.status.PermitUncheckedError();
    return CompactionServiceJobStatus::kFailure;
  }
  sub_compact->status = compaction_result.status;

  std::ostringstream output_files_oss;
  is_first_one = true;
  for (const auto& file : compaction_result.output_files) {
    output_files_oss << (is_first_one ? "" : ", ") << file.file_name;
    is_first_one = false;
  }

  ROCKS_LOG_INFO(db_options_.info_log,
                 "[%s] [JOB %d] Receive remote compaction result, output path: "
                 "%s, files: %s",
                 compaction_input.column_family.name.c_str(), job_id_,
                 compaction_result.output_path.c_str(),
                 output_files_oss.str().c_str());

  if (!s.ok()) {
    sub_compact->status = s;
    return CompactionServiceJobStatus::kFailure;
  }

  for (const auto& file : compaction_result.output_files) {
    uint64_t file_num = versions_->NewFileNumber();
    auto src_file = compaction_result.output_path + "/" + file.file_name;
    auto tgt_file = TableFileName(compaction->immutable_options()->cf_paths,
                                  file_num, compaction->output_path_id());
    s = fs_->RenameFile(src_file, tgt_file, IOOptions(), nullptr);
    if (!s.ok()) {
      sub_compact->status = s;
      return CompactionServiceJobStatus::kFailure;
    }

    FileMetaData meta;
    uint64_t file_size;
    s = fs_->GetFileSize(tgt_file, IOOptions(), &file_size, nullptr);
    if (!s.ok()) {
      sub_compact->status = s;
      return CompactionServiceJobStatus::kFailure;
    }
    meta.fd = FileDescriptor(file_num, compaction->output_path_id(), file_size,
                             file.smallest_seqno, file.largest_seqno);
    meta.smallest.DecodeFrom(file.smallest_internal_key);
    meta.largest.DecodeFrom(file.largest_internal_key);
    meta.oldest_ancester_time = file.oldest_ancester_time;
    meta.file_creation_time = file.file_creation_time;
    meta.marked_for_compaction = file.marked_for_compaction;

    auto cfd = compaction->column_family_data();
    sub_compact->outputs.emplace_back(std::move(meta),
                                      cfd->internal_comparator(), false, false,
                                      true, file.paranoid_hash);
  }
  sub_compact->compaction_job_stats = compaction_result.stats;
  sub_compact->num_output_records = compaction_result.num_output_records;
  sub_compact->approx_size = compaction_input.approx_size;  // is this used?
  sub_compact->total_bytes = compaction_result.total_bytes;
  RecordTick(stats_, REMOTE_COMPACT_READ_BYTES, compaction_result.bytes_read);
  RecordTick(stats_, REMOTE_COMPACT_WRITE_BYTES,
             compaction_result.bytes_written);
  return CompactionServiceJobStatus::kSuccess;
#endif
  // Future work: Support this by adding an output level
  return CompactionServiceJobStatus::kFailure;
}
#endif  // !ROCKSDB_LITE

struct IKeyValueLevel {
  Slice key;
  ParsedInternalKey ikey;
  Slice value;
  int level;

  IKeyValueLevel() = default;
  IKeyValueLevel(const InternalKey& internal_key, Slice arg_value,
                 int arg_level)
      : key(internal_key.Encode()), value(arg_value), level(arg_level) {
    ParseInternalKey(key, &ikey, true);
  }
  IKeyValueLevel(Slice arg_key, ParsedInternalKey arg_ikey, Slice arg_value,
                 int arg_level)
      : key(arg_key), ikey(arg_ikey), value(arg_value), level(arg_level) {}
  IKeyValueLevel(CompactionIterator& c_iter)
      : key(c_iter.key()), ikey(c_iter.ikey()) {
    assert(c_iter.value().size() ==
           sizeof(int) + sizeof(const char*) + sizeof(size_t));
    const char* buf = c_iter.value().data();
    level = *(int*)buf;
    value = Slice(*(const char**)(buf + sizeof(int)),
                  *(size_t*)(buf + sizeof(int) + sizeof(const char*)));
  }
};

struct IKeyValue {
  Slice key;
  ParsedInternalKey ikey;
  Slice value;

  IKeyValue() = default;
  IKeyValue(IKeyValueLevel rhs) : IKeyValue(rhs.key, rhs.ikey, rhs.value) {}
  IKeyValue(const InternalKey& internal_key, Slice arg_value)
      : key(internal_key.Encode()), value(arg_value) {
    ParseInternalKey(key, &ikey, true);
  }
  IKeyValue(Slice arg_key, ParsedInternalKey arg_ikey, Slice arg_value)
      : key(arg_key), ikey(arg_ikey), value(arg_value) {}
};

enum class Decision {
  kUndetermined,
  kNextLevel,
  kStartLevel,
};

struct Elem {
  Decision decision;
  IKeyValue kv;
  Elem(const Elem&) = default;

  Elem(Decision arg_decision, IKeyValue arg_kv)
      : decision(arg_decision), kv(arg_kv) {}
  Elem(Decision arg_decision, Slice key, ParsedInternalKey ikey, Slice value)
      : decision(arg_decision), kv(key, ikey, value) {}
  Elem(Decision arg_decision, const InternalKey& internal_key, Slice arg_value)
      : decision(arg_decision), kv(internal_key, arg_value) {}
};

// Future work: The caller should ZeroOutSequenceIfPossible if the final
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
      : c_iter_(c_iter),
        timers_(c.column_family_data()->internal_stats()->hotrap_timers()) {}
  optional<Elem> next() override {
    optional<IKeyValueLevel> ret = c_iter_.next();
    if (ret.has_value())
      return make_optional<Elem>(Decision::kNextLevel, ret.value());
    else
      return nullopt;
  }

 private:
  CompactionIterWrapper c_iter_;

  const TypedTimers<TimerType>& timers_;
};

class RouterIteratorIntraTier : public TraitIterator<Elem> {
 public:
  RouterIteratorIntraTier(CompactionRouter& router, const Compaction& c,
                          CompactionIterator& c_iter, Slice start, Bound end,
                          Tickers promotion_type)
      : router_(router),
        c_(c),
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
  CompactionRouter& router_;
  const Compaction& c_;
  Tickers promotion_type_;
  size_t promoted_bytes_;
  CompactionIterWrapper iter_;
};

class RouterIteratorFD2SD : public TraitIterator<Elem> {
 public:
  RouterIteratorFD2SD(CompactionRouter& router, const Compaction& c,
                      CompactionIterator& c_iter, Slice start, Bound end)
      : router_(router),
        c_(c),
        start_(start),
        end_(end),
        ucmp_(c.column_family_data()->user_comparator()),
        iter_(c_iter),
        hot_iter_(router.LowerBound(start_)),
        kvsize_promoted_(0),
        kvsize_retained_(0) {}
  ~RouterIteratorFD2SD() {
    auto stats = c_.immutable_options()->stats;
    RecordTick(stats, Tickers::PROMOTED_2FDLAST_BYTES, kvsize_promoted_);
    RecordTick(stats, Tickers::RETAINED_BYTES, kvsize_retained_);
  }
  Decision route(const IKeyValueLevel& kv) {
    // It is guaranteed that all versions of the same user key share the same
    // decision.
    const HotRecInfo* hot = hot_iter_.peek();
    while (hot != nullptr) {
      if (ucmp_->Compare(hot->last, kv.ikey.user_key) >= 0) break;
      hot_iter_.next();
      hot = hot_iter_.peek();
    }
    if (!hot) return Decision::kNextLevel;
    // kv.ikey.user_key <= hot->last
    Slice first = hot->first.empty() ? hot->last : hot->first;
    if (ucmp_->Compare(first, kv.ikey.user_key) <= 0) {
      return Decision::kStartLevel;
    } else {
      return Decision::kNextLevel;
    }
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
  CompactionRouter& router_;
  const Compaction& c_;
  const Slice start_;
  const Bound end_;

  const Comparator* ucmp_;
  CompactionIterWrapper iter_;
  Peekable<CompactionRouter::Iter> hot_iter_;

  size_t kvsize_promoted_;
  size_t kvsize_retained_;
};
class RouterIterator {
 public:
  RouterIterator(CompactionRouter* router, const Compaction& c,
                 CompactionIterator& c_iter, Slice start, Bound end)
      : timers_(c.column_family_data()->internal_stats()->hotrap_timers()) {
    int start_level = c.level();
    int latter_level = c.output_level();
    if (router == NULL) {
      // Future work: Handle the case that it's not empty, which is possible
      // when router was not NULL but then is set to NULL.
      assert(c.cached_records_to_promote().empty());
      iter_ = std::unique_ptr<IteratorWithoutRouter>(
          new IteratorWithoutRouter(c, c_iter));
    } else {
      size_t start_tier = router->Tier(start_level);
      size_t latter_tier = router->Tier(latter_level);
      if (start_tier != latter_tier) {
        iter_ = std::unique_ptr<RouterIteratorFD2SD>(
            new RouterIteratorFD2SD(*router, c, c_iter, start, end));
      } else if (router->Tier(latter_level + 1) != latter_tier) {
        iter_ = std::unique_ptr<RouterIteratorIntraTier>(
            new RouterIteratorIntraTier(*router, c, c_iter, start, end,
                                        Tickers::PROMOTED_2FDLAST_BYTES));
      } else {
        iter_ = std::unique_ptr<RouterIteratorIntraTier>(
            new RouterIteratorIntraTier(*router, c, c_iter, start, end,
                                        Tickers::PROMOTED_2SDFRONT_BYTES));
      }
    }
    cur_ = iter_->next();
  }
  bool Valid() { return cur_.has_value(); }
  void Next() {
    assert(Valid());
    cur_ = iter_->next();
  }
  Decision decision() { return cur_.value().decision; }
  const Slice& key() const { return cur_.value().kv.key; }
  const ParsedInternalKey& ikey() const { return cur_.value().kv.ikey; }
  const Slice& user_key() const { return cur_.value().kv.ikey.user_key; }
  const Slice& value() const { return cur_.value().kv.value; }

 private:
  std::unique_ptr<TraitIterator<Elem>> iter_;
  optional<Elem> cur_;

  const TypedTimers<TimerType>& timers_;
};

void CompactionJob::ProcessKeyValueCompaction(SubcompactionState* sub_compact) {
  assert(sub_compact);
  assert(sub_compact->compaction);

#ifndef ROCKSDB_LITE
  if (db_options_.compaction_service) {
    CompactionServiceJobStatus comp_status =
        ProcessKeyValueCompactionWithCompactionService(sub_compact);
    if (comp_status == CompactionServiceJobStatus::kSuccess ||
        comp_status == CompactionServiceJobStatus::kFailure) {
      return;
    }
    // fallback to local compaction
    assert(comp_status == CompactionServiceJobStatus::kUseLocal);
  }
#endif  // !ROCKSDB_LITE

  uint64_t prev_cpu_micros = db_options_.clock->CPUNanos() / 1000;

  const Compaction* c = sub_compact->compaction;
  ColumnFamilyData* cfd = c->column_family_data();
  const Comparator* ucmp = cfd->user_comparator();

  // Create compaction filter and fail the compaction if
  // IgnoreSnapshots() = false because it is not supported anymore
  const CompactionFilter* compaction_filter =
      cfd->ioptions()->compaction_filter;
  std::unique_ptr<CompactionFilter> compaction_filter_from_factory = nullptr;
  if (compaction_filter == nullptr) {
    compaction_filter_from_factory = c->CreateCompactionFilter();
    compaction_filter = compaction_filter_from_factory.get();
  }
  if (compaction_filter != nullptr && !compaction_filter->IgnoreSnapshots()) {
    sub_compact->status = Status::NotSupported(
        "CompactionFilter::IgnoreSnapshots() = false is not supported "
        "anymore.");
    return;
  }
  CompactionRouter* router = c->mutable_cf_options()->compaction_router;
  TimerGuard timer_guard =
      cfd->internal_stats()
          ->hotrap_timers_per_level()
          .timer(c->start_level(),
                 PerLevelTimerType::kProcessKeyValueCompaction)
          .start();
  const int level = c->level();

  CompactionRangeDelAggregator range_del_agg(&cfd->internal_comparator(),
                                             existing_snapshots_);

  const Slice* const start = sub_compact->start;
  const Slice* const end = sub_compact->end;

  ReadOptions read_options;
  read_options.verify_checksums = true;
  read_options.fill_cache = false;
  // Compaction iterators shouldn't be confined to a single prefix.
  // Compactions use Seek() for
  // (a) concurrent compactions,
  // (b) CompactionFilter::Decision::kRemoveAndSkipUntil.
  read_options.total_order_seek = true;

  // Note: if we're going to support subcompactions for user-defined
  // timestamps, the timestamp part will have to be stripped from the bounds
  // here.
  assert((!start && !end) || ucmp->timestamp_size() == 0);
  read_options.iterate_lower_bound = start;
  read_options.iterate_upper_bound = end;

  // Although the v2 aggregator is what the level iterator(s) know about,
  // the AddTombstones calls will be propagated down to the v1 aggregator.
  std::unique_ptr<InternalIterator> raw_input(
      versions_->MakeInputIterator(read_options, sub_compact->compaction,
                                   &range_del_agg, file_options_for_read_));
  InternalIterator* input = raw_input.get();

  IterKey start_ikey;
  IterKey end_ikey;
  Slice start_slice;
  Slice end_slice;

  if (start) {
    start_ikey.SetInternalKey(*start, kMaxSequenceNumber, kValueTypeForSeek);
    start_slice = start_ikey.GetInternalKey();
  }
  if (end) {
    end_ikey.SetInternalKey(*end, kMaxSequenceNumber, kValueTypeForSeek);
    end_slice = end_ikey.GetInternalKey();
  }

  std::unique_ptr<InternalIterator> clip;
  if (start || end) {
    clip.reset(new ClippingIterator(
        raw_input.get(), start ? &start_slice : nullptr,
        end ? &end_slice : nullptr, &cfd->internal_comparator()));
    input = clip.get();
  }

  std::unique_ptr<InternalIterator> blob_counter;

  if (sub_compact->compaction->DoesInputReferenceBlobFiles()) {
    sub_compact->blob_garbage_meter.reset(new BlobGarbageMeter);
    blob_counter.reset(
        new BlobCountingIterator(input, sub_compact->blob_garbage_meter.get()));
    input = blob_counter.get();
  }

  input->SeekToFirst();

  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_PROCESS_KV);

  // I/O measurement variables
  PerfLevel prev_perf_level = PerfLevel::kEnableTime;
  const uint64_t kRecordStatsEvery = 1000;
  uint64_t prev_write_nanos = 0;
  uint64_t prev_fsync_nanos = 0;
  uint64_t prev_range_sync_nanos = 0;
  uint64_t prev_prepare_write_nanos = 0;
  uint64_t prev_cpu_write_nanos = 0;
  uint64_t prev_cpu_read_nanos = 0;
  if (measure_io_stats_) {
    prev_perf_level = GetPerfLevel();
    SetPerfLevel(PerfLevel::kEnableTimeAndCPUTimeExceptForMutex);
    prev_write_nanos = IOSTATS(write_nanos);
    prev_fsync_nanos = IOSTATS(fsync_nanos);
    prev_range_sync_nanos = IOSTATS(range_sync_nanos);
    prev_prepare_write_nanos = IOSTATS(prepare_write_nanos);
    prev_cpu_write_nanos = IOSTATS(cpu_write_nanos);
    prev_cpu_read_nanos = IOSTATS(cpu_read_nanos);
  }

  MergeHelper merge(
      env_, ucmp, cfd->ioptions()->merge_operator.get(), compaction_filter,
      db_options_.info_log.get(),
      false /* internal key corruption is expected */,
      existing_snapshots_.empty() ? 0 : existing_snapshots_.back(),
      snapshot_checker_, compact_->compaction->level(), db_options_.stats);

  const MutableCFOptions* mutable_cf_options =
      sub_compact->compaction->mutable_cf_options();
  assert(mutable_cf_options);

  std::vector<std::string> blob_file_paths;

  std::unique_ptr<BlobFileBuilder> blob_file_builder(
      mutable_cf_options->enable_blob_files
          ? new BlobFileBuilder(
                versions_, fs_.get(),
                sub_compact->compaction->immutable_options(),
                mutable_cf_options, &file_options_, job_id_, cfd->GetID(),
                cfd->GetName(), Env::IOPriority::IO_LOW, write_hint_,
                io_tracer_, blob_callback_, BlobFileCreationReason::kCompaction,
                &blob_file_paths, &sub_compact->blob_file_additions)
          : nullptr);

  TEST_SYNC_POINT("CompactionJob::Run():Inprogress");
  TEST_SYNC_POINT_CALLBACK(
      "CompactionJob::Run():PausingManualCompaction:1",
      reinterpret_cast<void*>(
          const_cast<std::atomic<int>*>(manual_compaction_paused_)));

  Status status;
  const std::string* const full_history_ts_low =
      full_history_ts_low_.empty() ? nullptr : &full_history_ts_low_;
  sub_compact->c_iter.reset(new CompactionIterator(
      input, ucmp, &merge, versions_->LastSequence(), &existing_snapshots_,
      earliest_write_conflict_snapshot_, snapshot_checker_, env_,
      ShouldReportDetailedTime(env_, stats_),
      /*expect_valid_internal_key=*/true, &range_del_agg,
      blob_file_builder.get(), db_options_.allow_data_in_errors,
      sub_compact->compaction, compaction_filter, shutting_down_,
      preserve_deletes_seqnum_, manual_compaction_paused_,
      manual_compaction_canceled_, db_options_.info_log, full_history_ts_low));
  auto c_iter = sub_compact->c_iter.get();
  c_iter->SeekToFirst();
  if (c_iter->Valid() && sub_compact->compaction->output_level() != 0) {
    sub_compact->FillFilesToCutForTtl();
    // ShouldStopBefore() maintains state based on keys processed so far. The
    // compaction loop always calls it on the "next" key, thus won't tell it
    // the first key. So we do that here.
    sub_compact->ShouldStopBefore(c_iter->key(),
                                  // sub_compact->current_output_file_size);
                                  0);  // TODO: Is this correct?
  }
  const auto& c_iter_stats = c_iter->iter_stats();

  std::unique_ptr<SstPartitioner> partitioner =
      sub_compact->compaction->output_level() == 0
          ? nullptr
          : sub_compact->compaction->CreateSstPartitioner();
  std::string last_key_for_partitioner;

  // Future work: How to handle other cases?
  assert(c->num_input_levels() <= 2);
  const CompactionInputFiles& start_level_inputs = (*c->inputs())[0];
  assert(start_level_inputs.level == c->start_level());
  Slice start_level_smallest_user_key, start_level_largest_user_key;
  start_level_inputs.GetBoundaryKeys(ucmp, &start_level_smallest_user_key,
                                     &start_level_largest_user_key);
  Slice promotable_start =
      start == NULL ? start_level_smallest_user_key
                    : (ucmp->Compare(*start, start_level_smallest_user_key) < 0
                           ? start_level_smallest_user_key
                           : *start);
  Bound promotable_end =
      end == NULL
          ? Bound{.user_key = start_level_largest_user_key, .excluded = false}
          : (ucmp->Compare(*end, start_level_largest_user_key) <= 0
                 ? Bound{.user_key = *end, .excluded = true}
                 : Bound{
                       .user_key = start_level_largest_user_key,
                       .excluded = false,
                   });
  RouterIterator router_iter(router, *c, *c_iter, promotable_start,
                             promotable_end);

  std::string previous_user_key;
  auto compaction_start = rusty::time::Instant::now();
  while (status.ok() && !cfd->IsDropped() && router_iter.Valid()) {
    // Invariant: router_iter.status() is guaranteed to be OK if
    // router_iter->Valid() returns true.
    const Slice& key = router_iter.key();
    const Slice& value = router_iter.value();

    assert(!end || ucmp->Compare(router_iter.user_key(), *end) < 0);

    if (c_iter_stats.num_input_records % kRecordStatsEvery ==
        kRecordStatsEvery - 1) {
      RecordDroppedKeys(c_iter_stats, &sub_compact->compaction_job_stats);
      c_iter->ResetRecordCounts();
      RecordCompactionIOStats();
    }

    SubcompactionState::LevelOutput* level_output = NULL;
    switch (router_iter.decision()) {
      case Decision::kStartLevel:
        level_output = &sub_compact->start_level_output;
        break;
      case Decision::kNextLevel:
        level_output = &sub_compact->latter_level_output;
        break;
      case Decision::kUndetermined:
        assert(false);
        break;
    }
    assert(level_output != NULL);

    // Open output file if necessary
    if (level_output->builder == nullptr) {
      status = OpenCompactionOutputFile(sub_compact, level_output);
      if (!status.ok()) {
        break;
      }
    }
    status = level_output->AddToBuilder(key, value);
    if (!status.ok()) {
      break;
    }

    status = sub_compact->ProcessOutFlowIfNeeded(key, value);
    if (!status.ok()) {
      break;
    }

    level_output->current_output_file_size =
        level_output->builder->EstimatedFileSize();
    const ParsedInternalKey& ikey = router_iter.ikey();
    level_output->current_output()->meta.UpdateBoundaries(
        key, value, ikey.sequence, ikey.type);
    sub_compact->num_output_records++;

    // Close output file if it is big enough. Two possibilities determine it's
    // time to close it: (1) the current key should be this file's last key,
    // (2) the next key should not be in this file.
    //
    // TODO(aekmekji): determine if file should be closed earlier than this
    // during subcompactions (i.e. if output size, estimated by input size, is
    // going to be 1.2MB and max_output_file_size = 1MB, prefer to have 0.6MB
    // and 0.6MB instead of 1MB and 0.2MB)
    bool output_file_ended = false;
    if (sub_compact->compaction->output_level() != 0 &&
        level_output->current_output_file_size >=
            sub_compact->compaction->max_output_file_size()) {
      // (1) this key terminates the file. For historical reasons, the
      // iterator status before advancing will be given to
      // FinishCompactionOutputFile().
      output_file_ended = true;
    }
    TEST_SYNC_POINT_CALLBACK(
        "CompactionJob::Run():PausingManualCompaction:2",
        reinterpret_cast<void*>(
            const_cast<std::atomic<int>*>(manual_compaction_paused_)));
    if (partitioner.get()) {
      last_key_for_partitioner.assign(router_iter.user_key().data_,
                                      router_iter.user_key().size_);
    }
    router_iter.Next();
    if (c_iter->status().IsManualCompactionPaused()) {
      break;
    }
    if (!output_file_ended && router_iter.Valid()) {
      if (((partitioner.get() &&
            partitioner->ShouldPartition(PartitionerRequest(
                last_key_for_partitioner, router_iter.user_key(),
                level_output->current_output_file_size)) == kRequired) ||
           (sub_compact->compaction->output_level() != 0 &&
            sub_compact->ShouldStopBefore(
                router_iter.key(), level_output->current_output_file_size))) &&
          level_output->builder != nullptr) {
        // (2) this key belongs to the next file. For historical reasons, the
        // iterator status after advancing will be given to
        // FinishCompactionOutputFile().
        output_file_ended = true;
      }
    }
    if (output_file_ended) {
      const Slice* next_key = nullptr;
      if (router_iter.Valid()) {
        next_key = &router_iter.key();
      }
      CompactionIterationStats range_del_out_stats;
      status = FinishCompactionOutputFile(input->status(), sub_compact,
                                          level_output, &range_del_agg,
                                          &range_del_out_stats, next_key);
      RecordDroppedKeys(range_del_out_stats,
                        &sub_compact->compaction_job_stats);
    }
  }

  sub_compact->compaction_job_stats.num_blobs_read =
      c_iter_stats.num_blobs_read;
  sub_compact->compaction_job_stats.total_blob_bytes_read =
      c_iter_stats.total_blob_bytes_read;
  sub_compact->compaction_job_stats.num_input_deletion_records =
      c_iter_stats.num_input_deletion_records;
  sub_compact->compaction_job_stats.num_corrupt_keys =
      c_iter_stats.num_input_corrupt_records;
  sub_compact->compaction_job_stats.num_single_del_fallthru =
      c_iter_stats.num_single_del_fallthru;
  sub_compact->compaction_job_stats.num_single_del_mismatch =
      c_iter_stats.num_single_del_mismatch;
  sub_compact->compaction_job_stats.total_input_raw_key_bytes +=
      c_iter_stats.total_input_raw_key_bytes;
  sub_compact->compaction_job_stats.total_input_raw_value_bytes +=
      c_iter_stats.total_input_raw_value_bytes;

  RecordTick(stats_, FILTER_OPERATION_TOTAL_TIME,
             c_iter_stats.total_filter_time);

  if (c_iter_stats.num_blobs_relocated > 0) {
    RecordTick(stats_, BLOB_DB_GC_NUM_KEYS_RELOCATED,
               c_iter_stats.num_blobs_relocated);
  }
  if (c_iter_stats.total_blob_bytes_relocated > 0) {
    RecordTick(stats_, BLOB_DB_GC_BYTES_RELOCATED,
               c_iter_stats.total_blob_bytes_relocated);
  }

  RecordDroppedKeys(c_iter_stats, &sub_compact->compaction_job_stats);
  RecordCompactionIOStats();

  if (status.ok() && cfd->IsDropped()) {
    status =
        Status::ColumnFamilyDropped("Column family dropped during compaction");
  }
  if ((status.ok() || status.IsColumnFamilyDropped()) &&
      shutting_down_->load(std::memory_order_relaxed)) {
    status = Status::ShutdownInProgress("Database shutdown");
  }
  if ((status.ok() || status.IsColumnFamilyDropped()) &&
      ((manual_compaction_paused_ &&
        manual_compaction_paused_->load(std::memory_order_relaxed) > 0) ||
       (manual_compaction_canceled_ &&
        manual_compaction_canceled_->load(std::memory_order_relaxed)))) {
    status = Status::Incomplete(Status::SubCode::kManualCompactionPaused);
  }
  if (status.ok()) {
    status = input->status();
  }
  if (status.ok()) {
    status = c_iter->status();
  }

  if (status.ok() && sub_compact->latter_level_output.builder == nullptr &&
      sub_compact->latter_level_output.outputs.size() == 0 &&
      !range_del_agg.IsEmpty()) {
    // handle subcompaction containing only range deletions
    // Range deletions are always output to the next level.
    status = OpenCompactionOutputFile(sub_compact,
                                      &sub_compact->latter_level_output);
  }

  // Call FinishCompactionOutputFile() even if status is not ok: it needs to
  // close the output file.
  if (sub_compact->start_level_output.builder != nullptr) {
    CompactionIterationStats range_del_out_stats;
    Status s = FinishCompactionOutputFile(status, sub_compact,
                                          &sub_compact->start_level_output,
                                          &range_del_agg, &range_del_out_stats);
    if (!s.ok() && status.ok()) {
      status = s;
    }
    RecordDroppedKeys(range_del_out_stats, &sub_compact->compaction_job_stats);
  }
  if (sub_compact->latter_level_output.builder != nullptr) {
    CompactionIterationStats range_del_out_stats;
    Status s = FinishCompactionOutputFile(status, sub_compact,
                                          &sub_compact->latter_level_output,
                                          &range_del_agg, &range_del_out_stats);
    if (!s.ok() && status.ok()) {
      status = s;
    }
    RecordDroppedKeys(range_del_out_stats, &sub_compact->compaction_job_stats);
  }

  if (blob_file_builder) {
    if (status.ok()) {
      status = blob_file_builder->Finish();
    } else {
      blob_file_builder->Abandon(status);
    }
    blob_file_builder.reset();
  }

  cfd->internal_stats()
      ->hotrap_timers()
      .timer(TimerType::kCompaction)
      .add(compaction_start.elapsed());

  sub_compact->compaction_job_stats.cpu_micros =
      db_options_.clock->CPUNanos() / 1000 - prev_cpu_micros;

  if (measure_io_stats_) {
    sub_compact->compaction_job_stats.file_write_nanos +=
        IOSTATS(write_nanos) - prev_write_nanos;
    sub_compact->compaction_job_stats.file_fsync_nanos +=
        IOSTATS(fsync_nanos) - prev_fsync_nanos;
    sub_compact->compaction_job_stats.file_range_sync_nanos +=
        IOSTATS(range_sync_nanos) - prev_range_sync_nanos;
    sub_compact->compaction_job_stats.file_prepare_write_nanos +=
        IOSTATS(prepare_write_nanos) - prev_prepare_write_nanos;
    sub_compact->compaction_job_stats.cpu_micros -=
        (IOSTATS(cpu_write_nanos) - prev_cpu_write_nanos +
         IOSTATS(cpu_read_nanos) - prev_cpu_read_nanos) /
        1000;
    if (prev_perf_level != PerfLevel::kEnableTimeAndCPUTimeExceptForMutex) {
      SetPerfLevel(prev_perf_level);
    }
  }
#ifdef ROCKSDB_ASSERT_STATUS_CHECKED
  if (!status.ok()) {
    if (sub_compact->c_iter) {
      sub_compact->c_iter->status().PermitUncheckedError();
    }
    if (input) {
      input->status().PermitUncheckedError();
    }
  }
#endif  // ROCKSDB_ASSERT_STATUS_CHECKED

  sub_compact->c_iter.reset();
  blob_counter.reset();
  clip.reset();
  raw_input.reset();
  sub_compact->status = status;
}

uint64_t CompactionJob::GetCompactionId(SubcompactionState* sub_compact) {
  return (uint64_t)job_id_ << 32 | sub_compact->sub_job_id;
}

void CompactionJob::RecordDroppedKeys(
    const CompactionIterationStats& c_iter_stats,
    CompactionJobStats* compaction_job_stats) {
  if (c_iter_stats.num_record_drop_user > 0) {
    RecordTick(stats_, COMPACTION_KEY_DROP_USER,
               c_iter_stats.num_record_drop_user);
  }
  if (c_iter_stats.num_record_drop_hidden > 0) {
    RecordTick(stats_, COMPACTION_KEY_DROP_NEWER_ENTRY,
               c_iter_stats.num_record_drop_hidden);
    if (compaction_job_stats) {
      compaction_job_stats->num_records_replaced +=
          c_iter_stats.num_record_drop_hidden;
    }
  }
  if (c_iter_stats.num_record_drop_obsolete > 0) {
    RecordTick(stats_, COMPACTION_KEY_DROP_OBSOLETE,
               c_iter_stats.num_record_drop_obsolete);
    if (compaction_job_stats) {
      compaction_job_stats->num_expired_deletion_records +=
          c_iter_stats.num_record_drop_obsolete;
    }
  }
  if (c_iter_stats.num_record_drop_range_del > 0) {
    RecordTick(stats_, COMPACTION_KEY_DROP_RANGE_DEL,
               c_iter_stats.num_record_drop_range_del);
  }
  if (c_iter_stats.num_range_del_drop_obsolete > 0) {
    RecordTick(stats_, COMPACTION_RANGE_DEL_DROP_OBSOLETE,
               c_iter_stats.num_range_del_drop_obsolete);
  }
  if (c_iter_stats.num_optimized_del_drop_obsolete > 0) {
    RecordTick(stats_, COMPACTION_OPTIMIZED_DEL_DROP_OBSOLETE,
               c_iter_stats.num_optimized_del_drop_obsolete);
  }
}

Status CompactionJob::FinishCompactionOutputFile(
    const Status& input_status, SubcompactionState* sub_compact,
    SubcompactionState::LevelOutput* level_output,
    CompactionRangeDelAggregator* range_del_agg,
    CompactionIterationStats* range_del_out_stats,
    const Slice* next_table_min_key /* = nullptr */) {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_SYNC_FILE);
  assert(sub_compact != nullptr);
  assert(level_output->outfile);
  assert(level_output->builder != nullptr);
  assert(level_output->current_output() != nullptr);

  ColumnFamilyData* cfd = sub_compact->compaction->column_family_data();
  const Comparator* ucmp = cfd->user_comparator();

  // Check for iterator errors
  Status s = input_status;
  auto meta = &level_output->current_output()->meta;
  assert(meta != nullptr);
  if (s.ok()) {
    Slice lower_bound_guard, upper_bound_guard;
    std::string smallest_user_key;
    const Slice *lower_bound, *upper_bound;
    bool lower_bound_from_sub_compact = false;
    if (level_output->outputs.size() == 1) {
      // For the first output table, include range tombstones before the min
      // key but after the subcompaction boundary.
      lower_bound = sub_compact->start;
      lower_bound_from_sub_compact = true;
    } else if (meta->smallest.size() > 0) {
      // For subsequent output tables, only include range tombstones from min
      // key onwards since the previous file was extended to contain range
      // tombstones falling before min key.
      smallest_user_key = meta->smallest.user_key().ToString(false /*hex*/);
      lower_bound_guard = Slice(smallest_user_key);
      lower_bound = &lower_bound_guard;
    } else {
      lower_bound = nullptr;
    }
    if (next_table_min_key != nullptr) {
      // This may be the last file in the subcompaction in some cases, so we
      // need to compare the end key of subcompaction with the next file start
      // key. When the end key is chosen by the subcompaction, we know that
      // it must be the biggest key in output file. Therefore, it is safe to
      // use the smaller key as the upper bound of the output file, to ensure
      // that there is no overlapping between different output files.
      upper_bound_guard = ExtractUserKey(*next_table_min_key);
      if (sub_compact->end != nullptr &&
          ucmp->Compare(upper_bound_guard, *sub_compact->end) >= 0) {
        upper_bound = sub_compact->end;
      } else {
        upper_bound = &upper_bound_guard;
      }
    } else {
      // This is the last file in the subcompaction, so extend until the
      // subcompaction ends.
      upper_bound = sub_compact->end;
    }
    auto earliest_snapshot = kMaxSequenceNumber;
    if (existing_snapshots_.size() > 0) {
      earliest_snapshot = existing_snapshots_[0];
    }
    bool has_overlapping_endpoints;
    if (upper_bound != nullptr && meta->largest.size() > 0) {
      has_overlapping_endpoints =
          ucmp->Compare(meta->largest.user_key(), *upper_bound) == 0;
    } else {
      has_overlapping_endpoints = false;
    }

    // The end key of the subcompaction must be bigger or equal to the upper
    // bound. If the end of subcompaction is null or the upper bound is null,
    // it means that this file is the last file in the compaction. So there
    // will be no overlapping between this file and others.
    assert(sub_compact->end == nullptr || upper_bound == nullptr ||
           ucmp->Compare(*upper_bound, *sub_compact->end) <= 0);
    auto it = range_del_agg->NewIterator(lower_bound, upper_bound,
                                         has_overlapping_endpoints);
    // Position the range tombstone output iterator. There may be tombstone
    // fragments that are entirely out of range, so make sure that we do not
    // include those.
    if (lower_bound != nullptr) {
      it->Seek(*lower_bound);
    } else {
      it->SeekToFirst();
    }
    TEST_SYNC_POINT("CompactionJob::FinishCompactionOutputFile1");
    for (; it->Valid(); it->Next()) {
      auto tombstone = it->Tombstone();
      if (upper_bound != nullptr) {
        int cmp = ucmp->Compare(*upper_bound, tombstone.start_key_);
        if ((has_overlapping_endpoints && cmp < 0) ||
            (!has_overlapping_endpoints && cmp <= 0)) {
          // Tombstones starting after upper_bound only need to be included in
          // the next table. If the current SST ends before upper_bound, i.e.,
          // `has_overlapping_endpoints == false`, we can also skip over range
          // tombstones that start exactly at upper_bound. Such range
          // tombstones will be included in the next file and are not relevant
          // to the point keys or endpoints of the current file.
          break;
        }
      }

      if (bottommost_level_ && tombstone.seq_ <= earliest_snapshot) {
        // TODO(andrewkr): tombstones that span multiple output files are
        // counted for each compaction output file, so lots of double
        // counting.
        range_del_out_stats->num_range_del_drop_obsolete++;
        range_del_out_stats->num_record_drop_obsolete++;
        continue;
      }

      auto kv = tombstone.Serialize();
      assert(lower_bound == nullptr ||
             ucmp->Compare(*lower_bound, kv.second) < 0);
      // Range tombstone is not supported by output validator yet.
      level_output->builder->Add(kv.first.Encode(), kv.second);
      InternalKey smallest_candidate = std::move(kv.first);
      if (lower_bound != nullptr &&
          ucmp->Compare(smallest_candidate.user_key(), *lower_bound) <= 0) {
        // Pretend the smallest key has the same user key as lower_bound
        // (the max key in the previous table or subcompaction) in order for
        // files to appear key-space partitioned.
        //
        // When lower_bound is chosen by a subcompaction, we know that
        // subcompactions over smaller keys cannot contain any keys at
        // lower_bound. We also know that smaller subcompactions exist,
        // because otherwise the subcompaction woud be unbounded on the left.
        // As a result, we know that no other files on the output level will
        // contain actual keys at lower_bound (an output file may have a
        // largest key of lower_bound@kMaxSequenceNumber, but this only
        // indicates a large range tombstone was truncated). Therefore, it is
        // safe to use the tombstone's sequence number, to ensure that keys at
        // lower_bound at lower levels are covered by truncated tombstones.
        //
        // If lower_bound was chosen by the smallest data key in the file,
        // choose lowest seqnum so this file's smallest internal key comes
        // after the previous file's largest. The fake seqnum is OK because
        // the read path's file-picking code only considers user key.
        smallest_candidate = InternalKey(
            *lower_bound, lower_bound_from_sub_compact ? tombstone.seq_ : 0,
            kTypeRangeDeletion);
      }
      InternalKey largest_candidate = tombstone.SerializeEndKey();
      if (upper_bound != nullptr &&
          ucmp->Compare(*upper_bound, largest_candidate.user_key()) <= 0) {
        // Pretend the largest key has the same user key as upper_bound (the
        // min key in the following table or subcompaction) in order for files
        // to appear key-space partitioned.
        //
        // Choose highest seqnum so this file's largest internal key comes
        // before the next file's/subcompaction's smallest. The fake seqnum is
        // OK because the read path's file-picking code only considers the
        // user key portion.
        //
        // Note Seek() also creates InternalKey with (user_key,
        // kMaxSequenceNumber), but with kTypeDeletion (0x7) instead of
        // kTypeRangeDeletion (0xF), so the range tombstone comes before the
        // Seek() key in InternalKey's ordering. So Seek() will look in the
        // next file for the user key.
        largest_candidate =
            InternalKey(*upper_bound, kMaxSequenceNumber, kTypeRangeDeletion);
      }
#ifndef NDEBUG
      SequenceNumber smallest_ikey_seqnum = kMaxSequenceNumber;
      if (meta->smallest.size() > 0) {
        smallest_ikey_seqnum = GetInternalKeySeqno(meta->smallest.Encode());
      }
#endif
      meta->UpdateBoundariesForRange(smallest_candidate, largest_candidate,
                                     tombstone.seq_,
                                     cfd->internal_comparator());
      // The smallest key in a file is used for range tombstone truncation, so
      // it cannot have a seqnum of 0 (unless the smallest data key in a file
      // has a seqnum of 0). Otherwise, the truncated tombstone may expose
      // deleted keys at lower levels.
      assert(smallest_ikey_seqnum == 0 ||
             ExtractInternalKeyFooter(meta->smallest.Encode()) !=
                 PackSequenceAndType(0, kTypeRangeDeletion));
    }
  }
  return WriteCompactionOutputFile(s, sub_compact, level_output);
}

Status CompactionJob::WriteCompactionOutputFile(
    const Status& input_status, SubcompactionState* sub_compact,
    SubcompactionState::LevelOutput* level_output) {
  Status s = input_status;
  auto meta = &level_output->current_output()->meta;
  std::string file_checksum = kUnknownFileChecksum;
  std::string file_checksum_func_name = kUnknownFileChecksumFuncName;
  const uint64_t current_entries = level_output->builder->NumEntries();
  ColumnFamilyData* cfd = sub_compact->compaction->column_family_data();

  uint64_t output_number = meta->fd.GetNumber();
  assert(output_number != 0);
  int output_level = level_output->level();

  if (s.ok()) {
    s = level_output->builder->Finish(level_output->hot_per_byte_);
  } else {
    level_output->builder->Abandon();
  }
  IOStatus io_s = level_output->builder->io_status();
  if (s.ok()) {
    s = io_s;
  }
  const uint64_t current_bytes = level_output->builder->FileSize();
  if (s.ok()) {
    meta->fd.file_size = current_bytes;
    meta->marked_for_compaction = level_output->builder->NeedCompact();
    // With accurate smallest and largest key, we can get a slightly more
    // accurate oldest ancester time.
    // This makes oldest ancester time in manifest more accurate than in
    // table properties. Not sure how to resolve it.
    if (meta->smallest.size() > 0 && meta->largest.size() > 0) {
      uint64_t refined_oldest_ancester_time;
      Slice new_smallest = meta->smallest.user_key();
      Slice new_largest = meta->largest.user_key();
      if (!new_largest.empty() && !new_smallest.empty()) {
        refined_oldest_ancester_time =
            sub_compact->compaction->MinInputFileOldestAncesterTime(
                &(meta->smallest), &(meta->largest));
        if (refined_oldest_ancester_time != port::kMaxUint64) {
          meta->oldest_ancester_time = refined_oldest_ancester_time;
        }
      }
    }
  }
  level_output->current_output()->finished = true;
  sub_compact->total_bytes += current_bytes;
  if (output_level == sub_compact->start_level_output.level()) {
    sub_compact->retained_or_promoted_bytes += current_bytes;
  }

  // Finish and check for file errors
  if (s.ok()) {
    StopWatch sw(db_options_.clock, stats_, COMPACTION_OUTFILE_SYNC_MICROS);
    io_s = level_output->outfile->Sync(db_options_.use_fsync);
  }
  if (s.ok() && io_s.ok()) {
    io_s = level_output->outfile->Close();
  }
  if (s.ok() && io_s.ok()) {
    // Add the checksum information to file metadata.
    meta->file_checksum = level_output->outfile->GetFileChecksum();
    meta->file_checksum_func_name =
        level_output->outfile->GetFileChecksumFuncName();
    file_checksum = meta->file_checksum;
    file_checksum_func_name = meta->file_checksum_func_name;
  }
  if (s.ok()) {
    s = io_s;
  }
  if (sub_compact->io_status.ok()) {
    sub_compact->io_status = io_s;
    // Since this error is really a copy of the
    // "normal" status, it does not also need to be checked
    sub_compact->io_status.PermitUncheckedError();
  }
  level_output->outfile.reset();

  TableProperties tp;
  if (s.ok()) {
    tp = level_output->builder->GetTableProperties();
  }

  if (s.ok() && current_entries == 0 && tp.num_range_deletions == 0) {
    // If there is nothing to output, no necessary to generate a sst file.
    // This happens when the output level is bottom level, at the same time
    // the sub_compact output nothing.
    std::string fname =
        TableFileName(sub_compact->compaction->immutable_options()->cf_paths,
                      meta->fd.GetNumber(), meta->fd.GetPathId());

    // TODO(AR) it is not clear if there are any larger implications if
    // DeleteFile fails here
    Status ds = env_->DeleteFile(fname);
    if (!ds.ok()) {
      ROCKS_LOG_WARN(
          db_options_.info_log,
          "[%s] [JOB %d] Unable to remove SST file for table #%" PRIu64
          " at bottom level%s",
          cfd->GetName().c_str(), job_id_, output_number,
          meta->marked_for_compaction ? " (need compaction)" : "");
    }

    // Also need to remove the file from outputs, or it will be added to the
    // VersionEdit.
    assert(!level_output->outputs.empty());
    level_output->outputs.pop_back();
    meta = nullptr;
  }

  if (s.ok() && (current_entries > 0 || tp.num_range_deletions > 0)) {
    // Output to event logger and fire events.
    level_output->current_output()->table_properties =
        std::make_shared<TableProperties>(tp);
    ROCKS_LOG_INFO(db_options_.info_log,
                   "[%s] [JOB %d] Generated table #%" PRIu64 " at L%d: %" PRIu64
                   " keys, %" PRIu64 " bytes%s",
                   cfd->GetName().c_str(), job_id_, output_number, output_level,
                   current_entries, current_bytes,
                   meta->marked_for_compaction ? " (need compaction)" : "");
  }
  std::string fname;
  FileDescriptor output_fd;
  uint64_t oldest_blob_file_number = kInvalidBlobFileNumber;
  if (meta != nullptr) {
    fname = level_output->GetTableFileName(
        sub_compact->compaction->immutable_options(), meta->fd.GetNumber());
    output_fd = meta->fd;
    oldest_blob_file_number = meta->oldest_blob_file_number;
  } else {
    fname = "(nil)";
  }
  EventHelpers::LogAndNotifyTableFileCreationFinished(
      event_logger_, cfd->ioptions()->listeners, dbname_, cfd->GetName(), fname,
      job_id_, output_fd, oldest_blob_file_number, tp,
      TableFileCreationReason::kCompaction, s, file_checksum,
      file_checksum_func_name);

#ifndef ROCKSDB_LITE
  // Report new file to SstFileManagerImpl
  auto sfm =
      static_cast<SstFileManagerImpl*>(db_options_.sst_file_manager.get());
  if (sfm && meta != nullptr && meta->fd.GetPathId() == 0) {
    Status add_s = sfm->OnAddFile(fname);
    if (!add_s.ok() && s.ok()) {
      s = add_s;
    }
    if (sfm->IsMaxAllowedSpaceReached()) {
      // TODO(ajkr): should we return OK() if max space was reached by the
      // final compaction output file (similarly to how flush works when
      // full)?
      s = Status::SpaceLimit("Max allowed space was reached");
      TEST_SYNC_POINT(
          "CompactionJob::FinishCompactionOutputFile:"
          "MaxAllowedSpaceReached");
      InstrumentedMutexLock l(db_mutex_);
      db_error_handler_->SetBGError(s, BackgroundErrorReason::kCompaction);
    }
  }
#endif

  level_output->builder.reset();
  level_output->current_output_file_size = 0;
  return s;
}

Status CompactionJob::InstallCompactionResults(
    const MutableCFOptions& mutable_cf_options) {
  assert(compact_);

  db_mutex_->AssertHeld();

  auto* compaction = compact_->compaction;
  assert(compaction);

  {
    Compaction::InputLevelSummaryBuffer inputs_summary;
    ROCKS_LOG_INFO(db_options_.info_log,
                   "[%s] [JOB %d] Compacted %s => %" PRIu64
                   " bytes, "
                   "retained or promoted %" PRIu64 " bytes",
                   compaction->column_family_data()->GetName().c_str(), job_id_,
                   compaction->InputLevelSummary(&inputs_summary),
                   compact_->total_bytes + compact_->total_blob_bytes,
                   compact_->retained_or_promoted_bytes);
  }

  VersionEdit* const edit = compaction->edit();
  assert(edit);

  // Add compaction inputs
  compaction->AddInputDeletions(edit);

  std::unordered_map<uint64_t, BlobGarbageMeter::BlobStats> blob_total_garbage;

  for (const auto& sub_compact : compact_->sub_compact_states) {
    for (const auto& out : sub_compact.start_level_output.outputs) {
      edit->AddFile(compaction->start_level(), out.meta);
    }
    for (const auto& out : sub_compact.latter_level_output.outputs) {
      edit->AddFile(compaction->output_level(), out.meta);
    }

    for (const auto& blob : sub_compact.blob_file_additions) {
      edit->AddBlobFile(blob);
    }

    if (sub_compact.blob_garbage_meter) {
      const auto& flows = sub_compact.blob_garbage_meter->flows();

      for (const auto& pair : flows) {
        const uint64_t blob_file_number = pair.first;
        const BlobGarbageMeter::BlobInOutFlow& flow = pair.second;

        assert(flow.IsValid());
        if (flow.HasGarbage()) {
          blob_total_garbage[blob_file_number].Add(flow.GetGarbageCount(),
                                                   flow.GetGarbageBytes());
        }
      }
    }
  }

  for (const auto& pair : blob_total_garbage) {
    const uint64_t blob_file_number = pair.first;
    const BlobGarbageMeter::BlobStats& stats = pair.second;

    edit->AddBlobFileGarbage(blob_file_number, stats.GetCount(),
                             stats.GetBytes());
  }

  return versions_->LogAndApply(compaction->column_family_data(),
                                mutable_cf_options, edit, db_mutex_,
                                db_directory_);
}

void CompactionJob::RecordCompactionIOStats() {
  RecordTick(stats_, COMPACT_READ_BYTES, IOSTATS(bytes_read));
  RecordTick(stats_, COMPACT_WRITE_BYTES, IOSTATS(bytes_written));
  CompactionReason compaction_reason =
      compact_->compaction->compaction_reason();
  if (compaction_reason == CompactionReason::kFilesMarkedForCompaction) {
    RecordTick(stats_, COMPACT_READ_BYTES_MARKED, IOSTATS(bytes_read));
    RecordTick(stats_, COMPACT_WRITE_BYTES_MARKED, IOSTATS(bytes_written));
  } else if (compaction_reason == CompactionReason::kPeriodicCompaction) {
    RecordTick(stats_, COMPACT_READ_BYTES_PERIODIC, IOSTATS(bytes_read));
    RecordTick(stats_, COMPACT_WRITE_BYTES_PERIODIC, IOSTATS(bytes_written));
  } else if (compaction_reason == CompactionReason::kTtl) {
    RecordTick(stats_, COMPACT_READ_BYTES_TTL, IOSTATS(bytes_read));
    RecordTick(stats_, COMPACT_WRITE_BYTES_TTL, IOSTATS(bytes_written));
  }
  ThreadStatusUtil::IncreaseThreadOperationProperty(
      ThreadStatus::COMPACTION_BYTES_READ, IOSTATS(bytes_read));
  IOSTATS_RESET(bytes_read);
  ThreadStatusUtil::IncreaseThreadOperationProperty(
      ThreadStatus::COMPACTION_BYTES_WRITTEN, IOSTATS(bytes_written));
  IOSTATS_RESET(bytes_written);
}

Status CompactionJob::OpenCompactionOutputFile(
    SubcompactionState* sub_compact,
    SubcompactionState::LevelOutput* level_output) {
  assert(sub_compact != nullptr);
  assert(level_output->builder == nullptr);
  // no need to lock because VersionSet::next_file_number_ is atomic
  uint64_t file_number = versions_->NewFileNumber();
  std::string fname = level_output->GetTableFileName(
      sub_compact->compaction->immutable_options(), file_number);
  // Fire events.
  ColumnFamilyData* cfd = sub_compact->compaction->column_family_data();
#ifndef ROCKSDB_LITE
  EventHelpers::NotifyTableFileCreationStarted(
      cfd->ioptions()->listeners, dbname_, cfd->GetName(), fname, job_id_,
      TableFileCreationReason::kCompaction);
#endif  // !ROCKSDB_LITE
  // Make the output file
  std::unique_ptr<FSWritableFile> writable_file;
#ifndef NDEBUG
  bool syncpoint_arg = file_options_.use_direct_writes;
  TEST_SYNC_POINT_CALLBACK("CompactionJob::OpenCompactionOutputFile",
                           &syncpoint_arg);
#endif

  // Pass temperature of botommost files to FileSystem.
  FileOptions fo_copy = file_options_;
  Temperature temperature = sub_compact->compaction->output_temperature();
  if (temperature == Temperature::kUnknown && bottommost_level_) {
    temperature =
        sub_compact->compaction->mutable_cf_options()->bottommost_temperature;
  }
  fo_copy.temperature = temperature;

  Status s;
  IOStatus io_s = NewWritableFile(fs_.get(), fname, &writable_file, fo_copy);
  s = io_s;
  if (sub_compact->io_status.ok()) {
    sub_compact->io_status = io_s;
    // Since this error is really a copy of the io_s that is checked below as
    // s, it does not also need to be checked.
    sub_compact->io_status.PermitUncheckedError();
  }
  if (!s.ok()) {
    ROCKS_LOG_ERROR(
        db_options_.info_log,
        "[%s] [JOB %d] OpenCompactionOutputFiles for table #%" PRIu64
        " fails at NewWritableFile with status %s",
        sub_compact->compaction->column_family_data()->GetName().c_str(),
        job_id_, file_number, s.ToString().c_str());
    LogFlush(db_options_.info_log);
    EventHelpers::LogAndNotifyTableFileCreationFinished(
        event_logger_, cfd->ioptions()->listeners, dbname_, cfd->GetName(),
        fname, job_id_, FileDescriptor(), kInvalidBlobFileNumber,
        TableProperties(), TableFileCreationReason::kCompaction, s,
        kUnknownFileChecksum, kUnknownFileChecksumFuncName);
    return s;
  }

  // Try to figure out the output file's oldest ancester time.
  int64_t temp_current_time = 0;
  auto get_time_status = db_options_.clock->GetCurrentTime(&temp_current_time);
  // Safe to proceed even if GetCurrentTime fails. So, log and proceed.
  if (!get_time_status.ok()) {
    ROCKS_LOG_WARN(db_options_.info_log,
                   "Failed to get current time. Status: %s",
                   get_time_status.ToString().c_str());
  }
  uint64_t current_time = static_cast<uint64_t>(temp_current_time);
  InternalKey tmp_start, tmp_end;
  if (sub_compact->start != nullptr) {
    tmp_start.SetMinPossibleForUserKey(*(sub_compact->start));
  }
  if (sub_compact->end != nullptr) {
    tmp_end.SetMinPossibleForUserKey(*(sub_compact->end));
  }
  uint64_t oldest_ancester_time =
      sub_compact->compaction->MinInputFileOldestAncesterTime(
          (sub_compact->start != nullptr) ? &tmp_start : nullptr,
          (sub_compact->end != nullptr) ? &tmp_end : nullptr);
  if (oldest_ancester_time == port::kMaxUint64) {
    oldest_ancester_time = current_time;
  }

  // Initialize a SubcompactionState::Output and add it to
  // sub_compact->outputs
  {
    FileMetaData meta;
    meta.fd = FileDescriptor(file_number, level_output->path_id(), 0);
    meta.oldest_ancester_time = oldest_ancester_time;
    meta.file_creation_time = current_time;
    meta.temperature = temperature;
    level_output->outputs.emplace_back(
        std::move(meta), cfd->internal_comparator(),
        /*enable_order_check=*/
        sub_compact->compaction->mutable_cf_options()
            ->check_flush_compaction_key_order,
        /*enable_hash=*/paranoid_file_checks_);
  }

  writable_file->SetIOPriority(Env::IOPriority::IO_LOW);
  writable_file->SetWriteLifeTimeHint(write_hint_);
  FileTypeSet tmp_set = db_options_.checksum_handoff_file_types;
  writable_file->SetPreallocationBlockSize(static_cast<size_t>(
      sub_compact->compaction->OutputFilePreallocationSize()));
  const auto& listeners =
      sub_compact->compaction->immutable_options()->listeners;
  level_output->outfile.reset(new WritableFileWriter(
      std::move(writable_file), fname, file_options_, db_options_.clock,
      io_tracer_, db_options_.stats, listeners,
      db_options_.file_checksum_gen_factory.get(),
      tmp_set.Contains(FileType::kTableFile), false));

  TableBuilderOptions tboptions(
      *cfd->ioptions(), *(sub_compact->compaction->mutable_cf_options()),
      cfd->internal_comparator(), cfd->int_tbl_prop_collector_factories(),
      sub_compact->compaction->output_compression(),
      sub_compact->compaction->output_compression_opts(), cfd->GetID(),
      cfd->GetName(), sub_compact->compaction->output_level(),
      bottommost_level_, TableFileCreationReason::kCompaction,
      oldest_ancester_time, 0 /* oldest_key_time */, current_time, db_id_,
      db_session_id_, sub_compact->compaction->max_output_file_size(),
      file_number);
  level_output->builder.reset(
      NewTableBuilder(tboptions, level_output->outfile.get()));
  LogFlush(db_options_.info_log);
  // if (level_output == &sub_compact->start_level_output) {
  //   ROCKS_LOG_INFO(db_options_.info_log, "Compaction output file of start
  //   level opened: %s\n", fname.c_str());
  // }
  return s;
}

void CompactionJob::CleanupCompaction() {
  for (SubcompactionState& sub_compact : compact_->sub_compact_states) {
    const auto& sub_status = sub_compact.status;

    sub_compact.start_level_output.CleanupCompaction(sub_status, table_cache_);
    sub_compact.latter_level_output.CleanupCompaction(sub_status, table_cache_);
    // TODO: sub_compact.io_status is not checked like status. Not sure if
    // thats intentional. So ignoring the io_status as of now.
    sub_compact.io_status.PermitUncheckedError();
  }
  delete compact_;
  compact_ = nullptr;
}

#ifndef ROCKSDB_LITE
namespace {
void CopyPrefix(const Slice& src, size_t prefix_length, std::string* dst) {
  assert(prefix_length > 0);
  size_t length = src.size() > prefix_length ? prefix_length : src.size();
  dst->assign(src.data(), length);
}
}  // namespace

#endif  // !ROCKSDB_LITE

void CompactionJob::UpdateCompactionStats() {
  assert(compact_);

  Compaction* compaction = compact_->compaction;
  compaction_stats_.num_input_files_in_non_output_levels = 0;
  compaction_stats_.num_input_files_in_output_level = 0;
  for (int input_level = 0;
       input_level < static_cast<int>(compaction->num_input_levels());
       ++input_level) {
    if (compaction->level(input_level) != compaction->output_level()) {
      UpdateCompactionInputStatsHelper(
          &compaction_stats_.num_input_files_in_non_output_levels,
          &compaction_stats_.bytes_read_non_output_levels, input_level);
    } else {
      UpdateCompactionInputStatsHelper(
          &compaction_stats_.num_input_files_in_output_level,
          &compaction_stats_.bytes_read_output_level, input_level);
    }
  }

  assert(compaction_job_stats_);
  compaction_stats_.bytes_read_blob =
      compaction_job_stats_->total_blob_bytes_read;

  compaction_stats_.num_output_files =
      static_cast<int>(compact_->num_output_files);
  compaction_stats_.num_output_files_blob =
      static_cast<int>(compact_->num_blob_output_files);
  compaction_stats_.bytes_written = compact_->total_bytes;
  compaction_stats_.bytes_written_blob = compact_->total_blob_bytes;

  if (compaction_stats_.num_input_records > compact_->num_output_records) {
    compaction_stats_.num_dropped_records =
        compaction_stats_.num_input_records - compact_->num_output_records;
  }
}

void CompactionJob::UpdateCompactionInputStatsHelper(int* num_files,
                                                     uint64_t* bytes_read,
                                                     int input_level) {
  const Compaction* compaction = compact_->compaction;
  auto num_input_files = compaction->num_input_files(input_level);
  *num_files += static_cast<int>(num_input_files);

  for (size_t i = 0; i < num_input_files; ++i) {
    const auto* file_meta = compaction->input(input_level, i);
    *bytes_read += file_meta->fd.GetFileSize();
    compaction_stats_.num_input_records +=
        static_cast<uint64_t>(file_meta->num_entries);
  }
}

void CompactionJob::UpdateCompactionJobStats(
    const InternalStats::CompactionStats& stats) const {
#ifndef ROCKSDB_LITE
  compaction_job_stats_->elapsed_micros = stats.micros;

  // input information
  compaction_job_stats_->total_input_bytes =
      stats.bytes_read_non_output_levels + stats.bytes_read_output_level;
  compaction_job_stats_->num_input_records = stats.num_input_records;
  compaction_job_stats_->num_input_files =
      stats.num_input_files_in_non_output_levels +
      stats.num_input_files_in_output_level;
  compaction_job_stats_->num_input_files_at_output_level =
      stats.num_input_files_in_output_level;

  // output information
  compaction_job_stats_->total_output_bytes = stats.bytes_written;
  compaction_job_stats_->total_output_bytes_blob = stats.bytes_written_blob;
  compaction_job_stats_->num_output_records = compact_->num_output_records;
  compaction_job_stats_->num_output_files = stats.num_output_files;
  compaction_job_stats_->num_output_files_blob = stats.num_output_files_blob;

  if (stats.num_output_files > 0) {
    CopyPrefix(compact_->SmallestUserKey(),
               CompactionJobStats::kMaxPrefixLength,
               &compaction_job_stats_->smallest_output_key_prefix);
    CopyPrefix(compact_->LargestUserKey(), CompactionJobStats::kMaxPrefixLength,
               &compaction_job_stats_->largest_output_key_prefix);
  }
#else
  (void)stats;
#endif  // !ROCKSDB_LITE
}

void CompactionJob::LogCompaction() {
  Compaction* compaction = compact_->compaction;
  ColumnFamilyData* cfd = compaction->column_family_data();

  // Let's check if anything will get logged. Don't prepare all the info if
  // we're not logging
  if (db_options_.info_log_level <= InfoLogLevel::INFO_LEVEL) {
    Compaction::InputLevelSummaryBuffer inputs_summary;
    ROCKS_LOG_INFO(
        db_options_.info_log, "[%s] [JOB %d] Compacting %s, score %.2f",
        cfd->GetName().c_str(), job_id_,
        compaction->InputLevelSummary(&inputs_summary), compaction->score());
    char scratch[2345];
    compaction->Summary(scratch, sizeof(scratch));
    ROCKS_LOG_INFO(db_options_.info_log, "[%s] Compaction start summary: %s\n",
                   cfd->GetName().c_str(), scratch);
    // build event logger report
    auto stream = event_logger_->Log();
    stream << "job" << job_id_ << "event"
           << "compaction_started"
           << "compaction_reason"
           << GetCompactionReasonString(compaction->compaction_reason());
    for (size_t i = 0; i < compaction->num_input_levels(); ++i) {
      stream << ("files_L" + ToString(compaction->level(i)));
      stream.StartArray();
      for (auto f : *compaction->inputs(i)) {
        stream << f->fd.GetNumber();
      }
      stream.EndArray();
    }
    stream << "score" << compaction->score() << "input_data_size"
           << compaction->CalculateTotalInputSize();
  }
}

#ifndef ROCKSDB_LITE
void CompactionServiceCompactionJob::RecordCompactionIOStats() {
  compaction_result_->bytes_read += IOSTATS(bytes_read);
  compaction_result_->bytes_written += IOSTATS(bytes_written);
  CompactionJob::RecordCompactionIOStats();
}

CompactionServiceCompactionJob::CompactionServiceCompactionJob(
    int job_id, Compaction* compaction, const ImmutableDBOptions& db_options,
    const MutableDBOptions& mutable_db_options, const FileOptions& file_options,
    VersionSet* versions, const std::atomic<bool>* shutting_down,
    LogBuffer* log_buffer, FSDirectory* start_level_output_directory,
    FSDirectory* latter_level_output_directory, Statistics* stats,
    InstrumentedMutex* db_mutex, ErrorHandler* db_error_handler,
    std::vector<SequenceNumber> existing_snapshots,
    std::shared_ptr<Cache> table_cache, EventLogger* event_logger,
    const std::string& dbname, const std::shared_ptr<IOTracer>& io_tracer,
    const std::string& db_id, const std::string& db_session_id,
    const std::string& output_path,
    const CompactionServiceInput& compaction_service_input,
    CompactionServiceResult* compaction_service_result)
    : CompactionJob(
          job_id, compaction, db_options, mutable_db_options, file_options,
          versions, shutting_down, 0, log_buffer, nullptr,
          start_level_output_directory, latter_level_output_directory, nullptr,
          stats, db_mutex, db_error_handler, existing_snapshots,
          kMaxSequenceNumber, nullptr, table_cache, event_logger,
          compaction->mutable_cf_options()->paranoid_file_checks,
          compaction->mutable_cf_options()->report_bg_io_stats, dbname,
          &(compaction_service_result->stats), Env::Priority::USER, io_tracer,
          nullptr, nullptr, db_id, db_session_id,
          compaction->column_family_data()->GetFullHistoryTsLow()),
      output_path_(output_path),
      compaction_input_(compaction_service_input),
      compaction_result_(compaction_service_result) {}

Status CompactionServiceCompactionJob::Run() {
#if 0
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_RUN);

  auto* c = compact_->compaction;
  assert(c->column_family_data() != nullptr);
  assert(c->column_family_data()->current()->storage_info()->NumLevelFiles(
             compact_->compaction->level()) > 0);

  write_hint_ =
      c->column_family_data()->CalculateSSTWriteHint(c->output_level());
  bottommost_level_ = c->bottommost_level();

  Slice begin = compaction_input_.begin;
  Slice end = compaction_input_.end;
  compact_->sub_compact_states.emplace_back(
      c, compaction_input_.has_begin ? &begin : nullptr,
      compaction_input_.has_end ? &end : nullptr, compaction_input_.approx_size,
      /*sub_job_id*/ 0);

  log_buffer_->FlushBufferToLog();
  LogCompaction();
  const uint64_t start_micros = db_options_.clock->NowMicros();
  // Pick the only sub-compaction we should have
  assert(compact_->sub_compact_states.size() == 1);
  SubcompactionState* sub_compact = compact_->sub_compact_states.data();

  ProcessKeyValueCompaction(sub_compact);

  compaction_stats_.micros = db_options_.clock->NowMicros() - start_micros;
  compaction_stats_.cpu_micros = sub_compact->compaction_job_stats.cpu_micros;

  RecordTimeToHistogram(stats_, COMPACTION_TIME, compaction_stats_.micros);
  RecordTimeToHistogram(stats_, COMPACTION_CPU_TIME,
                        compaction_stats_.cpu_micros);

  Status status = sub_compact->status;
  IOStatus io_s = sub_compact->io_status;

  if (io_status_.ok()) {
    io_status_ = io_s;
  }

  if (status.ok()) {
    constexpr IODebugContext* dbg = nullptr;

    if (output_directory_) {
      io_s = output_directory_->FsyncWithDirOptions(IOOptions(), dbg,
                                                    DirFsyncOptions());
    }
  }
  if (io_status_.ok()) {
    io_status_ = io_s;
  }
  if (status.ok()) {
    status = io_s;
  }
  if (status.ok()) {
    // TODO: Add verify_table()
  }

  // Finish up all book-keeping to unify the subcompaction results
  AggregateStatistics();
  UpdateCompactionStats();
  RecordCompactionIOStats();

  LogFlush(db_options_.info_log);
  compact_->status = status;
  compact_->status.PermitUncheckedError();

  // Build compaction result
  compaction_result_->output_level = compact_->compaction->output_level();
  compaction_result_->output_path = output_path_;
  for (const auto& output_file : sub_compact->outputs) {
    auto& meta = output_file.meta;
    compaction_result_->output_files.emplace_back(
        MakeTableFileName(meta.fd.GetNumber()), meta.fd.smallest_seqno,
        meta.fd.largest_seqno, meta.smallest.Encode().ToString(),
        meta.largest.Encode().ToString(), meta.oldest_ancester_time,
        meta.file_creation_time, output_file.validator.GetHash(),
        meta.marked_for_compaction);
  }
  compaction_result_->num_output_records = sub_compact->num_output_records;
  compaction_result_->total_bytes = sub_compact->total_bytes;

  return status;
#endif
  return Status::NotSupported();  // TODO
}

void CompactionServiceCompactionJob::CleanupCompaction() {
  CompactionJob::CleanupCompaction();
}

// Internal binary format for the input and result data
enum BinaryFormatVersion : uint32_t {
  kOptionsString = 1,  // Use string format similar to Option string format
};

// offset_of is used to get the offset of a class data member
// ex: offset_of(&ColumnFamilyDescriptor::options)
// This call will return the offset of options in ColumnFamilyDescriptor class
//
// This is the same as offsetof() but allow us to work with non standard-layout
// classes and structures
// refs:
// http://en.cppreference.com/w/cpp/concept/StandardLayoutType
// https://gist.github.com/graphitemaster/494f21190bb2c63c5516
static ColumnFamilyDescriptor dummy_cfd("", ColumnFamilyOptions());
template <typename T1>
int offset_of(T1 ColumnFamilyDescriptor::*member) {
  return int(size_t(&(dummy_cfd.*member)) - size_t(&dummy_cfd));
}

static CompactionServiceInput dummy_cs_input;
template <typename T1>
int offset_of(T1 CompactionServiceInput::*member) {
  return int(size_t(&(dummy_cs_input.*member)) - size_t(&dummy_cs_input));
}

static std::unordered_map<std::string, OptionTypeInfo> cfd_type_info = {
    {"name",
     {offset_of(&ColumnFamilyDescriptor::name), OptionType::kEncodedString,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"options",
     {offset_of(&ColumnFamilyDescriptor::options), OptionType::kConfigurable,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone,
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const std::string& value, void* addr) {
        auto cf_options = static_cast<ColumnFamilyOptions*>(addr);
        return GetColumnFamilyOptionsFromString(opts, ColumnFamilyOptions(),
                                                value, cf_options);
      },
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const void* addr, std::string* value) {
        const auto cf_options = static_cast<const ColumnFamilyOptions*>(addr);
        std::string result;
        auto status =
            GetStringFromColumnFamilyOptions(opts, *cf_options, &result);
        *value = "{" + result + "}";
        return status;
      },
      [](const ConfigOptions& opts, const std::string& name, const void* addr1,
         const void* addr2, std::string* mismatch) {
        const auto this_one = static_cast<const ColumnFamilyOptions*>(addr1);
        const auto that_one = static_cast<const ColumnFamilyOptions*>(addr2);
        auto this_conf = CFOptionsAsConfigurable(*this_one);
        auto that_conf = CFOptionsAsConfigurable(*that_one);
        std::string mismatch_opt;
        bool result =
            this_conf->AreEquivalent(opts, that_conf.get(), &mismatch_opt);
        if (!result) {
          *mismatch = name + "." + mismatch_opt;
        }
        return result;
      }}},
};

static std::unordered_map<std::string, OptionTypeInfo> cs_input_type_info = {
    {"column_family",
     OptionTypeInfo::Struct("column_family", &cfd_type_info,
                            offset_of(&CompactionServiceInput::column_family),
                            OptionVerificationType::kNormal,
                            OptionTypeFlags::kNone)},
    {"db_options",
     {offset_of(&CompactionServiceInput::db_options), OptionType::kConfigurable,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone,
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const std::string& value, void* addr) {
        auto options = static_cast<DBOptions*>(addr);
        return GetDBOptionsFromString(opts, DBOptions(), value, options);
      },
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const void* addr, std::string* value) {
        const auto options = static_cast<const DBOptions*>(addr);
        std::string result;
        auto status = GetStringFromDBOptions(opts, *options, &result);
        *value = "{" + result + "}";
        return status;
      },
      [](const ConfigOptions& opts, const std::string& name, const void* addr1,
         const void* addr2, std::string* mismatch) {
        const auto this_one = static_cast<const DBOptions*>(addr1);
        const auto that_one = static_cast<const DBOptions*>(addr2);
        auto this_conf = DBOptionsAsConfigurable(*this_one);
        auto that_conf = DBOptionsAsConfigurable(*that_one);
        std::string mismatch_opt;
        bool result =
            this_conf->AreEquivalent(opts, that_conf.get(), &mismatch_opt);
        if (!result) {
          *mismatch = name + "." + mismatch_opt;
        }
        return result;
      }}},
    {"snapshots", OptionTypeInfo::Vector<uint64_t>(
                      offset_of(&CompactionServiceInput::snapshots),
                      OptionVerificationType::kNormal, OptionTypeFlags::kNone,
                      {0, OptionType::kUInt64T})},
    {"input_files", OptionTypeInfo::Vector<std::string>(
                        offset_of(&CompactionServiceInput::input_files),
                        OptionVerificationType::kNormal, OptionTypeFlags::kNone,
                        {0, OptionType::kEncodedString})},
    {"output_level",
     {offset_of(&CompactionServiceInput::output_level), OptionType::kInt,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"has_begin",
     {offset_of(&CompactionServiceInput::has_begin), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"begin",
     {offset_of(&CompactionServiceInput::begin), OptionType::kEncodedString,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"has_end",
     {offset_of(&CompactionServiceInput::has_end), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"end",
     {offset_of(&CompactionServiceInput::end), OptionType::kEncodedString,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"approx_size",
     {offset_of(&CompactionServiceInput::approx_size), OptionType::kUInt64T,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
};

static std::unordered_map<std::string, OptionTypeInfo>
    cs_output_file_type_info = {
        {"file_name",
         {offsetof(struct CompactionServiceOutputFile, file_name),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"smallest_seqno",
         {offsetof(struct CompactionServiceOutputFile, smallest_seqno),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"largest_seqno",
         {offsetof(struct CompactionServiceOutputFile, largest_seqno),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"smallest_internal_key",
         {offsetof(struct CompactionServiceOutputFile, smallest_internal_key),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"largest_internal_key",
         {offsetof(struct CompactionServiceOutputFile, largest_internal_key),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"oldest_ancester_time",
         {offsetof(struct CompactionServiceOutputFile, oldest_ancester_time),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"file_creation_time",
         {offsetof(struct CompactionServiceOutputFile, file_creation_time),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"paranoid_hash",
         {offsetof(struct CompactionServiceOutputFile, paranoid_hash),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"marked_for_compaction",
         {offsetof(struct CompactionServiceOutputFile, marked_for_compaction),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
};

static std::unordered_map<std::string, OptionTypeInfo>
    compaction_job_stats_type_info = {
        {"elapsed_micros",
         {offsetof(struct CompactionJobStats, elapsed_micros),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"cpu_micros",
         {offsetof(struct CompactionJobStats, cpu_micros), OptionType::kUInt64T,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
        {"num_input_records",
         {offsetof(struct CompactionJobStats, num_input_records),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_blobs_read",
         {offsetof(struct CompactionJobStats, num_blobs_read),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_input_files",
         {offsetof(struct CompactionJobStats, num_input_files),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_input_files_at_output_level",
         {offsetof(struct CompactionJobStats, num_input_files_at_output_level),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_output_records",
         {offsetof(struct CompactionJobStats, num_output_records),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_output_files",
         {offsetof(struct CompactionJobStats, num_output_files),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_output_files_blob",
         {offsetof(struct CompactionJobStats, num_output_files_blob),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"is_full_compaction",
         {offsetof(struct CompactionJobStats, is_full_compaction),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"is_manual_compaction",
         {offsetof(struct CompactionJobStats, is_manual_compaction),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"total_input_bytes",
         {offsetof(struct CompactionJobStats, total_input_bytes),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"total_blob_bytes_read",
         {offsetof(struct CompactionJobStats, total_blob_bytes_read),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"total_output_bytes",
         {offsetof(struct CompactionJobStats, total_output_bytes),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"total_output_bytes_blob",
         {offsetof(struct CompactionJobStats, total_output_bytes_blob),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_records_replaced",
         {offsetof(struct CompactionJobStats, num_records_replaced),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"total_input_raw_key_bytes",
         {offsetof(struct CompactionJobStats, total_input_raw_key_bytes),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"total_input_raw_value_bytes",
         {offsetof(struct CompactionJobStats, total_input_raw_value_bytes),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_input_deletion_records",
         {offsetof(struct CompactionJobStats, num_input_deletion_records),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_expired_deletion_records",
         {offsetof(struct CompactionJobStats, num_expired_deletion_records),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_corrupt_keys",
         {offsetof(struct CompactionJobStats, num_corrupt_keys),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"file_write_nanos",
         {offsetof(struct CompactionJobStats, file_write_nanos),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"file_range_sync_nanos",
         {offsetof(struct CompactionJobStats, file_range_sync_nanos),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"file_fsync_nanos",
         {offsetof(struct CompactionJobStats, file_fsync_nanos),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"file_prepare_write_nanos",
         {offsetof(struct CompactionJobStats, file_prepare_write_nanos),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"smallest_output_key_prefix",
         {offsetof(struct CompactionJobStats, smallest_output_key_prefix),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"largest_output_key_prefix",
         {offsetof(struct CompactionJobStats, largest_output_key_prefix),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_single_del_fallthru",
         {offsetof(struct CompactionJobStats, num_single_del_fallthru),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_single_del_mismatch",
         {offsetof(struct CompactionJobStats, num_single_del_mismatch),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
};

namespace {
// this is a helper struct to serialize and deserialize class Status, because
// Status's members are not public.
struct StatusSerializationAdapter {
  uint8_t code;
  uint8_t subcode;
  uint8_t severity;
  std::string message;

  StatusSerializationAdapter() {}
  explicit StatusSerializationAdapter(const Status& s) {
    code = s.code();
    subcode = s.subcode();
    severity = s.severity();
    auto msg = s.getState();
    message = msg ? msg : "";
  }

  Status GetStatus() {
    return Status(static_cast<Status::Code>(code),
                  static_cast<Status::SubCode>(subcode),
                  static_cast<Status::Severity>(severity), message);
  }
};
}  // namespace

static std::unordered_map<std::string, OptionTypeInfo>
    status_adapter_type_info = {
        {"code",
         {offsetof(struct StatusSerializationAdapter, code),
          OptionType::kUInt8T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"subcode",
         {offsetof(struct StatusSerializationAdapter, subcode),
          OptionType::kUInt8T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"severity",
         {offsetof(struct StatusSerializationAdapter, severity),
          OptionType::kUInt8T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"message",
         {offsetof(struct StatusSerializationAdapter, message),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
};

static std::unordered_map<std::string, OptionTypeInfo> cs_result_type_info = {
    {"status",
     {offsetof(struct CompactionServiceResult, status),
      OptionType::kCustomizable, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone,
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const std::string& value, void* addr) {
        auto status_obj = static_cast<Status*>(addr);
        StatusSerializationAdapter adapter;
        Status s = OptionTypeInfo::ParseType(
            opts, value, status_adapter_type_info, &adapter);
        *status_obj = adapter.GetStatus();
        return s;
      },
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const void* addr, std::string* value) {
        const auto status_obj = static_cast<const Status*>(addr);
        StatusSerializationAdapter adapter(*status_obj);
        std::string result;
        Status s = OptionTypeInfo::SerializeType(opts, status_adapter_type_info,
                                                 &adapter, &result);
        *value = "{" + result + "}";
        return s;
      },
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const void* addr1, const void* addr2, std::string* mismatch) {
        const auto status1 = static_cast<const Status*>(addr1);
        const auto status2 = static_cast<const Status*>(addr2);
        StatusSerializationAdapter adatper1(*status1);
        StatusSerializationAdapter adapter2(*status2);
        return OptionTypeInfo::TypesAreEqual(opts, status_adapter_type_info,
                                             &adatper1, &adapter2, mismatch);
      }}},
    {"output_files",
     OptionTypeInfo::Vector<CompactionServiceOutputFile>(
         offsetof(struct CompactionServiceResult, output_files),
         OptionVerificationType::kNormal, OptionTypeFlags::kNone,
         OptionTypeInfo::Struct("output_files", &cs_output_file_type_info, 0,
                                OptionVerificationType::kNormal,
                                OptionTypeFlags::kNone))},
    {"output_level",
     {offsetof(struct CompactionServiceResult, output_level), OptionType::kInt,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"output_path",
     {offsetof(struct CompactionServiceResult, output_path),
      OptionType::kEncodedString, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"num_output_records",
     {offsetof(struct CompactionServiceResult, num_output_records),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"total_bytes",
     {offsetof(struct CompactionServiceResult, total_bytes),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"bytes_read",
     {offsetof(struct CompactionServiceResult, bytes_read),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"bytes_written",
     {offsetof(struct CompactionServiceResult, bytes_written),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"stats", OptionTypeInfo::Struct(
                  "stats", &compaction_job_stats_type_info,
                  offsetof(struct CompactionServiceResult, stats),
                  OptionVerificationType::kNormal, OptionTypeFlags::kNone)},
};

Status CompactionServiceInput::Read(const std::string& data_str,
                                    CompactionServiceInput* obj) {
  if (data_str.size() <= sizeof(BinaryFormatVersion)) {
    return Status::InvalidArgument("Invalid CompactionServiceInput string");
  }
  auto format_version = DecodeFixed32(data_str.data());
  if (format_version == kOptionsString) {
    ConfigOptions cf;
    cf.invoke_prepare_options = false;
    cf.ignore_unknown_options = true;
    return OptionTypeInfo::ParseType(
        cf, data_str.substr(sizeof(BinaryFormatVersion)), cs_input_type_info,
        obj);
  } else {
    return Status::NotSupported(
        "Compaction Service Input data version not supported: " +
        ToString(format_version));
  }
}

Status CompactionServiceInput::Write(std::string* output) {
  char buf[sizeof(BinaryFormatVersion)];
  EncodeFixed32(buf, kOptionsString);
  output->append(buf, sizeof(BinaryFormatVersion));
  ConfigOptions cf;
  cf.invoke_prepare_options = false;
  return OptionTypeInfo::SerializeType(cf, cs_input_type_info, this, output);
}

Status CompactionServiceResult::Read(const std::string& data_str,
                                     CompactionServiceResult* obj) {
  if (data_str.size() <= sizeof(BinaryFormatVersion)) {
    return Status::InvalidArgument("Invalid CompactionServiceResult string");
  }
  auto format_version = DecodeFixed32(data_str.data());
  if (format_version == kOptionsString) {
    ConfigOptions cf;
    cf.invoke_prepare_options = false;
    cf.ignore_unknown_options = true;
    return OptionTypeInfo::ParseType(
        cf, data_str.substr(sizeof(BinaryFormatVersion)), cs_result_type_info,
        obj);
  } else {
    return Status::NotSupported(
        "Compaction Service Result data version not supported: " +
        ToString(format_version));
  }
}

Status CompactionServiceResult::Write(std::string* output) {
  char buf[sizeof(BinaryFormatVersion)];
  EncodeFixed32(buf, kOptionsString);
  output->append(buf, sizeof(BinaryFormatVersion));
  ConfigOptions cf;
  cf.invoke_prepare_options = false;
  return OptionTypeInfo::SerializeType(cf, cs_result_type_info, this, output);
}

#ifndef NDEBUG
bool CompactionServiceResult::TEST_Equals(CompactionServiceResult* other) {
  std::string mismatch;
  return TEST_Equals(other, &mismatch);
}

bool CompactionServiceResult::TEST_Equals(CompactionServiceResult* other,
                                          std::string* mismatch) {
  ConfigOptions cf;
  cf.invoke_prepare_options = false;
  return OptionTypeInfo::TypesAreEqual(cf, cs_result_type_info, this, other,
                                       mismatch);
}

bool CompactionServiceInput::TEST_Equals(CompactionServiceInput* other) {
  std::string mismatch;
  return TEST_Equals(other, &mismatch);
}

bool CompactionServiceInput::TEST_Equals(CompactionServiceInput* other,
                                         std::string* mismatch) {
  ConfigOptions cf;
  cf.invoke_prepare_options = false;
  return OptionTypeInfo::TypesAreEqual(cf, cs_input_type_info, this, other,
                                       mismatch);
}
#endif  // NDEBUG
#endif  // !ROCKSDB_LITE

}  // namespace ROCKSDB_NAMESPACE
