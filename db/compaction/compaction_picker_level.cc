//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <string>
#include <utility>
#include <vector>

#include "db/compaction/compaction_picker_level.h"
#include "logging/log_buffer.h"
#include "test_util/sync_point.h"

namespace rocksdb {

bool LevelCompactionPicker::NeedsCompaction(
    const VersionStorageInfo* vstorage) const {
  if (!vstorage->ExpiredTtlFiles().empty()) {
    return true;
  }
  if (!vstorage->FilesMarkedForPeriodicCompaction().empty()) {
    return true;
  }
  if (!vstorage->BottommostFilesMarkedForCompaction().empty()) {
    return true;
  }
  if (!vstorage->FilesMarkedForCompaction().empty()) {
    return true;
  }
  for (int i = 0; i <= vstorage->MaxInputLevel(); i++) {
    if (vstorage->is_nvmcf && vstorage->CompactionScoreLevel(i) == 0) continue;
    if (vstorage->CompactionScore(i) >= 1) {
      return true;
    }
  }
  return false;
}

namespace {
// A class to build a leveled compaction step-by-step.
class LevelCompactionBuilder {
 public:
  LevelCompactionBuilder(const std::string& cf_name,
                         VersionStorageInfo* vstorage,
                         CompactionPicker* compaction_picker,
                         LogBuffer* log_buffer,
                         const MutableCFOptions& mutable_cf_options,
                         const ImmutableCFOptions& ioptions)
      : cf_name_(cf_name),
        vstorage_(vstorage),
        compaction_picker_(compaction_picker),
        log_buffer_(log_buffer),
        mutable_cf_options_(mutable_cf_options),
        ioptions_(ioptions),
        ccitem(nullptr){}

  // Pick and return a compaction.
  Compaction* PickCompaction(bool for_column_compaction = false, NvmCfModule* nvmcf = nullptr);

  // Pick the initial files to compact to the next level. (or together
  // in Intra-L0 compactions)
  void SetupInitialFiles();

  // If the initial files are from L0 level, pick other L0
  // files if needed.
  bool SetupOtherL0FilesIfNeeded();

///
  bool SetupColumnCompactionInputs(NvmCfModule* nvmcf);

///

  // Based on initial files, setup other files need to be compacted
  // in this compaction, accordingly.
  bool SetupOtherInputsIfNeeded();

  Compaction* GetCompaction();

  // For the specfied level, pick a file that we want to compact.
  // Returns false if there is no file to compact.
  // If it returns true, inputs->files.size() will be exactly one.
  // If level is 0 and there is already a compaction on that level, this
  // function will return false.
  bool PickFileToCompact();

  // For L0->L0, picks the longest span of files that aren't currently
  // undergoing compaction for which work-per-deleted-file decreases. The span
  // always starts from the newest L0 file.
  //
  // Intra-L0 compaction is independent of all other files, so it can be
  // performed even when L0->base_level compactions are blocked.
  //
  // Returns true if `inputs` is populated with a span of files to be compacted;
  // otherwise, returns false.
  bool PickIntraL0Compaction();

  void PickExpiredTtlFiles();

  void PickFilesMarkedForPeriodicCompaction();

  const std::string& cf_name_;
  VersionStorageInfo* vstorage_;
  CompactionPicker* compaction_picker_;
  LogBuffer* log_buffer_;
  int start_level_ = -1;
  int output_level_ = -1;
  int parent_index_ = -1;
  int base_index_ = -1;
  double start_level_score_ = 0;
  bool is_manual_ = false;
  CompactionInputFiles start_level_inputs_;
  std::vector<CompactionInputFiles> compaction_inputs_;       // CompactionInputFiles: 管理相同 level 中需要 compaction 的 files 信息
  CompactionInputFiles output_level_inputs_;
  std::vector<FileMetaData*> grandparents_;
  CompactionReason compaction_reason_ = CompactionReason::kUnknown;

  const MutableCFOptions& mutable_cf_options_;
  const ImmutableCFOptions& ioptions_;
  // Pick a path ID to place a newly generated file, with its level
  static uint32_t GetPathId(const ImmutableCFOptions& ioptions,
                            const MutableCFOptions& mutable_cf_options,
                            int level);

  static const int kMinFilesForIntraL0Compaction = 4;
///
  ColumnCompactionItem* ccitem;
///
};

void LevelCompactionBuilder::PickExpiredTtlFiles() {
  if (vstorage_->ExpiredTtlFiles().empty()) {
    return;
  }

  auto continuation = [&](std::pair<int, FileMetaData*> level_file) {
    // If it's being compacted it has nothing to do here.
    // If this assert() fails that means that some function marked some
    // files as being_compacted, but didn't call ComputeCompactionScore()
    assert(!level_file.second->being_compacted);
    start_level_ = level_file.first;
    output_level_ =
        (start_level_ == 0) ? vstorage_->base_level() : start_level_ + 1;

    if ((start_level_ == vstorage_->num_non_empty_levels() - 1) ||
        (start_level_ == 0 &&
         !compaction_picker_->level0_compactions_in_progress()->empty())) {
      return false;
    }

    start_level_inputs_.files = {level_file.second};
    start_level_inputs_.level = start_level_;
    return compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                      &start_level_inputs_);
  };

  for (auto& level_file : vstorage_->ExpiredTtlFiles()) {
    if (continuation(level_file)) {
      // found the compaction!
      return;
    }
  }

  start_level_inputs_.files.clear();
}

void LevelCompactionBuilder::PickFilesMarkedForPeriodicCompaction() {
  if (vstorage_->FilesMarkedForPeriodicCompaction().empty()) {
    return;
  }

  auto continuation = [&](std::pair<int, FileMetaData*> level_file) {
    // If it's being compacted it has nothing to do here.
    // If this assert() fails that means that some function marked some
    // files as being_compacted, but didn't call ComputeCompactionScore()
    assert(!level_file.second->being_compacted);
    output_level_ = start_level_ = level_file.first;

    if (start_level_ == 0 &&
        !compaction_picker_->level0_compactions_in_progress()->empty()) {
      return false;
    }

    start_level_inputs_.files = {level_file.second};
    start_level_inputs_.level = start_level_;
    return compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                      &start_level_inputs_);
  };

  for (auto& level_file : vstorage_->FilesMarkedForPeriodicCompaction()) {
    if (continuation(level_file)) {
      // found the compaction!
      return;
    }
  }

  start_level_inputs_.files.clear();
}

void LevelCompactionBuilder::SetupInitialFiles() {
  // Find the compactions by size on all levels.
  bool skipped_l0_to_base = false;
  for (int i = 0; i < compaction_picker_->NumberLevels() - 1; i++) {
    start_level_score_ = vstorage_->CompactionScore(i);
    start_level_ = vstorage_->CompactionScoreLevel(i);
    /// for nvm level-0
    if(vstorage_->is_nvmcf && start_level_ == 0) continue;
    ///
    assert(i == 0 || start_level_score_ <= vstorage_->CompactionScore(i - 1));
    if (start_level_score_ >= 1) {
      if (skipped_l0_to_base && start_level_ == vstorage_->base_level()) {
        // If L0->base_level compaction is pending, don't schedule further
        // compaction from base level. Otherwise L0->base_level compaction
        // may starve.
        continue;
      }
      output_level_ =
          (start_level_ == 0) ? vstorage_->base_level() : start_level_ + 1;
      if (PickFileToCompact()) {
        // found the compaction!
        if (start_level_ == 0) {
          // L0 score = `num L0 files` / `level0_file_num_compaction_trigger`
          compaction_reason_ = CompactionReason::kLevelL0FilesNum;
        } else {
          // L1+ score = `Level files size` / `MaxBytesForLevel`
          compaction_reason_ = CompactionReason::kLevelMaxLevelSize;
        }
        break;
      } else {
        // didn't find the compaction, clear the inputs
        start_level_inputs_.clear();
        if (start_level_ == 0) {
          skipped_l0_to_base = true;
          // L0->base_level may be blocked due to ongoing L0->base_level
          // compactions. It may also be blocked by an ongoing compaction from
          // base_level downwards.
          //
          // In these cases, to reduce L0 file count and thus reduce likelihood
          // of write stalls, we can attempt compacting a span of files within
          // L0.
          if (PickIntraL0Compaction()) {
            output_level_ = 0;
            compaction_reason_ = CompactionReason::kLevelL0FilesNum;
            break;
          }
        }
      }
    }
  }

  // if we didn't find a compaction, check if there are any files marked for
  // compaction
  if (start_level_inputs_.empty()) {
    parent_index_ = base_index_ = -1;

    compaction_picker_->PickFilesMarkedForCompaction(
        cf_name_, vstorage_, &start_level_, &output_level_,
        &start_level_inputs_);
    if (!start_level_inputs_.empty()) {
      is_manual_ = true;
      compaction_reason_ = CompactionReason::kFilesMarkedForCompaction;
      return;
    }
  }

  // Bottommost Files Compaction on deleting tombstones
  if (start_level_inputs_.empty()) {
    size_t i;
    for (i = 0; i < vstorage_->BottommostFilesMarkedForCompaction().size();
         ++i) {
      auto& level_and_file = vstorage_->BottommostFilesMarkedForCompaction()[i];
      assert(!level_and_file.second->being_compacted);
      start_level_inputs_.level = output_level_ = start_level_ =
          level_and_file.first;
      start_level_inputs_.files = {level_and_file.second};
      if (compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                     &start_level_inputs_)) {
        break;
      }
    }
    if (i == vstorage_->BottommostFilesMarkedForCompaction().size()) {
      start_level_inputs_.clear();
    } else {
      assert(!start_level_inputs_.empty());
      compaction_reason_ = CompactionReason::kBottommostFiles;
      return;
    }
  }

  // TTL Compaction
  if (start_level_inputs_.empty()) {
    PickExpiredTtlFiles();
    if (!start_level_inputs_.empty()) {
      compaction_reason_ = CompactionReason::kTtl;
      return;
    }
  }

  // Periodic Compaction
  if (start_level_inputs_.empty()) {
    PickFilesMarkedForPeriodicCompaction();
    if (!start_level_inputs_.empty()) {
      compaction_reason_ = CompactionReason::kPeriodicCompaction;
      return;
    }
  }
}

bool LevelCompactionBuilder::SetupOtherL0FilesIfNeeded() {
  if (start_level_ == 0 && output_level_ != 0) {
    return compaction_picker_->GetOverlappingL0Files(
        vstorage_, &start_level_inputs_, output_level_, &parent_index_);
  }
  return true;
}

bool LevelCompactionBuilder::SetupOtherInputsIfNeeded() {
  // Setup input files from output level. For output to L0, we only compact
  // spans of files that do not interact with any pending compactions, so don't
  // need to consider other levels.
  if (output_level_ != 0) {
    output_level_inputs_.level = output_level_;
    if (!compaction_picker_->SetupOtherInputs(
            cf_name_, mutable_cf_options_, vstorage_, &start_level_inputs_,
            &output_level_inputs_, &parent_index_, base_index_)) {
      return false;
    }

    compaction_inputs_.push_back(start_level_inputs_);
    if (!output_level_inputs_.empty()) {
      compaction_inputs_.push_back(output_level_inputs_);
    }

    // In some edge cases we could pick a compaction that will be compacting
    // a key range that overlap with another running compaction, and both
    // of them have the same output level. This could happen if
    // (1) we are running a non-exclusive manual compaction
    // (2) AddFile ingest a new file into the LSM tree
    // We need to disallow this from happening.
    if (compaction_picker_->FilesRangeOverlapWithCompaction(compaction_inputs_,
                                                            output_level_)) {
      // This compaction output could potentially conflict with the output
      // of a currently running compaction, we cannot run it.
      return false;
    }
    compaction_picker_->GetGrandparents(vstorage_, start_level_inputs_,
                                        output_level_inputs_, &grandparents_);
  } else {
    compaction_inputs_.push_back(start_level_inputs_);
  }
  return true;
}

// 1. 生成 ColumnCompactionItem (保存需要 column compaction 的 files, key, size 信息)
// 2. 根据 ccitem 信息，更新 compaction_inputs_ 以及 compaction 后的 key range
// 3. 根据 key range 获取 output_level_ 中的 FileMetaData 信息
//     (1) output_level_ = vstorage_->base_level(): Level that L0 data should be compacted to
bool LevelCompactionBuilder::SetupColumnCompactionInputs(NvmCfModule* nvmcf) {
    start_level_ = 0;
    start_level_inputs_.level = 0;
    output_level_ = vstorage_->base_level();
    output_level_inputs_.level = output_level_;
    compaction_reason_ = CompactionReason::kLevelMaxLevelSize;
#ifdef STATISTIC_OPEN
    RECORD_LOG("%ld nvm cf pick column compaction\n",global_stats.compaction_num + 1);
    uint64_t start_time = get_now_micros();
#else
    RECORD_LOG("nvm cf pick column compaction\n");
#endif
    // 根据 vstorage_ L0files, 获取需要 compaction 的 信息: ccitem
    ccitem = nvmcf->PickColumnCompaction(vstorage_);
    if(ccitem == nullptr) {
      return false;
    }

#ifdef STATISTIC_OPEN
    uint64_t end_time = get_now_micros();
    global_stats.pick_compaction_time += (end_time - start_time);
#endif
    start_level_score_ = nvmcf->GetCompactionScore();
    RECORD_LOG("[Info] Column Compaction: L0 select num:%lu L0 select size:%.2f MB\n", ccitem->files.size(), 1.0 * ccitem->L0select_size/1048576);
    RECORD_LOG("[Info] Column Compaction: L0smallest:%s L0largest:%s\n", ccitem->L0smallest.DebugString(true).c_str(), ccitem->L0largest.DebugString(true).c_str());
    // FileEntry 信息
    for(unsigned int i = 0; i < ccitem->files.size(); i++) {
      RECORD_LOG("select L0:%lu keynum:%lu size:%.2f MB\n", ccitem->files.at(i)->filenum, ccitem->keys_num.at(i), 1.0*ccitem->keys_size.at(i)/1048576.0);
    }
    
    // compaction 的 L0 FileMetaData 信息
    for(unsigned int i = 0; i < ccitem->L0compactionfiles.size(); i++) {
      start_level_inputs_.files.push_back(ccitem->L0compactionfiles.at(i));
    }
    
    // 可能存在与 L1 有 overlap 的 L0 files
    for(unsigned int i = 0; i < ccitem->L1compactionfiles.size(); i++) {
      RECORD_LOG("select L1:%lu [%s-%s]\n",ccitem->L1compactionfiles.at(i)->fd.GetNumber(),ccitem->L1compactionfiles.at(i)->smallest.DebugString(true).c_str(),ccitem->L1compactionfiles.at(i)->largest.DebugString(true).c_str());
      output_level_inputs_.files.push_back(ccitem->L1compactionfiles.at(i));
    }
    for(unsigned int i = 0;i < ccitem->L1compactionfiles.size();i++) {
      if(ccitem->L1compactionfiles.at(i)->being_compacted) {  //挑选的L1 files 有正在compacted，失败
          start_level_inputs_.clear();
          output_level_inputs_.clear();
          RECORD_LOG("[Warn] select L1:%ld being compacted!\n",ccitem->L1compactionfiles.at(i)->fd.GetNumber());
          delete ccitem;
          return false;
      }
    }
    // 添加需要 compaction 的 L0 和 L1 信息
    compaction_inputs_.push_back(start_level_inputs_);
    if (!output_level_inputs_.empty()) {
      compaction_inputs_.push_back(output_level_inputs_);
    }

    // 获取 L0 compaction 后的 key range
    InternalKey start;
    InternalKey limit;
    if(!output_level_inputs_.empty()) {
      InternalKey start1, limit1, start2, limit2;
      start1 = ccitem->L0smallest;
      limit1 = ccitem->L0largest;
      compaction_picker_->GetRange(output_level_inputs_, &start2, &limit2);
      compaction_picker_->GetRange(&start, &limit, &start1, &limit1, &start2, &limit2);
    }
    else {
      start = ccitem->L0smallest;
      limit = ccitem->L0largest;
    }
    // vstorage 的 output_level_inputs_.level 层数据中，与 start, end 有交集的数据 => grandparents_
    vstorage_->GetOverlappingInputs(output_level_inputs_.level + 1, &start,
                                   &limit, &grandparents_);
    return true;
}
///

// 1. 获取 Column Compaction 之前的必要信息 (ColumnCompactionItem, start_level_inputs_, output_level_inputs_, 输入输出)
// 2. 返回 Compaction 实例 (compaction.cc)
Compaction* LevelCompactionBuilder::PickCompaction(bool for_column_compaction, NvmCfModule* nvmcf) {
  // Pick up the first file to start compaction. It may have been extended
  // to a clean cut.
  if(for_column_compaction) {
    RECORD_LOG("[Info] Column Compactoin: pick column compaction\n");
    if(!SetupColumnCompactionInputs(nvmcf)) {  //选择 column Compaction 失败
      RECORD_LOG("[Warn] pick column compaction failed!\n");
      return nullptr;
    }
  }
  else {
    // 不是 nvmcf, 或者不满足 L0 column compaction 条件
    SetupInitialFiles();
    if (start_level_inputs_.empty()) {
      return nullptr;
    }
    assert(start_level_ >= 0 && output_level_ >= 0);

    // If it is a L0 -> base level compaction, we need to set up other L0
    // files if needed.
    if (!SetupOtherL0FilesIfNeeded()) {
      return nullptr;
    }

    // Pick files in the output level and expand more files in the start level
    // if needed.
    if (!SetupOtherInputsIfNeeded()) {
      return nullptr;
    }
  }

  // Form a compaction object containing the files we picked.
  Compaction* c = GetCompaction();

  TEST_SYNC_POINT_CALLBACK("LevelCompactionPicker::PickCompaction:Return", c);

  return c;
}

// compactoin.cc
Compaction* LevelCompactionBuilder::GetCompaction() {
  auto c = new Compaction(
      vstorage_, ioptions_, mutable_cf_options_, std::move(compaction_inputs_),
      output_level_,
      MaxFileSizeForLevel(mutable_cf_options_, output_level_,
                          ioptions_.compaction_style, vstorage_->base_level(),
                          ioptions_.level_compaction_dynamic_level_bytes),
      mutable_cf_options_.max_compaction_bytes,
      GetPathId(ioptions_, mutable_cf_options_, output_level_),
      GetCompressionType(ioptions_, vstorage_, mutable_cf_options_,
                         output_level_, vstorage_->base_level()),
      GetCompressionOptions(ioptions_, vstorage_, output_level_),
      /* max_subcompactions */ 0, std::move(grandparents_), is_manual_,
      start_level_score_, false /* deletion_compaction */, compaction_reason_, ccitem);

  // If it's level 0 compaction, make sure we don't execute any other level 0
  // compactions in parallel
  compaction_picker_->RegisterCompaction(c);

  // Creating a compaction influences the compaction score because the score
  // takes running compactions into account (by skipping files that are already
  // being compacted). Since we just changed compaction score, we recalculate it
  // here
  vstorage_->ComputeCompactionScore(ioptions_, mutable_cf_options_);
  return c;
}

/*
 * Find the optimal path to place a file
 * Given a level, finds the path where levels up to it will fit in levels
 * up to and including this path
 */
uint32_t LevelCompactionBuilder::GetPathId(
    const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options, int level) {
  uint32_t p = 0;
  assert(!ioptions.cf_paths.empty());

  // size remaining in the most recent path
  uint64_t current_path_size = ioptions.cf_paths[0].target_size;

  uint64_t level_size;
  int cur_level = 0;

  // max_bytes_for_level_base denotes L1 size.
  // We estimate L0 size to be the same as L1.
  level_size = mutable_cf_options.max_bytes_for_level_base;

  // Last path is the fallback
  while (p < ioptions.cf_paths.size() - 1) {
    if (level_size <= current_path_size) {
      if (cur_level == level) {
        // Does desired level fit in this path?
        return p;
      } else {
        current_path_size -= level_size;
        if (cur_level > 0) {
          if (ioptions.level_compaction_dynamic_level_bytes) {
            // Currently, level_compaction_dynamic_level_bytes is ignored when
            // multiple db paths are specified. https://github.com/facebook/
            // rocksdb/blob/master/db/column_family.cc.
            // Still, adding this check to avoid accidentally using
            // max_bytes_for_level_multiplier_additional
            level_size = static_cast<uint64_t>(
                level_size * mutable_cf_options.max_bytes_for_level_multiplier);
          } else {
            level_size = static_cast<uint64_t>(
                level_size * mutable_cf_options.max_bytes_for_level_multiplier *
                mutable_cf_options.MaxBytesMultiplerAdditional(cur_level));
          }
        }
        cur_level++;
        continue;
      }
    }
    p++;
    current_path_size = ioptions.cf_paths[p].target_size;
  }
  return p;
}

bool LevelCompactionBuilder::PickFileToCompact() {
  // level 0 files are overlapping. So we cannot pick more
  // than one concurrent compactions at this level. This
  // could be made better by looking at key-ranges that are
  // being compacted at level 0.
  if (start_level_ == 0 &&
      !compaction_picker_->level0_compactions_in_progress()->empty()) {
    TEST_SYNC_POINT("LevelCompactionPicker::PickCompactionBySize:0");
    return false;
  }

  start_level_inputs_.clear();

  assert(start_level_ >= 0);

  // Pick the largest file in this level that is not already
  // being compacted
  const std::vector<int>& file_size =
      vstorage_->FilesByCompactionPri(start_level_);
  const std::vector<FileMetaData*>& level_files =
      vstorage_->LevelFiles(start_level_);

  unsigned int cmp_idx;
  for (cmp_idx = vstorage_->NextCompactionIndex(start_level_);
       cmp_idx < file_size.size(); cmp_idx++) {
    int index = file_size[cmp_idx];
    auto* f = level_files[index];

    // do not pick a file to compact if it is being compacted
    // from n-1 level.
    if (f->being_compacted) {
      continue;
    }

    start_level_inputs_.files.push_back(f);
    start_level_inputs_.level = start_level_;
    if (!compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                    &start_level_inputs_) ||
        compaction_picker_->FilesRangeOverlapWithCompaction(
            {start_level_inputs_}, output_level_)) {
      // A locked (pending compaction) input-level file was pulled in due to
      // user-key overlap.
      start_level_inputs_.clear();
      continue;
    }

    // Now that input level is fully expanded, we check whether any output files
    // are locked due to pending compaction.
    //
    // Note we rely on ExpandInputsToCleanCut() to tell us whether any output-
    // level files are locked, not just the extra ones pulled in for user-key
    // overlap.
    InternalKey smallest, largest;
    compaction_picker_->GetRange(start_level_inputs_, &smallest, &largest);
    CompactionInputFiles output_level_inputs;
    output_level_inputs.level = output_level_;
    vstorage_->GetOverlappingInputs(output_level_, &smallest, &largest,
                                    &output_level_inputs.files);
    if (!output_level_inputs.empty() &&
        !compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                    &output_level_inputs)) {
      start_level_inputs_.clear();
      continue;
    }
    base_index_ = index;
    break;
  }

  // store where to start the iteration in the next call to PickCompaction
  vstorage_->SetNextCompactionIndex(start_level_, cmp_idx);

  return start_level_inputs_.size() > 0;
}

bool LevelCompactionBuilder::PickIntraL0Compaction() {
  start_level_inputs_.clear();
  const std::vector<FileMetaData*>& level_files =
      vstorage_->LevelFiles(0 /* level */);
  if (level_files.size() <
          static_cast<size_t>(
              mutable_cf_options_.level0_file_num_compaction_trigger + 2) ||
      level_files[0]->being_compacted) {
    // If L0 isn't accumulating much files beyond the regular trigger, don't
    // resort to L0->L0 compaction yet.
    return false;
  }
  return FindIntraL0Compaction(
      level_files, kMinFilesForIntraL0Compaction, port::kMaxUint64,
      mutable_cf_options_.max_compaction_bytes, &start_level_inputs_);
}
}  // namespace

Compaction* LevelCompactionPicker::PickCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    VersionStorageInfo* vstorage, LogBuffer* log_buffer, bool for_column_compaction, NvmCfModule* nvmcf) {
  LevelCompactionBuilder builder(cf_name, vstorage, this, log_buffer,
                                 mutable_cf_options, ioptions_);
  return builder.PickCompaction(for_column_compaction, nvmcf);
}
}  // namespace rocksdb
