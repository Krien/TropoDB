// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once
#ifdef TROPODB_PLUGIN_ENABLED
#ifndef ZNS_VERSION_SET_H
#define ZNS_VERSION_SET_H

#include "db/dbformat.h"
#include "db/tropodb/tropodb_config.h"
#include "db/tropodb/index/tropodb_version.h"
#include "db/tropodb/index/tropodb_version_edit.h"
#include "db/tropodb/io/szd_port.h"
#include "db/tropodb/persistence/tropodb_manifest.h"
#include "db/tropodb/ref_counter.h"
#include "db/tropodb/table/tropodb_sstable_manager.h"
#include "db/tropodb/table/tropodb_table_cache.h"
#include "db/tropodb/table/tropodb_zonemetadata.h"
#include "port/port.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
/**
 * @brief Manages the current index and its information; and allows swapping to
 * a new index.
 *
 */
class TropoVersionSet {
 public:
  TropoVersionSet(const InternalKeyComparator& icmp,
                TropoSSTableManager* znssstable, TropoManifest* manifest,
                const uint64_t lba_size, const uint64_t zone_cap,
                TropoTableCache* table_cache, Env* env);
  TropoVersionSet(const TropoVersionSet&) = delete;
  TropoVersionSet& operator=(const TropoVersionSet&) = delete;
  ~TropoVersionSet();

  Status WriteSnapshot(std::string* snapshot_dst, TropoVersion* version);
  Status LogAndApply(TropoVersionEdit* edit);
  void RecalculateScore();
  Status RemoveObsoleteZones(TropoVersionEdit* edit);

  void GetLiveZones(const uint8_t level, std::set<uint64_t>& live);
  void GetSaveDeleteRange(const uint8_t level,
                          std::pair<uint64_t, uint64_t>* range);
  Status ReclaimStaleSSTablesL0(port::Mutex* mutex_, port::CondVar* cond);
  Status ReclaimStaleSSTablesLN(port::Mutex* mutex_, port::CondVar* cond);

  inline TropoVersion* current() const { return current_; }
  inline uint64_t LastSequence() const { return last_sequence_; }
  inline void SetLastSequence(uint64_t s) {
    assert(s >= last_sequence_);
    last_sequence_ = s;
  }
  inline uint64_t NewSSNumber() { return ss_number_++; }
  inline uint64_t NewSSNumberL0() { return ss_number_l0_++; }

  inline int NumLevelZones(uint8_t level) const {
    assert(level < TropoDBConfig::level_count);
    return current_->ss_[level].size();
  }
  inline int NumLevelBytes(uint8_t level) const {
    const std::vector<SSZoneMetaData*>& ss = current_->ss_[level];
    int64_t sum = 0;
    for (size_t i = 0; i < ss.size(); i++) {
      sum += ss[i]->lba_count * lba_size_;
    }
    return sum;
  }

  bool NeedsCompaction() const {
    // printf("Score %f \n", current_->compaction_score_);
    return current_->compaction_score_ >= 1 &&
           current_->compaction_level_ != TropoDBConfig::level_count + 1;
  }

  bool NeedsL0Compaction() const {
    bool needcompaction =
        current_->ss_[0].size() > TropoDBConfig::ss_compact_treshold[0] ||
        NeedsL0CompactionForce();
    if (!needcompaction) {
      for (size_t i = 0; i < TropoDBConfig::wal_concurrency; i++) {
        if (NeedsL0CompactionForceParallel(i)) {
          return true;
        }
      }
    }
    return needcompaction;
  }

  bool NeedsL0CompactionForce() const {
    return znssstable_->GetFractionFilled(0) /
               TropoDBConfig::ss_compact_treshold_force[0] >=
           1;
  }

  bool NeedsL0CompactionForceParallel(uint8_t parallel_number) const {
    return znssstable_->GetFractionFilledL0(parallel_number) /
               TropoDBConfig::ss_compact_treshold_force[0] >=
           1;
  }

  void GetRange(const std::vector<SSZoneMetaData*>& inputs,
                InternalKey* smallest, InternalKey* largest);
  void GetRange2(const std::vector<SSZoneMetaData*>& inputs1,
                 const std::vector<SSZoneMetaData*>& inputs2,
                 InternalKey* smallest, InternalKey* largest);
  void SetupOtherInputs(TropoCompaction* c, uint64_t max_lba_c);
  bool OnlyNeedDeletes(uint8_t level);
  TropoCompaction* PickCompaction(uint8_t level,
                                const std::vector<SSZoneMetaData*>& busy);
  // ONLY call on startup or recovery, this is not thread safe and drops current
  // data.
  Status Recover();
  std::string DebugString();

 private:
  class Builder;

  friend class TropoVersion;
  friend class TropoCompaction;

  void AppendVersion(TropoVersion* v);
  Status CommitVersion(TropoVersion* v, TropoSSTableManager* man);
  Status DecodeFrom(const Slice& input, TropoVersionEdit* edit);

  TropoVersion dummy_versions_;
  TropoVersion* current_;
  const InternalKeyComparator icmp_;
  TropoSSTableManager* znssstable_;
  TropoManifest* manifest_;
  uint64_t lba_size_;
  uint64_t zone_cap_;
  uint64_t last_sequence_;
  std::atomic<uint64_t> ss_number_;
  std::atomic<uint64_t> ss_number_l0_;
  bool logged_;
  TropoTableCache* table_cache_;
  Env* env_;

  // Per-level key at which the next compaction at that level should start.
  // Either an empty string, or a valid InternalKey.
  std::array<std::string, TropoDBConfig::level_count> compact_pointer_;
};

class TropoVersionSet::Builder {
 public:
  Builder(TropoVersionSet* vset, TropoVersion* base);
  ~Builder();
  void Apply(const TropoVersionEdit* edit);
  void SaveTo(TropoVersion* v);
  void MaybeAddZone(TropoVersion* v, uint8_t level, SSZoneMetaData* f);

 private:
  // Helper to sort by v->files_[file_number].smallest
  struct BySmallestKey {
    const InternalKeyComparator* internal_comparator;

    bool operator()(SSZoneMetaData* m1, SSZoneMetaData* m2) const {
      int r = internal_comparator->Compare(m1->smallest, m2->smallest);
      if (r != 0) {
        return (r < 0);
      } else {
        // Break ties by file number
        return (m1->number < m2->number);
      }
    }
  };

  typedef std::set<SSZoneMetaData*, BySmallestKey> ZoneSet;
  struct LevelState {
    std::set<uint64_t> deleted_ss;
    ZoneSet* added_ss;
    std::vector<SSZoneMetaData*> deleted_ss_pers;
  };

  std::pair<uint64_t, uint64_t> ss_deleted_range_;
  Slice fragmented_data_;

  TropoVersionSet* vset_;
  TropoVersion* base_;
  std::array<LevelState, TropoDBConfig::level_count> levels_;
};
}  // namespace ROCKSDB_NAMESPACE

#endif
#endif
