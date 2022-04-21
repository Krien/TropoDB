// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_VERSION_SET_H
#define ZNS_VERSION_SET_H

#include "db/dbformat.h"
#include "db/zns_impl/index/zns_version.h"
#include "db/zns_impl/index/zns_version_edit.h"
#include "db/zns_impl/io/device_wrapper.h"
#include "db/zns_impl/persistence/zns_manifest.h"
#include "db/zns_impl/ref_counter.h"
#include "db/zns_impl/table/zns_sstable_manager.h"
#include "db/zns_impl/table/zns_zonemetadata.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
/**
 * @brief Manages the current index and its information; and allows swapping to
 * a new index.
 *
 */
class ZnsVersionSet {
 public:
  ZnsVersionSet(const InternalKeyComparator& icmp,
                ZNSSSTableManager* znssstable, ZnsManifest* manifest,
                uint64_t lba_size);
  ZnsVersionSet(const ZnsVersionSet&) = delete;
  ZnsVersionSet& operator=(const ZnsVersionSet&) = delete;
  ~ZnsVersionSet();

  Status WriteSnapshot(std::string* snapshot_dst, ZnsVersion* version);
  Status LogAndApply(ZnsVersionEdit* edit);
  Status RemoveObsoleteZones(ZnsVersionEdit* edit);

  void GetLiveZoneRanges(size_t level,
                         std::vector<std::pair<uint64_t, uint64_t>>* ranges);

  inline ZnsVersion* current() { return current_; }
  inline uint64_t LastSequence() const { return last_sequence_; }
  inline void SetLastSequence(uint64_t s) {
    assert(s >= last_sequence_);
    last_sequence_ = s;
  }
  inline uint64_t NewSSNumber() { return ss_number_++; }
  inline int NumLevelZones(int level) const {
    assert(level >= 0 && level < 7);
    return current_->ss_[level].size();
  }
  inline int NumLevelBytes(int level) const {
    const std::vector<SSZoneMetaData*>& ss = current_->ss_[level];
    int64_t sum = 0;
    for (size_t i = 0; i < ss.size(); i++) {
      sum += ss[i]->lba_count * lba_size_;
    }
    return sum;
  }

  bool NeedsCompaction() const {
    //printf("Score %f \n", current_->compaction_score_);
    return current_->compaction_score_ >= 1;
  }

  Status Compact(ZnsCompaction* c);
  // ONLY call on startup or recovery, this is not thread safe and drops current
  // data.
  Status Recover();
  std::string DebugString();

 private:
  class Builder;

  friend class ZnsVersion;
  friend class ZnsCompaction;

  void AppendVersion(ZnsVersion* v);
  Status CommitVersion(ZnsVersion* v, ZNSSSTableManager* man);
  Status DecodeFrom(const Slice& input, ZnsVersionEdit* edit,
                    ZNSSSTableManager* man);

  ZnsVersion dummy_versions_;
  ZnsVersion* current_;
  const InternalKeyComparator icmp_;
  ZNSSSTableManager* znssstable_;
  ZnsManifest* manifest_;
  uint64_t lba_size_;
  uint64_t last_sequence_;
  uint64_t ss_number_;
  bool logged_;
};

class ZnsVersionSet::Builder {
 public:
  Builder(ZnsVersionSet* vset, ZnsVersion* base);
  ~Builder();
  void Apply(const ZnsVersionEdit* edit);
  void SaveTo(ZnsVersion* v);
  void MaybeAddZone(ZnsVersion* v, int level, SSZoneMetaData* f);

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
  };

  ZnsVersionSet* vset_;
  ZnsVersion* base_;
  LevelState levels_[7];
};
}  // namespace ROCKSDB_NAMESPACE

#endif
#endif
