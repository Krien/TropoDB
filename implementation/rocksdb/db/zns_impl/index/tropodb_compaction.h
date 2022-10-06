// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once
#ifdef TROPODB_PLUGIN_ENABLED
#ifndef ZNS_COMPACTION_H
#define ZNS_COMPACTION_H

#include "db/dbformat.h"
#include "db/zns_impl/tropodb_config.h"
#include "db/zns_impl/index/tropodb_version.h"
#include "db/zns_impl/index/tropodb_version_edit.h"
#include "db/zns_impl/index/tropodb_version_set.h"
#include "db/zns_impl/persistence/tropodb_manifest.h"
#include "db/zns_impl/ref_counter.h"
#include "db/zns_impl/table/zns_sstable_manager.h"
#include "db/zns_impl/table/zns_zonemetadata.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

struct DeferredLNCompaction {
  port::Mutex mutex_;
  port::CondVar new_task_;  // In case deferred is waiting for main
  bool last_{false};        // Signal that we are done and deferred should be gone
  bool done_{false};        // Signal that deferred is done
  std::vector<SSTableBuilder*> deferred_builds_; // All deferred builders to use for compaction
  uint8_t index_{0};
  uint8_t level_{0};
  ZnsVersionEdit* edit_{nullptr};
  DeferredLNCompaction() : new_task_(&mutex_) {}
};

class ZnsCompaction {
 public:
  ZnsCompaction(ZnsVersionSet* vset, uint8_t first_level, Env* env);
  ~ZnsCompaction();

  // BG Thread coordination
  bool HasOverlapWithOtherCompaction(std::vector<SSZoneMetaData*> metas);

  // Compaction information
  void GetCompactionTargets(std::vector<SSZoneMetaData*>* metas);
  inline bool IsBusy() const { return busy_; }

  // Trivial ops
  bool IsTrivialMove() const;
  Status DoTrivialMove(ZnsVersionEdit* edit);

  // Compactions
  void MarkCompactedTablesAsDead(ZnsVersionEdit* edit);
  Status DoCompaction(ZnsVersionEdit* edit);

 private:
  friend class ZnsVersionSet;

  // Compaction
  static Iterator* GetLNIterator(void* arg, const Slice& file_value,
                                 const Comparator* cmp);
  Iterator* MakeCompactionIterator();
  Status FlushSSTable(SSTableBuilder** builder, ZnsVersionEdit* edit_,
                      SSZoneMetaData* meta);
  static void DeferCompactionWrite(void* deferred_compaction);

  // helpers
  bool IsBaseLevelForKey(const Slice& user_key);

  // Meta
  uint8_t first_level_;
  uint64_t max_lba_count_;
  // References
  ZnsVersionSet* vset_;
  ZnsVersion* version_;
  ZnsVersionEdit edit_;
  // Target
  std::array<std::vector<SSZoneMetaData*>, 2U> targets_;
  size_t level_ptrs_[ZnsConfig::level_count];

  std::vector<SSZoneMetaData*> grandparents_;
  bool busy_;

  SystemClock* const clock_;

  // Deferred
  Env* env_;
  DeferredLNCompaction deferred_;
  std::vector<SSZoneMetaData*> metas_;
};
}  // namespace ROCKSDB_NAMESPACE

#endif
#endif
