// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_COMPACTION_H
#define ZNS_COMPACTION_H

#include "db/dbformat.h"
#include "db/zns_impl/config.h"
#include "db/zns_impl/index/zns_version.h"
#include "db/zns_impl/index/zns_version_edit.h"
#include "db/zns_impl/index/zns_version_set.h"
#include "db/zns_impl/persistence/zns_manifest.h"
#include "db/zns_impl/ref_counter.h"
#include "db/zns_impl/table/zns_sstable_manager.h"
#include "db/zns_impl/table/zns_zonemetadata.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
class ZnsCompaction {
 public:
  ZnsCompaction(ZnsVersionSet* vset);
  ~ZnsCompaction();

  void SetupTargets(const std::vector<SSZoneMetaData*>& t1,
                    const std::vector<SSZoneMetaData*>& t2);
  void MarkStaleTargetsReusable(ZnsVersionEdit* edit);
  bool IsTrivialMove() const;
  Status DoTrivialMove(ZnsVersionEdit* edit);
  Iterator* MakeCompactionIterator();
  Status DoCompaction(ZnsVersionEdit* edit);

 private:
  static Iterator* GetLNIterator(void* arg, const Slice& file_value,
                                 const InternalKeyComparator& icmp);
  Status FlushSSTable(SSTableBuilder** builder, ZnsVersionEdit* edit_,
                      SSZoneMetaData* meta);
  bool IsBaseLevelForKey(const Slice& user_key);

  size_t first_level_;
  // arbitrary???
  uint64_t max_lba_count = 8;
  ZnsVersionSet* vset_;
  ZnsVersion* version_;
  ZnsVersionEdit edit_;
  std::vector<SSZoneMetaData*> targets_[2];
  size_t level_ptrs_[ZnsConfig::level_count];
};
}  // namespace ROCKSDB_NAMESPACE

#endif
#endif
