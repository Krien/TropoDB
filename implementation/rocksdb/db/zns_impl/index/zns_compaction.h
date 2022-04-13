// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_COMPACTION_H
#define ZNS_COMPACTION_H

#include "db/dbformat.h"
#include "db/zns_impl/index/zns_version.h"
#include "db/zns_impl/index/zns_version_edit.h"
#include "db/zns_impl/index/zns_version_set.h"
#include "db/zns_impl/ref_counter.h"
#include "db/zns_impl/zns_manifest.h"
#include "db/zns_impl/zns_sstable_manager.h"
#include "db/zns_impl/zns_zonemetadata.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
class ZnsCompaction {
 public:
  ZnsCompaction(ZnsVersionSet* vset, int first_level);
  ~ZnsCompaction();

  bool IsTrivialMove() const;
  Iterator* MakeCompactionIterator();
  Status Compact(ZnsVersionEdit* edit);
  Status MoveUp(ZnsVersionEdit* edit, SSZoneMetaData* ss, int level);
  void SetupTargets(const std::vector<SSZoneMetaData*>& t1,
                    const std::vector<SSZoneMetaData*>& t2);

 private:
  static Iterator* GetLNIterator(void* arg, const Slice& file_value);

  int first_level_;
  uint64_t max_lba_count;
  ZnsVersionSet* vset_;
  ZnsVersion* version_;
  ZnsVersionEdit edit_;
  std::vector<SSZoneMetaData*> targets_[2];
};
}  // namespace ROCKSDB_NAMESPACE

#endif
#endif
