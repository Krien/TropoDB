// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_VERSION_H
#define ZNS_VERSION_H

#include "db/dbformat.h"
#include "db/lookup_key.h"
#include "db/zns_impl/device_wrapper.h"
#include "db/zns_impl/ref_counter.h"
#include "db/zns_impl/zns_manifest.h"
#include "db/zns_impl/zns_sstable_manager.h"
#include "db/zns_impl/zns_zonemetadata.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
// Prevent issues with cycles
class ZnsVersionEdit;
class ZnsVersion;
class ZnsVersionSet;
class ZnsCompaction;

enum class ZnsCommitTag : uint32_t {
  kEdit = 1,
  kSSManager = 2,
  kClosing = 3
};

enum class ZnsVersionTag : uint32_t {
  kComparator = 1,
  kLogNumber = 2,
  kNextSSTableNumber = 3,
  kLastSequence = 4,
  kCompactPointer = 5,
  kDeletedSSTable = 6,
  kNewSSTable = 7,
  
  kPrevLogNumber = 9
};

/**
 * @brief Readonly index structure that allows reading SSTables from ZNS.
 */
class ZnsVersion : public RefCounter {
 public:
  void Clear();
  Status Get(const ReadOptions& options, const LookupKey& key,
             std::string* value);

 private:
  friend class ZnsVersionSet;
  friend class ZnsCompaction;

  ZnsVersion();
  explicit ZnsVersion(ZnsVersionSet* vset);
  ~ZnsVersion();

  std::vector<SSZoneMetaData*> ss_[7];
  ZnsVersionSet* vset_;
  double compaction_score_;
  int compaction_level_;
};
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif