// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_VERSION_H
#define ZNS_VERSION_H

#include "db/dbformat.h"
#include "db/lookup_key.h"
#include "db/zns_impl/config.h"
#include "db/zns_impl/io/szd_port.h"
#include "db/zns_impl/persistence/zns_manifest.h"
#include "db/zns_impl/ref_counter.h"
#include "db/zns_impl/table/zns_sstable_manager.h"
#include "db/zns_impl/table/zns_zonemetadata.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
// Prevent issues with cycles
class ZnsVersionEdit;
class ZnsVersion;
class ZnsVersionSet;
class ZnsCompaction;

enum class ZnsCommitTag : uint32_t { kEdit = 1, kSSManager = 2, kClosing = 3 };

enum class ZnsVersionTag : uint32_t {
  kComparator = 1,
  kLogNumber = 2,
  kNextSSTableNumber = 3,
  kLastSequence = 4,
  kCompactPointer = 5,
  kDeletedSSTable = 6,
  kNewSSTable = 7,

  kPrevLogNumber = 9,
  kDeletedRange = 0xa,
  kFragmentedData = 0xb
};

/**
 * @brief Readonly index structure that allows reading SSTables from ZNS.
 */
class ZnsVersion : public RefCounter {
 public:
  void Clear();
  Status Get(const ReadOptions& options, const LookupKey& key,
             std::string* value);

  void GetOverlappingInputs(uint8_t level, const InternalKey* begin,
                            const InternalKey* end,
                            std::vector<SSZoneMetaData*>* inputs);
  static Iterator* GetLNIterator(void* arg, const Slice& file_value,
                                 const Comparator* cmp);
  void AddIterators(const ReadOptions& options, std::vector<Iterator*>* iters);
  inline uint8_t CompactionLevel() const { return compaction_level_; }

 private:
  friend class ZnsVersionSet;
  friend class ZnsCompaction;

  ZnsVersion();
  explicit ZnsVersion(ZnsVersionSet* vset);
  ~ZnsVersion();

  // Version specific
  std::array<std::vector<SSZoneMetaData*>, ZnsConfig::level_count> ss_;
  std::array<std::vector<SSZoneMetaData*>, ZnsConfig::level_count> ss_d_;
  std::pair<uint64_t, uint64_t> ss_deleted_range_;
  // Parent
  ZnsVersionSet* vset_;
  // Linked list
  ZnsVersion* next_;
  ZnsVersion* prev_;
  // Compaction
  double compaction_score_;
  uint8_t compaction_level_;
  // DEBUG
  uint64_t debug_nr_;
};
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
