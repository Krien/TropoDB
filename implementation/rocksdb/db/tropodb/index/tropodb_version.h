// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once
#ifdef TROPODB_PLUGIN_ENABLED
#ifndef ZNS_VERSION_H
#define ZNS_VERSION_H

#include "db/dbformat.h"
#include "db/lookup_key.h"
#include "db/tropodb/tropodb_config.h"
#include "db/tropodb/io/szd_port.h"
#include "db/tropodb/persistence/tropodb_manifest.h"
#include "db/tropodb/ref_counter.h"
#include "db/tropodb/table/tropodb_sstable_manager.h"
#include "db/tropodb/table/tropodb_zonemetadata.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
// Prevent issues with cycles
class TropoVersionEdit;
class TropoVersion;
class TropoVersionSet;
class TropoCompaction;

enum class TropoCommitTag : uint32_t { kEdit = 1, kSSManager = 2, kClosing = 3 };

enum class TropoVersionTag : uint32_t {
  kComparator = 1,
  kLogNumber = 2,
  kNextSSTableNumber = 3,
  kLastSequence = 4,
  kCompactPointer = 5,
  kDeletedSSTable = 6,
  kNewSSTable = 7,
  /* what = 8,*/
  kPrevLogNumber = 9,
  kDeletedRange = 0xa,
  kFragmentedData = 0xb
};

/**
 * @brief Readonly index structure that allows reading SSTables from ZNS.
 */
class TropoVersion : public RefCounter {
 public:
  void Clear();
  Status Get(const ReadOptions& options, const LookupKey& key,
             std::string* value);
  void GetOverlappingInputs(uint8_t level, const InternalKey* begin,
                            const InternalKey* end,
                            std::vector<SSZoneMetaData*>* inputs);
  static Iterator* GetLNIterator(void* arg, const Slice& file_value,
                                 const Comparator* cmp);
  inline uint8_t CompactionLevel() const { return compaction_level_; }

 private:
  friend class TropoVersionSet;
  friend class TropoCompaction;

  // Managed by friend classes (TropoVersionSet)
  TropoVersion();
  explicit TropoVersion(TropoVersionSet* vset);
  ~TropoVersion();

  // FIXME: Do not use this function! It is broken and will not be maintained
  void AddIterators(const ReadOptions& options, std::vector<Iterator*>* iters);

  // Version specific
  std::array<std::vector<SSZoneMetaData*>, TropoDBConfig::level_count> ss_;
  std::array<std::vector<SSZoneMetaData*>, TropoDBConfig::level_count> ss_d_;
  std::pair<uint64_t, uint64_t> ss_deleted_range_;
  // Parent
  TropoVersionSet* vset_;
  // Linked list
  TropoVersion* next_;
  TropoVersion* prev_;
  // Compaction
  double compaction_score_;
  uint8_t compaction_level_;
  // DEBUG
  uint64_t debug_nr_;
};
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
