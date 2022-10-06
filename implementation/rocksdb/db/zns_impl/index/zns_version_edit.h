// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once
#ifdef TROPODB_PLUGIN_ENABLED
#ifndef ZNS_VERSION_EDIT_H
#define ZNS_VERSION_EDIT_H

#include "db/dbformat.h"
#include "db/zns_impl/index/zns_version.h"
#include "db/zns_impl/io/szd_port.h"
#include "db/zns_impl/persistence/zns_manifest.h"
#include "db/zns_impl/ref_counter.h"
#include "db/zns_impl/table/zns_sstable_manager.h"
#include "db/zns_impl/table/zns_zonemetadata.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
typedef std::set<std::pair<uint8_t, uint64_t>> DeletedZoneSet;
typedef std::pair<uint64_t, uint64_t> DeletedZoneRange;
/**
 * @brief Prepares the changes to the index structure to allow for CoW behaviour
 * for the index.
 */
class ZnsVersionEdit {
 public:
  ZnsVersionEdit();
  ~ZnsVersionEdit() = default;
  void Clear();

  // SSTables
  void AddSSDefinition(const uint8_t level, const SSZoneMetaData& meta);
  void RemoveSSDefinition(const uint8_t level, const SSZoneMetaData& meta);
  void RemoveSSDefinitionOnlyMeta(const uint8_t level,
                                  const SSZoneMetaData& meta);

  // Used for persistency logic
  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  // Setters
  void SetLastSequence(const SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetSSNumber(const uint64_t num) {
    has_next_ss_number = true;
    ss_number = num;
  }
  void SetCompactPointer(uint8_t level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }
  void AddDeletedRange(const std::pair<uint64_t, uint64_t>& range) {
    deleted_range_ = range;
    has_deleted_range_ = true;
  }
  void AddFragmentedData(const Slice& fragmented_data) {
    // Important! Do use strings as Slice copies will not work correctly with
    // strings holding calloced C-strings...
    fragmented_data_ = fragmented_data.ToString();
    has_fragmented_data_ = true;
  }
  void AddDeletedSSTable(uint8_t level, const SSZoneMetaData& meta) {
    deleted_ss_pers_.push_back(std::make_pair(level, meta));
  }

 private:
  friend class ZnsVersionSet;
  friend class ZnsCompaction;

  std::vector<std::pair<uint8_t, SSZoneMetaData>> new_ss_;
  DeletedZoneSet deleted_ss_;
  std::string fragmented_data_;
  bool has_fragmented_data_;
  DeletedZoneRange deleted_range_;
  bool has_deleted_range_;
  std::vector<std::pair<uint8_t, SSZoneMetaData>> deleted_ss_pers_;

  std::vector<std::pair<uint8_t, InternalKey>> compact_pointers_;

  SequenceNumber last_sequence_;
  bool has_last_sequence_;
  std::string comparator_;
  bool has_comparator_;
  uint64_t ss_number;
  bool has_next_ss_number;
};
}  // namespace ROCKSDB_NAMESPACE

#endif
#endif
