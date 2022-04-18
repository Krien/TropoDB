// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_VERSION_EDIT_H
#define ZNS_VERSION_EDIT_H

#include "db/dbformat.h"
#include "db/zns_impl/device_wrapper.h"
#include "db/zns_impl/index/zns_version.h"
#include "db/zns_impl/ref_counter.h"
#include "db/zns_impl/zns_manifest.h"
#include "db/zns_impl/zns_sstable_manager.h"
#include "db/zns_impl/zns_zonemetadata.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
typedef std::set<std::pair<int, uint64_t>> DeletedZoneSet;
/**
 * @brief Prepares the changes to the index structure to allow for CoW behaviour
 * for the index.
 */
class ZnsVersionEdit {
 public:
  ZnsVersionEdit();
  ~ZnsVersionEdit() = default;

  void Clear();
  void AddSSDefinition(int level, uint64_t number, uint64_t lba,
                       uint64_t lba_count, uint64_t numbers,
                       const InternalKey& smallest, const InternalKey& largest);
  void RemoveSSDefinition(int level, uint64_t number);
  // Used for Manifest logic
  void EncodeTo(std::string* dst);
  Status DecodeFrom(const Slice& src);

  // Setters
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetSSNumber(uint64_t num) {
    has_next_ss_number = true;
    ss_number = num;
  }

 private:
  friend class ZnsVersionSet;
  friend class ZnsCompaction;

  std::vector<std::pair<int, SSZoneMetaData>> new_ss_;
  DeletedZoneSet deleted_ss_;
  std::vector<std::pair<int, SSZoneMetaData>> deleted_ss_seq_;
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
