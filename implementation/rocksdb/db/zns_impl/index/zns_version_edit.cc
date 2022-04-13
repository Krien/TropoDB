// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/zns_impl/index/zns_version_edit.h"

#include "db/zns_impl/index/zns_version.h"
#include "rocksdb/rocksdb_namespace.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {
ZnsVersionEdit::ZnsVersionEdit() { Clear(); }

void ZnsVersionEdit::Clear() {
  last_sequence_ = 0;
  has_last_sequence_ = false;
  new_ss_.clear();
  deleted_ss_.clear();
  deleted_ss_seq_.clear();
  has_comparator_ = false;
  comparator_.clear();
}

void ZnsVersionEdit::AddSSDefinition(int level, uint64_t number, uint64_t lba,
                                     uint64_t lba_count, uint64_t numbers,
                                     const InternalKey& smallest,
                                     const InternalKey& largest) {
  SSZoneMetaData f;
  f.number = number;
  f.lba = lba;
  f.numbers = numbers;
  f.lba_count = lba_count;
  f.smallest = smallest;
  f.largest = largest;
  new_ss_.push_back(std::make_pair(level, f));
}

void ZnsVersionEdit::RemoveSSDefinition(int level, uint64_t number) {
  deleted_ss_.insert(std::make_pair(level, number));
}

void ZnsVersionEdit::EncodeTo(std::string* dst) {
  // comparator
  if (has_comparator_) {
    PutVarint32(dst, static_cast<uint32_t>(VersionTag::kComparator));
    PutLengthPrefixedSlice(dst, comparator_);
  }
  // last sequence
  if (has_last_sequence_) {
    PutVarint32(dst, static_cast<uint32_t>(VersionTag::kLastSequence));
    PutVarint64(dst, last_sequence_);
  }

  // compaction pointers

  // deleted pointers
  for (const auto& deleted_file_kvp : deleted_ss_) {
    PutVarint32(dst, static_cast<uint32_t>(VersionTag::kDeletedFile));
    PutVarint32(dst, deleted_file_kvp.first);   // level
    PutVarint64(dst, deleted_file_kvp.second);  // lba
  }

  // new files
  for (size_t i = 0; i < new_ss_.size(); i++) {
    const SSZoneMetaData& m = new_ss_[i].second;
    PutVarint32(dst, static_cast<uint32_t>(VersionTag::kNewFile));
    PutVarint32(dst, new_ss_[i].first);  // level
    PutVarint64(dst, m.number);
    PutVarint64(dst, m.lba);
    PutVarint64(dst, m.numbers);
    PutVarint64(dst, m.lba_count);
    PutLengthPrefixedSlice(dst, m.smallest.Encode());
    PutLengthPrefixedSlice(dst, m.largest.Encode());
  }
}
}  // namespace ROCKSDB_NAMESPACE
