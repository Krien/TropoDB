// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/zns_impl/index/zns_version_edit.h"

#include "db/zns_impl/config.h"
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
  deleted_range_.clear();
  compact_pointers_.clear();
  has_comparator_ = false;
  comparator_.clear();
  has_next_ss_number = false;
  ss_number = 0;
}

void ZnsVersionEdit::AddSSDefinition(const uint8_t level, const uint64_t number,
                                     const uint64_t lba,
                                     const uint64_t lba_count,
                                     const uint64_t numbers,
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

void ZnsVersionEdit::RemoveSSDefinition(const uint8_t level,
                                        const uint64_t number) {
  deleted_ss_.insert(std::make_pair(level, number));
}

void ZnsVersionEdit::EncodeTo(std::string* dst) const {
  // comparator
  if (has_comparator_) {
    PutVarint32(dst, static_cast<uint32_t>(ZnsVersionTag::kComparator));
    PutLengthPrefixedSlice(dst, comparator_);
  }
  // last sequence
  if (has_last_sequence_) {
    PutVarint32(dst, static_cast<uint32_t>(ZnsVersionTag::kLastSequence));
    PutVarint64(dst, last_sequence_);
  }

  if (has_next_ss_number) {
    PutVarint32(dst, static_cast<uint32_t>(ZnsVersionTag::kNextSSTableNumber));
    PutVarint64(dst, ss_number);
  }

  // compaction pointers
  for (size_t i = 0; i < compact_pointers_.size(); i++) {
    PutVarint32(dst, static_cast<uint32_t>(ZnsVersionTag::kCompactPointer));
    PutFixed8(dst, compact_pointers_[i].first);  // level
    PutLengthPrefixedSlice(dst, compact_pointers_[i].second.Encode());
  }

  // deleted pointers
  for (const auto& deleted_range : deleted_range_) {
    PutVarint32(dst, static_cast<uint32_t>(ZnsVersionTag::kDeletedSSTable));
    PutFixed8(dst, deleted_range.first);            // level
    PutVarint64(dst, deleted_range.second.first);   // range first
    PutVarint64(dst, deleted_range.second.second);  // range last
  }

  // new files
  for (size_t i = 0; i < new_ss_.size(); i++) {
    const SSZoneMetaData& m = new_ss_[i].second;
    PutVarint32(dst, static_cast<uint32_t>(ZnsVersionTag::kNewSSTable));
    PutFixed8(dst, new_ss_[i].first);  // level
    PutVarint64(dst, m.number);
    PutVarint64(dst, m.lba);
    PutVarint64(dst, m.numbers);
    PutVarint64(dst, m.lba_count);
    PutLengthPrefixedSlice(dst, m.smallest.Encode());
    PutLengthPrefixedSlice(dst, m.largest.Encode());
  }
}

static bool GetInternalKey(Slice* input, InternalKey* dst) {
  Slice str;
  if (GetLengthPrefixedSlice(input, &str)) {
    dst->DecodeFrom(str);
    return true;
  } else {
    return false;
  }
}

static bool GetLevel(Slice* input, uint8_t* level) {
  uint8_t v;
  if (GetFixed8(input, &v) && v < ZnsConfig::level_count) {
    *level = v;
    return true;
  } else {
    return false;
  }
}

Status ZnsVersionEdit::DecodeFrom(const Slice& src) {
  const char* msg = nullptr;
  Slice input = Slice(src);
  uint32_t tag;
  ZnsVersionTag versiontag;
  Slice str;
  uint8_t level;
  uint64_t number;
  uint64_t number_second;
  SSZoneMetaData m;
  InternalKey key;

  while (msg == nullptr && GetVarint32(&input, &tag)) {
    versiontag = static_cast<ZnsVersionTag>(tag);
    switch (versiontag) {
      case ZnsVersionTag::kComparator:
        if (GetLengthPrefixedSlice(&input, &str)) {
          comparator_ = str.ToString();
          has_comparator_ = true;
        } else {
          msg = "comparator name";
        }
        break;
      case ZnsVersionTag::kLastSequence:
        if (GetVarint64(&input, &last_sequence_)) {
          has_last_sequence_ = true;
        } else {
          msg = "last sequence number";
        }
        break;
      case ZnsVersionTag::kNextSSTableNumber:
        if (GetVarint64(&input, &ss_number)) {
          has_next_ss_number = true;
        } else {
          msg = "next ss number";
        }
        break;
      case ZnsVersionTag::kDeletedSSTable:
        if (GetLevel(&input, &level) && GetVarint64(&input, &number) &&
            GetVarint64(&input, &number_second)) {
          deleted_range_.push_back(
              std::make_pair(level, std::make_pair(number, number_second)));
        } else {
          msg = "deleted sstable entry";
        }
        break;
      case ZnsVersionTag::kNewSSTable:
        if (GetLevel(&input, &level) && GetVarint64(&input, &m.number) &&
            GetVarint64(&input, &m.lba) && GetVarint64(&input, &m.numbers) &&
            GetVarint64(&input, &m.lba_count) &&
            GetInternalKey(&input, &m.smallest) &&
            GetInternalKey(&input, &m.largest)) {
          new_ss_.push_back(std::make_pair(level, m));
        } else {
          msg = "new sstable entry";
        }
        break;
      case ZnsVersionTag::kCompactPointer:
        if (GetLevel(&input, &level) && GetInternalKey(&input, &key)) {
          compact_pointers_.push_back(std::make_pair(level, key));
        } else {
          msg = "compaction pointer";
        }
        break;
      default:
        msg = "unknown or unsupported tag";
        break;
    }
  }
  if (msg == nullptr && !input.empty()) {
    msg = "invalid tag";
  }
  if (msg != nullptr) {
    return Status::Corruption("VersionEdit", msg);
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
