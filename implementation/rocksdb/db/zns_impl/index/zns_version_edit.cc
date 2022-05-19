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
  new_ss_L0_.clear();
  new_ss_LN_.clear();
  deleted_ss_.clear();
  deleted_range_.clear();
  compact_pointers_.clear();
  has_comparator_ = false;
  comparator_.clear();
  has_next_ss_number = false;
  ss_number = 0;
}

void ZnsVersionEdit::AddSSDefinition(const uint8_t level,
                                     const SSZoneMetaData* basemeta) {
  if (level == 0) {
    const SSZoneMetaDataL0* meta =
        dynamic_cast<const SSZoneMetaDataL0*>(basemeta);
    SSZoneMetaDataL0 f;
    f.number = meta->number;
    f.lba = meta->lba;
    f.numbers = meta->numbers;
    f.lba_count = meta->lba_count;
    f.smallest = meta->smallest;
    f.largest = meta->largest;
    new_ss_L0_.push_back(std::make_pair(level, f));
  } else {
    const SSZoneMetaDataLN* meta =
        dynamic_cast<const SSZoneMetaDataLN*>(basemeta);
    SSZoneMetaDataLN f;
    f.number = meta->number;
    f.lba_regions = meta->lba_regions;
    std::copy(meta->lbas, meta->lbas + f.lba_regions, f.lbas);
    std::copy(meta->lba_region_sizes, meta->lba_region_sizes + f.lba_regions,
              f.lba_region_sizes);
    f.numbers = meta->numbers;
    f.lba_count = meta->lba_count;
    f.smallest = meta->smallest;
    f.largest = meta->largest;
    new_ss_LN_.push_back(std::make_pair(level, f));
  }
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

  // deleted ranges
  for (const auto& deleted_range : deleted_range_) {
    PutVarint32(dst, static_cast<uint32_t>(ZnsVersionTag::kDeletedSSTable));
    PutFixed8(dst, deleted_range.first);            // level
    PutVarint64(dst, deleted_range.second.first);   // range first
    PutVarint64(dst, deleted_range.second.second);  // range last
  }

  // deleted LN

  // new files
  for (size_t i = 0; i < new_ss_L0_.size(); i++) {
    const SSZoneMetaDataL0& m = new_ss_L0_[i].second;
    PutVarint32(dst, static_cast<uint32_t>(ZnsVersionTag::kNewSSTable));
    PutFixed8(dst, new_ss_L0_[i].first);  // level
    m.Encode(dst);
  }
  for (size_t i = 0; i < new_ss_LN_.size(); i++) {
    const SSZoneMetaDataLN& m = new_ss_LN_[i].second;
    PutVarint32(dst, static_cast<uint32_t>(ZnsVersionTag::kNewSSTable));
    PutFixed8(dst, new_ss_LN_[i].first);  // level
    m.Encode(dst);
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
  SSZoneMetaDataL0 m0;
  SSZoneMetaDataLN mn;
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
        if (GetLevel(&input, &level) &&
            ((level == 0 && m0.DecodeFrom(&input)) ||
             (mn.DecodeFrom(&input)))) {
          if (level == 0) {
            new_ss_L0_.push_back(std::make_pair(0, m0));
          } else {
            new_ss_LN_.push_back(std::make_pair(level, mn));
          }
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
