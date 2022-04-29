// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/zns_impl/index/zns_version_set.h"

#include "db/zns_impl/config.h"
#include "db/zns_impl/index/zns_compaction.h"
#include "db/zns_impl/index/zns_version.h"
#include "db/zns_impl/index/zns_version_edit.h"
#include "db/zns_impl/table/iterators/merging_iterator.h"
#include "db/zns_impl/table/iterators/sstable_ln_iterator.h"
#include "db/zns_impl/table/zns_sstable.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
ZnsVersionSet::ZnsVersionSet(const InternalKeyComparator& icmp,
                             ZNSSSTableManager* znssstable,
                             ZnsManifest* manifest, const uint64_t lba_size,
                             ZnsTableCache* table_cache)
    : dummy_versions_(this),
      current_(nullptr),
      icmp_(icmp),
      znssstable_(znssstable),
      manifest_(manifest),
      lba_size_(lba_size),
      ss_number_(0),
      logged_(false),
      table_cache_(table_cache) {
  AppendVersion(new ZnsVersion(this));
};

ZnsVersionSet::~ZnsVersionSet() {
  current_->Unref();
  assert(dummy_versions_.next_ == &dummy_versions_);
}

void ZnsVersionSet::AppendVersion(ZnsVersion* v) {
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != nullptr) {
    current_->Unref();
  }
  current_ = v;
  v->Ref();

  // Append to linked list
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

void ZnsVersionSet::GetLiveZoneRanges(
    const size_t level, std::vector<std::pair<uint64_t, uint64_t>>* ranges) {
  std::pair ran = std::make_pair<uint64_t, uint64_t>(0, 0);
  bool ran_set = false;
  for (ZnsVersion* v = dummy_versions_.next_; v != &dummy_versions_;
       v = v->next_) {
    const std::vector<SSZoneMetaData*>& metas = v->ss_[level];
    std::pair temp_ran = std::make_pair<uint64_t, uint64_t>(0, 0);
    znssstable_->GetRange(level, metas, &temp_ran);
    if (!ran_set) {
      ran = temp_ran;
      ran_set = true;
      // printf("range %lu %lu \n", ran.first, ran.second);
    } else if (temp_ran.first != temp_ran.second) {
      ran.first = temp_ran.first;
    }
  }
  ranges->push_back(ran);
}

Status ZnsVersionSet::WriteSnapshot(std::string* snapshot_dst,
                                    ZnsVersion* version) {
  ZnsVersionEdit edit;
  edit.SetComparatorName(icmp_.user_comparator()->Name());
  // compaction stuff
  for (size_t level = 0; level < ZnsConfig::level_count; level++) {
    const std::vector<SSZoneMetaData*>& ss = version->ss_[level];
    for (size_t i = 0; i < ss.size(); i++) {
      const SSZoneMetaData* m = ss[i];
      edit.AddSSDefinition(level, m->number, m->lba, m->lba_count, m->numbers,
                           m->smallest, m->largest);
    }
  }
  edit.SetLastSequence(last_sequence_);
  edit.EncodeTo(snapshot_dst);
  return Status::OK();
}

Status ZnsVersionSet::LogAndApply(ZnsVersionEdit* edit) {
  Status s = Status::OK();
  // TODO: sanity checking...
  edit->SetLastSequence(last_sequence_);

  // TODO: improve... this is horrendous
  ZnsVersion* v = new ZnsVersion(this);
  {
    Builder builder(this, current_);
    builder.Apply(edit);
    builder.SaveTo(v);
  }
  s = CommitVersion(v, znssstable_);
  // Installing?
  if (s.ok()) {
    AppendVersion(v);
  }
  RecalculateScore();
  return s;
}

void ZnsVersionSet::RecalculateScore() {
  ZnsVersion* v = current_;
  size_t best_level = ZnsConfig::level_count + 1;
  double best_score = -1;
  double score;
  for (size_t i = 0; i < ZnsConfig::level_count - 1; i++) {
    score =
        znssstable_->GetFractionFilled(i) / ZnsConfig::ss_compact_treshold[i];
    if (score > best_score) {
      best_score = score;
      best_level = i;
      // printf("Score %f from level %d\n", best_score, best_level);
    }
  }
  v->compaction_level_ = best_level;
  v->compaction_score_ = best_score;
}

Status ZnsVersionSet::CommitVersion(ZnsVersion* v, ZNSSSTableManager* man) {
  Status s;
  // Setup version (for now CoW)
  std::string version_body;
  s = WriteSnapshot(&version_body, v);
  std::string version_data;
  PutVarint32(&version_data, static_cast<uint32_t>(ZnsCommitTag::kEdit));
  PutLengthPrefixedSlice(&version_data, version_body);
  // Setup SSManager pointers
  std::string manager_body;
  man->EncodeTo(&manager_body);
  std::string manager_data;
  PutVarint32(&manager_data, static_cast<uint32_t>(ZnsCommitTag::kSSManager));
  PutLengthPrefixedSlice(&manager_data, manager_body);
  // Padding
  std::string closer;
  PutVarint32(&closer, static_cast<uint32_t>(ZnsCommitTag::kClosing));
  // Write
  Slice result = version_data.append(manager_data).append(closer);
  uint64_t current_lba;
  s = manifest_->GetCurrentWriteHead(&current_lba);
  s = manifest_->NewManifest(result);
  if (s.ok()) {
    s = manifest_->SetCurrent(current_lba);
  }
  return Status::OK();
}

Status ZnsVersionSet::Compact(ZnsCompaction* c) {
  c->SetupTargets(current_->ss_[current_->compaction_level_],
                  current_->ss_[current_->compaction_level_ + 1]);
  return Status::OK();
}

Status ZnsVersionSet::RemoveObsoleteZones(ZnsVersionEdit* edit) {
  Status s = Status::OK();
  std::vector<std::pair<size_t, rocksdb::SSZoneMetaData>>& base_ss =
      edit->deleted_ss_seq_;
  std::vector<std::pair<size_t, rocksdb::SSZoneMetaData>>::const_iterator
      base_iter = base_ss.begin();
  std::vector<std::pair<size_t, rocksdb::SSZoneMetaData>>::const_iterator
      base_end = base_ss.end();
  for (; base_iter != base_end; ++base_iter) {
    const int level = (*base_iter).first;
    SSZoneMetaData m = (*base_iter).second;
    table_cache_->Evict(m.number);
  }
  return s;
}

Status ZnsVersionSet::DecodeFrom(const Slice& src, ZnsVersionEdit* edit,
                                 ZNSSSTableManager* man) {
  Status s = Status::OK();
  Slice input = Slice(src);
  uint32_t tag;
  Slice sub_input;
  ZnsCommitTag committag;
  bool force = false;
  while (!force && s.ok() && GetVarint32(&input, &tag)) {
    committag = static_cast<ZnsCommitTag>(tag);
    switch (committag) {
      case ZnsCommitTag::kEdit:
        if (GetLengthPrefixedSlice(&input, &sub_input)) {
          s = edit->DecodeFrom(sub_input);
        } else {
          s = Status::Corruption("VersionSet", "edit data");
        }
        break;
      case ZnsCommitTag::kSSManager:
        if (GetLengthPrefixedSlice(&input, &sub_input)) {
          s = man->DecodeFrom(sub_input);
        } else {
          s = Status::Corruption("VersionSet", "SStable data");
        }
        break;
      case ZnsCommitTag::kClosing:
        force = true;
        break;
      default:
        s = Status::Corruption("VersionSet", "unknown or unsupported tag");
        break;
    }
  }
  if (s.ok() && !input.empty() && !force) {
    s = Status::Corruption("VersionSet", "invalid tag");
  }
  return s;
}

Status ZnsVersionSet::Recover() {
  Status s;
  uint64_t start_manifest, end_manifest;
  s = manifest_->Recover();
  std::string manifest_data;
  s = manifest_->ReadManifest(&manifest_data);

  ZnsVersionEdit edit;
  s = DecodeFrom(manifest_data, &edit, znssstable_);
  if (!s.ok()) {
    return s;
  }

  // Install recovered edit
  if (s.ok()) {
    LogAndApply(&edit);
  }
  if (edit.has_last_sequence_) {
    last_sequence_ = edit.last_sequence_;
  }
  if (edit.has_next_ss_number) {
    ss_number_ = edit.ss_number;
  }

  // Setup numbers, temporary hack...
  if (ss_number_ == 0) {
    for (size_t i = 0; i < ZnsConfig::level_count; i++) {
      std::vector<SSZoneMetaData*>& m = current_->ss_[i];
      for (size_t j = 0; j < m.size(); j++) {
        ss_number_ = ss_number_ > m[j]->number ? ss_number_ : m[j]->number + 1;
      }
    }
  }
  return Status::OK();
}

std::string ZnsVersionSet::DebugString() {
  std::string result;
  for (size_t i = 0; i < ZnsConfig::level_count; i++) {
    std::vector<SSZoneMetaData*>& m = current_->ss_[i];
    result.append("\t" + std::to_string(i) + ": " + std::to_string(m.size()) +
                  "\n");
  }
  return result;
}

}  // namespace ROCKSDB_NAMESPACE
