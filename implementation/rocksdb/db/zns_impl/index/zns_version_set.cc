// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/zns_impl/index/zns_version_set.h"

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
                             ZnsManifest* manifest, uint64_t lba_size)
    : current_(nullptr),
      icmp_(icmp),
      znssstable_(znssstable),
      manifest_(manifest),
      lba_size_(lba_size),
      ss_number_(0),
      logged_(false) {
  AppendVersion(new ZnsVersion(this));
};

ZnsVersionSet::~ZnsVersionSet() { current_->Unref(); }

void ZnsVersionSet::AppendVersion(ZnsVersion* v) {
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != nullptr) {
    current_->Unref();
  }
  current_ = v;
  v->Ref();
}

Status ZnsVersionSet::WriteSnapshot(std::string* snapshot_dst) {
  ZnsVersionEdit edit;
  edit.SetComparatorName(icmp_.user_comparator()->Name());
  // compaction stuff
  for (int level = 0; level < 7; level++) {
    const std::vector<SSZoneMetaData*>& ss = current_->ss_[level];
    for (size_t i = 0; i < ss.size(); i++) {
      const SSZoneMetaData* m = ss[i];
      edit.AddSSDefinition(level, m->number, m->lba, m->lba_count, m->numbers,
                           m->smallest, m->largest);
    }
  }
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
  int best_level = -1;
  double best_score = -1;
  double score;
  for (int i = 0; i < 7 - 1; i++) {
    score = v->ss_[i].size() / (3 + i * 3);
    if (score > best_score) {
      best_score = score;
      best_level = i;
    }
  }
  v->compaction_level_ = best_level;
  v->compaction_score_ = best_score;
  s = CommitVersion(edit, znssstable_);
  // Installing?
  if (s.ok()) {
    AppendVersion(v);
  }
  return s;
}

Status ZnsVersionSet::CommitVersion(ZnsVersionEdit* edit,
                                    ZNSSSTableManager* man) {
  Status s;
  // Setup version (for now CoW)
  std::string version_body;
  s = WriteSnapshot(&version_body);
  edit->EncodeTo(&version_body);
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
  std::vector<std::pair<int, rocksdb::SSZoneMetaData>>& base_ss =
      edit->deleted_ss_seq_;
  std::vector<std::pair<int, rocksdb::SSZoneMetaData>>::const_iterator
      base_iter = base_ss.begin();
  std::vector<std::pair<int, rocksdb::SSZoneMetaData>>::const_iterator
      base_end = base_ss.end();
  for (; base_iter != base_end; ++base_iter) {
    const int level = (*base_iter).first;
    SSZoneMetaData m = (*base_iter).second;
    s = znssstable_->InvalidateSSZone(level, &m);
    if (!s.ok()) {
      return s;
    }
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
    for (size_t i = 0; i < 7; i++) {
      std::vector<SSZoneMetaData*>& m = current_->ss_[i];
      for (size_t j = 0; j < m.size(); j++) {
        ss_number_ = ss_number_ > m[j]->number ? ss_number_ : m[j]->number + 1;
      }
    }
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
