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
  std::string snapshot = "";
  if (!logged_) {
    s = WriteSnapshot(&snapshot);
    if (s.ok()) {
      logged_ = true;
    }
  }

  // MANIFEST STUFF...
  uint64_t current_lba;
  s = manifest_->GetCurrentWriteHead(&current_lba);
  if (!s.ok()) {
    return s;
  }
  edit->SetComparatorName(icmp_.user_comparator()->Name());
  std::string record;
  edit->EncodeTo(&record);
  s = manifest_->NewManifest(snapshot + record);
  if (s.ok()) {
    s = manifest_->SetCurrent(current_lba);
  }
  // Installing?
  if (s.ok()) {
    AppendVersion(v);
  }
  return s;
}

Status ZnsVersionSet::Compact(ZnsCompaction* c) {
  c->SetupTargets(current_->ss_[current_->compaction_level_],
                  current_->ss_[current_->compaction_level_ + 1]);
  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE
