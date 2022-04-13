// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/zns_impl/index/zns_compaction.h"

#include "db/zns_impl/index/zns_version.h"
#include "db/zns_impl/index/zns_version_edit.h"
#include "db/zns_impl/index/zns_version_set.h"
#include "db/zns_impl/table/iterators/merging_iterator.h"
#include "db/zns_impl/table/iterators/sstable_ln_iterator.h"
#include "db/zns_impl/table/zns_sstable.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
ZnsCompaction::ZnsCompaction(ZnsVersionSet* vset, int first_level)
    : first_level_(first_level), vset_(vset) {}

ZnsCompaction::~ZnsCompaction() {}

void ZnsCompaction::SetupTargets(const std::vector<SSZoneMetaData*>& t1,
                                 const std::vector<SSZoneMetaData*>& t2) {
  targets_[0] = t1;
  targets_[1] = t2;
}

Iterator* ZnsCompaction::GetLNIterator(void* arg, const Slice& file_value) {
  ZNSSSTableManager* zns = reinterpret_cast<ZNSSSTableManager*>(arg);
  SSZoneMetaData* meta = new SSZoneMetaData();
  uint64_t lba_start = DecodeFixed64(file_value.data());
  uint64_t lba_count = DecodeFixed64(file_value.data() + 8);
  return zns->NewIterator(1, meta);
}

bool ZnsCompaction::IsTrivialMove() const {
  // add grandparent stuff level + 2
  return targets_[1].size() == 0 && targets_[0].size() == 1;
}

Iterator* ZnsCompaction::MakeCompactionIterator() {
  // 1 for each SStable in L0, 1 for each later level
  size_t iterators_needed = 1;
  iterators_needed += first_level_ == 0 ? targets_[0].size() : 1;
  Iterator** iterators = new Iterator*[iterators_needed];
  size_t iterator_index = 0;
  // L0
  if (first_level_ == 0) {
    const std::vector<SSZoneMetaData*>& l0ss = targets_[0];
    std::vector<SSZoneMetaData*>::const_iterator base_iter = l0ss.begin();
    std::vector<SSZoneMetaData*>::const_iterator base_end = l0ss.end();
    for (; base_iter != base_end; ++base_iter) {
      iterators[iterator_index++] =
          vset_->znssstable_->NewIterator(0, *base_iter);
    }
  }
  // LN
  int i = first_level_ == 0 ? 1 : 0;
  for (; i <= 1; i++) {
    iterators[iterator_index++] =
        new LNIterator(new LNZoneIterator(vset_->icmp_, &targets_[i]),
                       &GetLNIterator, vset_->znssstable_);
  }
  return NewMergingIterator(&vset_->icmp_, iterators, iterators_needed);
}

Status ZnsCompaction::Compact(ZnsVersionEdit* edit) {
  printf("Starting compaction..\n");
  Status s = Status::OK();
  // TODO: drastic fix, this is inefficient, out of place and wrong...
  {
    for (int i = 0; i <= 0; i++) {
      std::vector<SSZoneMetaData*>::const_iterator base_iter =
          targets_[i].begin();
      std::vector<SSZoneMetaData*>::const_iterator base_end = targets_[i].end();
      for (; base_iter != base_end; ++base_iter) {
        edit->RemoveSSDefinition(i, (*base_iter)->number);
        edit->deleted_ss_seq_.push_back(std::make_pair(i, *base_iter));
      }
    }
  }
  {
    SSZoneMetaData meta;
    meta.number = vset_->NewSSNumber();
    SSTableBuilder* builder =
        vset_->znssstable_->NewBuilder(first_level_ + 1, &meta);
    {
      Iterator* merger = MakeCompactionIterator();
      merger->SeekToFirst();
      if (!merger->Valid()) {
        return Status::Corruption("No valid merging iterator");
      }
      for (; merger->Valid(); merger->Next()) {
        const Slice& key = merger->key();
        const Slice& value = merger->value();
        s = builder->Apply(key, value);
      }
      s = builder->Finalise();
      s = builder->Flush();
    }
    edit->AddSSDefinition(1, meta.number, meta.lba, meta.lba_count,
                          meta.numbers, meta.smallest, meta.largest);
    delete builder;
  }
  return s;
}

Status ZnsCompaction::MoveUp(ZnsVersionEdit* edit, SSZoneMetaData* ss,
                             int original_level) {
  Status s = Status::OK();
  edit->RemoveSSDefinition(original_level, ss->number);
  edit->deleted_ss_seq_.push_back(std::make_pair(original_level, *ss));
  SSZoneMetaData new_meta(ss);
  s = vset_->znssstable_->CopySSTable(original_level, original_level + 1,
                                      &new_meta);
  if (!s.ok()) {
    return s;
  }
  new_meta.number = vset_->NewSSNumber();
  edit->AddSSDefinition(original_level + 1, new_meta.number, new_meta.lba,
                        new_meta.lba_count, new_meta.numbers, new_meta.smallest,
                        new_meta.largest);
  return s;
}

}  // namespace ROCKSDB_NAMESPACE