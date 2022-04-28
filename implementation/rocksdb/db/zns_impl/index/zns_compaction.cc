// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/zns_impl/index/zns_compaction.h"

#include "db/zns_impl/config.h"
#include "db/zns_impl/index/zns_version.h"
#include "db/zns_impl/index/zns_version_edit.h"
#include "db/zns_impl/index/zns_version_set.h"
#include "db/zns_impl/table/iterators/merging_iterator.h"
#include "db/zns_impl/table/iterators/sstable_ln_iterator.h"
#include "db/zns_impl/table/zns_sstable.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
ZnsCompaction::ZnsCompaction(ZnsVersionSet* vset) : vset_(vset) {
  first_level_ = vset->current_->compaction_level_;
  printf("Compacting from <%d>\n", first_level_);
  for (size_t i = 0; i < ZnsConfig::level_count; i++) {
    level_ptrs_[i] = 0;
  }
}

ZnsCompaction::~ZnsCompaction() {}

void ZnsCompaction::SetupTargets(const std::vector<SSZoneMetaData*>& t1,
                                 const std::vector<SSZoneMetaData*>& t2) {
  targets_[0] = t1;
  targets_[1] = t2;
}

Iterator* ZnsCompaction::GetLNIterator(void* arg, const Slice& file_value,
                                       const InternalKeyComparator& icmp) {
  ZNSSSTableManager* zns = reinterpret_cast<ZNSSSTableManager*>(arg);
  SSZoneMetaData* meta = new SSZoneMetaData();
  uint64_t lba_start = DecodeFixed64(file_value.data());
  uint64_t lba_count = DecodeFixed64(file_value.data() + 8);
  size_t level = (size_t)DecodeFixed16(file_value.data() + 16);
  meta->lba = lba_start;
  meta->lba_count = lba_count;
  Iterator* iterator = zns->NewIterator(level, meta, icmp);
  return iterator;
}

bool ZnsCompaction::IsTrivialMove() const {
  // add grandparent stuff level + 2
  return targets_[1].size() == 0 && targets_[0].size() == 1;
}

Status ZnsCompaction::DoTrivialMove(ZnsVersionEdit* edit) {
  Status s = Status::OK();
  SSZoneMetaData new_meta(targets_[0][0]);
  s = vset_->znssstable_->CopySSTable(first_level_, first_level_ + 1,
                                      &new_meta);
  if (!s.ok()) {
    return s;
  }
  new_meta.number = vset_->NewSSNumber();
  edit->AddSSDefinition(first_level_ + 1, new_meta.number, new_meta.lba,
                        new_meta.lba_count, new_meta.numbers, new_meta.smallest,
                        new_meta.largest);
  return s;
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
          vset_->znssstable_->NewIterator(0, *base_iter, vset_->icmp_);
    }
  }
  // LN
  int i = first_level_ == 0 ? 1 : 0;
  for (; i <= 1; i++) {
    iterators[iterator_index++] = new LNIterator(
        new LNZoneIterator(vset_->icmp_, &targets_[i], first_level_ + i),
        &GetLNIterator, vset_->znssstable_, vset_->icmp_);
    // printf("Iterators... %d %lu\n", i, iterators_needed);
  }
  return NewMergingIterator(&vset_->icmp_, iterators, iterators_needed);
}

void ZnsCompaction::MarkStaleTargetsReusable(ZnsVersionEdit* edit) {
  for (int i = 0; i <= 1; i++) {
    std::vector<SSZoneMetaData*>::const_iterator base_iter =
        targets_[i].begin();
    std::vector<SSZoneMetaData*>::const_iterator base_end = targets_[i].end();
    for (; base_iter != base_end; ++base_iter) {
      edit->RemoveSSDefinition(i + first_level_, (*base_iter)->number);
      edit->deleted_ss_seq_.push_back(
          std::make_pair(i + first_level_, *base_iter));
    }
  }
}

Status ZnsCompaction::FlushSSTable(SSTableBuilder** builder,
                                   ZnsVersionEdit* edit, SSZoneMetaData* meta) {
  Status s = Status::OK();
  SSTableBuilder* current_builder = *builder;
  meta->number = vset_->NewSSNumber();
  s = current_builder->Finalise();
  s = current_builder->Flush();
  // printf("adding... %u %lu %lu\n", first_level_ + 1, meta->lba,
  //       meta->lba_count);
  edit->AddSSDefinition(first_level_ + 1, meta->number, meta->lba,
                        meta->lba_count, meta->numbers, meta->smallest,
                        meta->largest);
  delete current_builder;
  current_builder = vset_->znssstable_->NewBuilder(first_level_ + 1, meta);

  *builder = current_builder;
  return s;
}

bool ZnsCompaction::IsBaseLevelForKey(const Slice& user_key) {
  const Comparator* user_cmp = vset_->icmp_.user_comparator();
  for (size_t lvl = first_level_ + 1; lvl < ZnsConfig::level_count; lvl++) {
    const std::vector<SSZoneMetaData*>& ss = vset_->current_->ss_[lvl];
    while (level_ptrs_[lvl] < ss.size()) {
      SSZoneMetaData* m = ss[level_ptrs_[lvl]];
      if (user_cmp->Compare(user_key, m->largest.user_key()) <= 0) {
        // We've advanced far enough
        if (user_cmp->Compare(user_key, m->smallest.user_key()) >= 0) {
          // Key falls in this file's range, so definitely not base level
          return false;
        }
        break;
      }
      level_ptrs_[lvl]++;
    }
  }
  return true;
}

Status ZnsCompaction::DoCompaction(ZnsVersionEdit* edit) {
  // printf("Starting compaction..\n");
  Status s = Status::OK();
  {
    SSZoneMetaData meta;
    SSTableBuilder* builder =
        vset_->znssstable_->NewBuilder(first_level_ + 1, &meta);
    {
      Iterator* merger = MakeCompactionIterator();
      merger->SeekToFirst();
      if (!merger->Valid()) {
        return Status::Corruption("No valid merging iterator");
      }
      ParsedInternalKey ikey;
      std::string current_user_key;
      bool has_current_user_key = false;
      SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
      SequenceNumber min_seq = vset_->LastSequence();
      const Comparator* ucmp = vset_->icmp_.user_comparator();
      for (; merger->Valid(); merger->Next()) {
        const Slice& key = merger->key();
        const Slice& value = merger->value();
        // verify
        bool drop = false;
        if (!ParseInternalKey(key, &ikey, false).ok()) {
          // Do not hide error keys
          current_user_key.clear();
          has_current_user_key = false;
          last_sequence_for_key = kMaxSequenceNumber;
        } else {
          if (!has_current_user_key ||
              ucmp->Compare(ikey.user_key, Slice(current_user_key)) != 0) {
            // first occurrence of this user key
            current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
            has_current_user_key = true;
            last_sequence_for_key = kMaxSequenceNumber;
          }
          if (last_sequence_for_key <= min_seq) {
            drop = true;
          } else if (ikey.type == kTypeDeletion && ikey.sequence <= min_seq &&
                     IsBaseLevelForKey(ikey.user_key)) {
            drop = true;
          }
        }
        last_sequence_for_key = ikey.sequence;
        // add
        if (drop) {
        } else {
          s = builder->Apply(key, value);
          if (builder->GetSize() / vset_->lba_size_ >= max_lba_count) {
            s = FlushSSTable(&builder, edit, &meta);
          }
        }
      }
      if (builder->GetSize() > 0) {
        s = FlushSSTable(&builder, edit, &meta);
      }
    }
    if (builder != nullptr) {
      delete builder;
    }
  }
  return s;
}
}  // namespace ROCKSDB_NAMESPACE
