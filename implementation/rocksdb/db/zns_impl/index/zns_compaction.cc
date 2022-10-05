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
#include "db/zns_impl/utils/tropodb_logger.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
ZnsCompaction::ZnsCompaction(ZnsVersionSet* vset, uint8_t first_level, Env* env)
    : first_level_(first_level),
      max_lba_count_((ZnsConfig::max_bytes_sstable_ + vset->lba_size_ - 1) /
                     vset->lba_size_),
      vset_(vset),
      version_(nullptr),
      busy_(false),
      clock_(SystemClock::Default().get()),
      env_(env) {
  // printf(
  //     "Max compaction size %lu %lu %lu %lu\n", ZnsConfig::max_bytes_sstable_,
  //     ((ZnsConfig::max_bytes_sstable_ + vset->lba_size_ - 1) /
  //     vset->lba_size_), max_lba_count_, vset->zone_cap_);
  for (size_t i = 0; i < ZnsConfig::level_count; i++) {
    level_ptrs_[i] = 0;
  }
}

ZnsCompaction::~ZnsCompaction() {
  if (version_ != nullptr) {
    version_->Unref();
  }
}

bool ZnsCompaction::HasOverlapWithOtherCompaction(
    std::vector<SSZoneMetaData*> metas) {
  for (const auto& target : metas) {
    for (const auto& c0 : targets_[0]) {
      // printf("overlap %lu == %lu \n", target->number, c0->number);
      if (target->number == c0->number) {
        return true;
      }
    }
    for (const auto& c1 : targets_[1]) {
      // printf("overlap %lu == %lu \n", target->number, c1->number);
      if (target->number == c1->number) {
        return true;
      }
    }
  }
  return false;
}

void ZnsCompaction::GetCompactionTargets(std::vector<SSZoneMetaData*>* metas) {
  for (const auto& c0 : targets_[0]) {
    // printf("ADDING %lu \n", c0->number);
    metas->push_back(c0);
  }
  for (const auto& c1 : targets_[1]) {
    // printf("ADDING %lu \n", c1->number);
    metas->push_back(c1);
  }
}

Iterator* ZnsCompaction::GetLNIterator(void* arg, const Slice& file_value,
                                       const Comparator* cmp) {
  ZNSSSTableManager* zns = reinterpret_cast<ZNSSSTableManager*>(arg);
  SSZoneMetaData meta;
  meta.LN.lba_regions = DecodeFixed8(file_value.data());
  // printf("Decoding :");
  for (size_t i = 0; i < meta.LN.lba_regions; i++) {
    meta.LN.lbas[i] = DecodeFixed64(file_value.data() + 1 + 16 * i);
    meta.LN.lba_region_sizes[i] = DecodeFixed64(file_value.data() + 9 + 16 * i);
    // printf(" %lu %lu - ", meta.LN.lbas[i], meta.LN.lba_region_sizes[i]);
  }
  uint64_t lba_count =
      DecodeFixed64(file_value.data() + 1 + 16 * meta.LN.lba_regions);
  uint8_t level =
      DecodeFixed8(file_value.data() + 9 + 16 * meta.LN.lba_regions);
  uint64_t number =
      DecodeFixed64(file_value.data() + 10 + 16 * meta.LN.lba_regions);
  meta.lba_count = lba_count;
  meta.number = number;
  // printf("%u %lu %lu %u \n", meta.LN.lba_regions, meta.number,
  // meta.lba_count,
  //        level);
  Iterator* iterator = zns->NewIterator(level, std::move(meta), cmp);
  return iterator;
}

static int64_t TotalLbas(const std::vector<SSZoneMetaData*>& ss) {
  int64_t sum = 0;
  for (size_t i = 0; i < ss.size(); i++) {
    sum += ss[i]->lba_count;
  }
  return sum;
}

static int64_t MaxGrandParentOverlapBytes(uint64_t lba_size) {
  return 10 * (((ZnsConfig::max_bytes_sstable_ + lba_size - 1) / lba_size) *
               lba_size);
}

bool ZnsCompaction::IsTrivialMove() const {
  // add grandparent stuff level + 2
  // Allow for higher levels...
  // printf("Compacting on %u: %lu %lu \n", first_level_, targets_[0].size(),
  //        targets_[1].size());
  return targets_[0].size() == 1 && targets_[1].size() == 0 &&
         TotalLbas(grandparents_) <=
             MaxGrandParentOverlapBytes(vset_->lba_size_);
}

Status ZnsCompaction::DoTrivialMove(ZnsVersionEdit* edit) {
  Status s = Status::OK();
  SSZoneMetaData* old_meta = targets_[0][0];
  // printf("Copying %lu %lu %lu \n", old_meta->number, old_meta->L0.lba,
  //        old_meta->lba_count);
  SSZoneMetaData meta;
  s = vset_->znssstable_->CopySSTable(first_level_, first_level_ + 1, *old_meta,
                                      &meta);
  meta.number = vset_->NewSSNumber();
  // printf("Copied %lu %lu %lu \n", meta.number, meta.L0.lba, meta.lba_count);
  if (!s.ok()) {
    return s;
  }
  edit->AddSSDefinition(first_level_ + 1, meta);
  // printf("Adding %lu \n", meta.number);
  // printf("adding... %u %lu %lu %s %s\n", first_level_ + 1,
  //        first_level_ == 0 ? meta.L0.lba : meta.LN.lbas[0], meta.lba_count,
  //        s.getState(), s.ok() ? "OK trivial" : "NOK trivial");
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
      iterators[iterator_index++] = vset_->znssstable_->NewIterator(
          0, **base_iter, vset_->icmp_.user_comparator());
    }
  }
  // LN
  int i = first_level_ == 0 ? 1 : 0;
  for (; i <= 1; i++) {
    iterators[iterator_index++] =
        new LNIterator(new LNZoneIterator(vset_->icmp_.user_comparator(),
                                          &targets_[i], first_level_ + i),
                       &GetLNIterator, vset_->znssstable_,
                       vset_->icmp_.user_comparator(), env_);
    // printf("Iterators... %d %lu\n", first_level_ + i, iterators_needed);
  }
  return NewMergingIterator(&vset_->icmp_, iterators, iterators_needed);
}

void ZnsCompaction::MarkStaleTargetsReusable(ZnsVersionEdit* edit) {
  for (int i = 0; i <= 1; i++) {
    std::vector<SSZoneMetaData*>::const_iterator base_iter =
        targets_[i].begin();
    std::vector<SSZoneMetaData*>::const_iterator base_end = targets_[i].end();
    if (base_iter == base_end) {
      continue;
    }
    uint64_t lba = (*base_iter)->L0.lba;
    uint64_t number = (*base_iter)->number;
    uint64_t count = 0;
    for (; base_iter != base_end; ++base_iter) {
      if (i == 0 && first_level_ > 0 && IsTrivialMove()) {
        // printf("No delete needed %u %u %d \n", i, i + first_level_,
        // IsTrivialMove());
        edit->RemoveSSDefinitionOnlyMeta(i + first_level_, *(*base_iter));
      } else {
        edit->RemoveSSDefinition(i + first_level_, *(*base_iter));
      }
      if ((*base_iter)->number < number) {
        number = (*base_iter)->number;
        lba = (*base_iter)->L0.lba;
      }
      count += (*base_iter)->lba_count;
    }

    // Setup deleted range when on L0
    if (i + first_level_ == 0) {
      // Carry over (move head of deleted range)
      std::pair<uint64_t, uint64_t> new_deleted_range;
      if (vset_->current_->ss_deleted_range_.first != 0) {
        new_deleted_range =
            std::make_pair(vset_->current_->ss_deleted_range_.first,
                           count + vset_->current_->ss_deleted_range_.second);
      } else {
        // No deleted range yet, so create one.
        new_deleted_range = std::make_pair(lba, count);
      }
      // printf("delete range %u %lu %lu \n", first_level_ + i,
      //        new_deleted_range.first, new_deleted_range.second);
      edit->AddDeletedRange(new_deleted_range);
    }
  }
}

void ZnsCompaction::DeferCompactionWrite(void* c) {
  DeferredLNCompaction* deferred = reinterpret_cast<DeferredLNCompaction*>(c);
  while (true) {
    // Make progress
    deferred->mutex_.Lock();
    if (deferred->index_ >= deferred->deferred_builds_.size()) {
      printf("Deferred awaiting new task\n");
      if (deferred->last_) {
        break;
      }
      deferred->new_task_.Wait();
    }
    SSTableBuilder* current_builder =
        deferred->deferred_builds_[deferred->index_];
    deferred->mutex_.Unlock();

    // The meat of the function
    printf("Deferred flush?\n");
    Status s = Status::OK();
    if (current_builder == nullptr) {
      printf("Deferred flush builder == nullptr");
      s = Status::Corruption();
    } else {
      s = current_builder->Flush();
    }
    // TODO: error must be stored in deferred data to propogate the issue.
    deferred->mutex_.Lock();
    if (!s.ok()) {
      printf("error writing table\n");
    } else {
      deferred->edit_->AddSSDefinition(deferred->level_,
                                       *(current_builder->GetMeta()));
      delete current_builder;
      deferred->deferred_builds_[deferred->index_] = nullptr;
    }
    // Acquire tasks
    deferred->index_++;
    printf("Deferred requesting new task\n");
    deferred->new_task_.SignalAll();
    deferred->mutex_.Unlock();
  }
  printf("Deferred done \n");
  deferred->done_ = true;
  deferred->new_task_.SignalAll();
  deferred->mutex_.Unlock();
}

Status ZnsCompaction::FlushSSTable(SSTableBuilder** builder,
                                   ZnsVersionEdit* edit, SSZoneMetaData* meta) {
  Status s = Status::OK();
  SSTableBuilder* current_builder = *builder;
  meta->number = vset_->NewSSNumber();
  s = current_builder->Finalise();

  if (ZnsConfig::compaction_allow_deferring_writes) {
    deferred_.mutex_.Lock();
    while (deferred_.deferred_builds_.size() > deferred_.index_ &&
           deferred_.deferred_builds_.size() - deferred_.index_ >
               ZnsConfig::compaction_maximum_deferred_writes) {
      // printf("Too many flushes, waiting\n");
      deferred_.new_task_.Wait();
    }
    // printf("Adding new deferred flush \n");
    deferred_.deferred_builds_.push_back(current_builder);
    deferred_.new_task_.SignalAll();
    deferred_.mutex_.Unlock();
    metas_.push_back(new SSZoneMetaData);
    current_builder = vset_->znssstable_->NewBuilder(first_level_ + 1,
                                                     metas_[metas_.size() - 1]);
  } else {
    // uint64_t before = clock_->NowMicros();
    s = current_builder->Flush();
    // printf("Time of flush to LN %lu \n", clock_->NowMicros() - before);
    if (s.ok()) {
      edit->AddSSDefinition(first_level_ + 1, *meta);
    }
    delete current_builder;
    current_builder = vset_->znssstable_->NewBuilder(first_level_ + 1, meta);
  }

  *builder = current_builder;
  if (!s.ok()) {
    printf("error writing table\n");
  }
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
  SSZoneMetaData meta;
  SSTableBuilder* builder;
  // Setup deferred thread
  if (ZnsConfig::compaction_allow_deferring_writes) {
    deferred_.edit_ = edit;
    deferred_.level_ = first_level_ + 1;
    // printf("scheduling\n");
    env_->Schedule(&ZnsCompaction::DeferCompactionWrite, &(this->deferred_),
                   rocksdb::Env::LOW);
    metas_.push_back(new SSZoneMetaData);
    builder = vset_->znssstable_->NewBuilder(first_level_ + 1,
                                             metas_[metas_.size() - 1]);
  } else {
    builder = vset_->znssstable_->NewBuilder(first_level_ + 1, &meta);
  }
  {
    {
      // uint64_t before = clock_->NowMicros();
      Iterator* merger = MakeCompactionIterator();
      // printf("Time to make iterator %lu \n", clock_->NowMicros() - before);
      merger->SeekToFirst();
      if (!merger->Valid()) {
        delete merger;
        printf("Invalid merger\n");
        return Status::Corruption("No valid merging iterator");
      }
      ParsedInternalKey ikey;
      std::string current_user_key;
      bool has_current_user_key = false;
      SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
      SequenceNumber min_seq = vset_->LastSequence();
      const Comparator* ucmp = vset_->icmp_.user_comparator();
      // before = clock_->NowMicros();
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
          // estimate if flush before would be better...
          uint64_t max_size = max_lba_count_;
          if ((builder->GetSize() + builder->EstimateSizeImpact(key, value) +
               vset_->lba_size_ - 1) /
                  vset_->lba_size_ >=
              max_size) {
            // printf("Time for LN merge %lu \n", clock_->NowMicros() -
            // before);
            if (ZnsConfig::compaction_allow_deferring_writes) {
              s = FlushSSTable(&builder, edit, metas_[metas_.size() - 1]);
            } else {
              s = FlushSSTable(&builder, edit, &meta);
            }
            // before = clock_->NowMicros();
            if (!s.ok()) {
              break;
            }
          }
          s = builder->Apply(key, value);
        }
      }
      if (s.ok() && builder->GetSize() > 0) {
        // printf("Time for LN merge %lu \n", clock_->NowMicros() - before);
        if (ZnsConfig::compaction_allow_deferring_writes) {
          s = FlushSSTable(&builder, edit, metas_[metas_.size() - 1]);
        } else {
          s = FlushSSTable(&builder, edit, &meta);
        }
      }
      // Shutdown write thread
      if (ZnsConfig::compaction_allow_deferring_writes) {
        deferred_.mutex_.Lock();
        deferred_.last_ = true;
        deferred_.new_task_.SignalAll();
        while (!deferred_.done_) {
          deferred_.new_task_.Wait();
          deferred_.mutex_.Unlock();
        }
        TROPODB_DEBUG("Deferred quiting \n");
      }
      delete merger;
    }
  }
  if (ZnsConfig::compaction_allow_deferring_writes) {
    for (int i = metas_.size() - 1; i >= 0; i--) {
      delete metas_[i];
    }
  }
  return s;
}
}  // namespace ROCKSDB_NAMESPACE
