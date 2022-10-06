// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/tropodb/index/tropodb_compaction.h"

#include <numeric>

#include "db/tropodb/tropodb_config.h"
#include "db/tropodb/index/tropodb_version.h"
#include "db/tropodb/index/tropodb_version_edit.h"
#include "db/tropodb/index/tropodb_version_set.h"
#include "db/tropodb/table/iterators/merging_iterator.h"
#include "db/tropodb/table/iterators/sstable_ln_iterator.h"
#include "db/tropodb/table/tropodb_sstable.h"
#include "db/tropodb/utils/tropodb_logger.h"
#include "rocksdb/slice.h"
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
  // Initialise array to 0s
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
      if (target->number == c0->number) {
        return true;
      }
    }
    for (const auto& c1 : targets_[1]) {
      if (target->number == c1->number) {
        return true;
      }
    }
  }
  return false;
}

void ZnsCompaction::GetCompactionTargets(std::vector<SSZoneMetaData*>* metas) {
  metas->clear();
  metas->insert(metas->end(), targets_[0].begin(), targets_[0].end());
  metas->insert(metas->end(), targets_[1].begin(), targets_[1].end());
}

static uint64_t LbasInSSTables(const std::vector<SSZoneMetaData*>& ss) {
  return std::accumulate(ss.begin(), ss.end(), 0,
                         [](int64_t sum, SSZoneMetaData* sstable) {
                           return sum + sstable->lba_count;
                         });
}

static uint64_t MaxGrandParentOverlapBytes(uint64_t lba_size) {
  return ZnsConfig::compaction_max_grandparents_overlapping_tables *
         (((ZnsConfig::max_bytes_sstable_ + lba_size - 1) / lba_size) *
          lba_size);
}

bool ZnsCompaction::IsTrivialMove() const {
  // A move is trivial if it requires no merging in the next level.
  // Unlesss, a move has many impacts on grandparents (level + 2), requiring
  // high costs later.
  return targets_[0].size() == 1 && targets_[1].size() == 0 &&
         LbasInSSTables(grandparents_) <=
             MaxGrandParentOverlapBytes(vset_->lba_size_);
}

Status ZnsCompaction::DoTrivialMove(ZnsVersionEdit* edit) {
  Status s = Status::OK();
  SSZoneMetaData* old_meta = targets_[0][0];
  SSZoneMetaData meta;
  s = vset_->znssstable_->CopySSTable(first_level_, first_level_ + 1, *old_meta,
                                      &meta);
  meta.number = vset_->NewSSNumber();
  if (!s.ok()) {
    return s;
  }
  edit->AddSSDefinition(first_level_ + 1, meta);
  return s;
}

void ZnsCompaction::MarkCompactedTablesAsDead(ZnsVersionEdit* edit) {
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
    // TODO: we no longer use this. remove
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
      edit->AddDeletedRange(new_deleted_range);
    }
  }
}

Iterator* ZnsCompaction::GetLNIterator(void* arg, const Slice& file_value,
                                       const Comparator* cmp) {
  ZNSSSTableManager* zns = reinterpret_cast<ZNSSSTableManager*>(arg);
  Iterator* iterator = zns->GetLNIterator(file_value, cmp);
  return iterator;
}

Iterator* ZnsCompaction::MakeCompactionIterator() {
  size_t iterators_needed = 0;
  // When first level = 0, we need an iterator for each target L0 SStable, else
  // only one LN.
  iterators_needed += first_level_ == 0 ? targets_[0].size() : 1;
  // The second level is always > L0, so 1 iterator suffices
  iterators_needed += 1;
  // Variables to hold the iterators
  Iterator** iterators = new Iterator*[iterators_needed];
  size_t iterator_index = 0;

  // Add L0 iterators
  if (first_level_ == 0) {
    const std::vector<SSZoneMetaData*>& l0ss = targets_[0];
    std::vector<SSZoneMetaData*>::const_iterator base_iter = l0ss.begin();
    std::vector<SSZoneMetaData*>::const_iterator base_end = l0ss.end();
    for (; base_iter != base_end; ++base_iter) {
      iterators[iterator_index++] = vset_->znssstable_->NewIterator(
          0, **base_iter, vset_->icmp_.user_comparator());
    }
  }
  // Add LN iterators
  for (int i = first_level_ == 0 ? 1 : 0; i <= 1; i++) {
    iterators[iterator_index++] =
        new LNIterator(new LNZoneIterator(vset_->icmp_.user_comparator(),
                                          &targets_[i], first_level_ + i),
                       &GetLNIterator, vset_->znssstable_,
                       vset_->icmp_.user_comparator(), env_);
  }
  // Merge all iterators together
  return NewMergingIterator(&vset_->icmp_, iterators, iterators_needed);
}

void ZnsCompaction::DeferCompactionWrite(void* deferred_compaction) {
  DeferredLNCompaction* deferred =
      reinterpret_cast<DeferredLNCompaction*>(deferred_compaction);
  while (true) {
    // Wait for task
    deferred->mutex_.Lock();
    if (deferred->index_ >= deferred->deferred_builds_.size()) {
      // Host asked the defer thread to die, so die.
      if (deferred->last_) {
        break;
      }
      deferred->new_task_.Wait();
    }

    // Set current task
    SSTableBuilder* current_builder =
        deferred->deferred_builds_[deferred->index_];
    deferred->mutex_.Unlock();

    // Process task
    Status s = Status::OK();
    if (current_builder == nullptr) {
      TROPODB_ERROR("ERROR: Deferred flush: current builder == nullptr");
      s = Status::Corruption();
    } else {
      s = current_builder->Flush();
    }
    // TODO: error must be stored in deferred data to propogate the issue.

    // Add to (potential) version structure
    deferred->mutex_.Lock();
    if (!s.ok()) {
      TROPODB_ERROR("ERROR: Deferred flush: error writing table\n");
    } else {
      deferred->edit_->AddSSDefinition(deferred->level_,
                                       *(current_builder->GetMeta()));
      delete current_builder;
      deferred->deferred_builds_[deferred->index_] = nullptr;
    }

    // Finish task
    deferred->index_++;
    deferred->new_task_.SignalAll();
    deferred->mutex_.Unlock();
  }
  // Die
  deferred->done_ = true;
  deferred->new_task_.SignalAll();
  deferred->mutex_.Unlock();
}

Status ZnsCompaction::FlushSSTable(SSTableBuilder** builder,
                                   ZnsVersionEdit* edit, SSZoneMetaData* meta) {
  Status s = Status::OK();
  // Setup flush task
  SSTableBuilder* current_builder = *builder;
  meta->number = vset_->NewSSNumber();
  s = current_builder->Finalise();
  if (!s.ok()) {
    TROPODB_ERROR("ERROR: Compaction: Error creating flush task\n");
  }

  // Either defer or block current thread and do it now
  if (ZnsConfig::compaction_allow_deferring_writes) {
    // It is possible the deferred threads mailbox is full, be polite and wait.
    deferred_.mutex_.Lock();
    while (deferred_.deferred_builds_.size() > deferred_.index_ &&
           deferred_.deferred_builds_.size() - deferred_.index_ >
               ZnsConfig::compaction_maximum_deferred_writes) {
      deferred_.new_task_.Wait();
    }

    // Push task to deferred thread
    deferred_.deferred_builds_.push_back(current_builder);
    deferred_.new_task_.SignalAll();
    deferred_.mutex_.Unlock();
    metas_.push_back(new SSZoneMetaData);

    // Create a new task to do in the main thread
    current_builder = vset_->znssstable_->NewSSTableBuilder(
        first_level_ + 1, metas_[metas_.size() - 1]);
    *builder = current_builder;
    return s;
  } else {
    // Flush manually
    s = current_builder->Flush();
    if (s.ok()) {
      edit->AddSSDefinition(first_level_ + 1, *meta);
    } else {
      TROPODB_ERROR("ERROR: Compaction: Error writing table\n");
    }

    // Cleanup our work and create a new task.
    delete current_builder;
    current_builder =
        vset_->znssstable_->NewSSTableBuilder(first_level_ + 1, meta);
    *builder = current_builder;
    return s;
  }
}

bool ZnsCompaction::IsBaseLevelForKey(const Slice& user_key) {
  // Look if the key has potential to live further in the tree.
  const Comparator* user_cmp = vset_->icmp_.user_comparator();
  for (size_t lvl = first_level_ + 1; lvl < ZnsConfig::level_count; lvl++) {
    const std::vector<SSZoneMetaData*>& sstables = vset_->current_->ss_[lvl];
    while (level_ptrs_[lvl] < sstables.size()) {
      SSZoneMetaData* m = sstables[level_ptrs_[lvl]];
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
  Status s = Status::OK();
  SSZoneMetaData meta;
  SSTableBuilder* builder;

  // Spawn deferred thread (if we have enabled it)
  if (ZnsConfig::compaction_allow_deferring_writes) {
    deferred_.edit_ = edit;
    deferred_.level_ = first_level_ + 1;
    env_->Schedule(&ZnsCompaction::DeferCompactionWrite, &(this->deferred_),
                   rocksdb::Env::LOW);
    // Setup our joblist
    metas_.push_back(new SSZoneMetaData);
    builder = vset_->znssstable_->NewSSTableBuilder(first_level_ + 1,
                                                    metas_[metas_.size() - 1]);
  } else {
    // Setup our job(list). We can only process one at the same time without
    // deferred.
    builder = vset_->znssstable_->NewSSTableBuilder(first_level_ + 1, &meta);
  }

  // Setup SSTable iterator
  Iterator* merger;
  {
    merger = MakeCompactionIterator();
    merger->SeekToFirst();
    if (!merger->Valid()) {
      delete merger;
      TROPODB_ERROR("ERROR: Compaction: Merging iterator invalid\n");
      return Status::Corruption("No valid merging iterator");
    }
  }

  // Iterate over SSTable iterator, merge and write
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  SequenceNumber min_seq = vset_->LastSequence();
  const Comparator* ucmp = vset_->icmp_.user_comparator();
  {
    // K-way Merge-sort old SSTables and write new SSTables
    for (; merger->Valid(); merger->Next()) {
      const Slice& key = merger->key();
      const Slice& value = merger->value();
      bool drop = false;

      // verify if key is valid or should be dropped
      if (!ParseInternalKey(key, &ikey, false).ok()) {
        // Do not hide error keys
        current_user_key.clear();
        has_current_user_key = false;
        last_sequence_for_key = kMaxSequenceNumber;
        TROPODB_ERROR("ERROR: Compaction: Invalid key found\n");
      } else {
        if (!has_current_user_key ||
            ucmp->Compare(ikey.user_key, Slice(current_user_key)) != 0) {
          // first occurrence of this user key
          current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
          has_current_user_key = true;
          last_sequence_for_key = kMaxSequenceNumber;
        }
        // Check if key is "shadowed" and can be dropped
        if (last_sequence_for_key <= min_seq) {
          drop = true;
        } else if (ikey.type == kTypeDeletion && ikey.sequence <= min_seq &&
                   IsBaseLevelForKey(ikey.user_key)) {
          drop = true;
        }
      }
      // Ensure that next iteration we can recognise the key
      last_sequence_for_key = ikey.sequence;

      // Add key to "new" tables or drop key
      if (drop) {
      } else {
        // Estimate impact on size and determine if we need to flush first
        uint64_t max_size = max_lba_count_;
        if ((builder->GetSize() + builder->EstimateSizeImpact(key, value) +
             vset_->lba_size_ - 1) /
                vset_->lba_size_ >=
            max_size) {
          // Flush
          if (ZnsConfig::compaction_allow_deferring_writes) {
            s = FlushSSTable(&builder, edit, metas_[metas_.size() - 1]);
          } else {
            s = FlushSSTable(&builder, edit, &meta);
          }
          if (!s.ok()) {
            TROPODB_ERROR("ERROR: Compaction: Could not flush\n");
            break;
          }
        }
        // Only now add key to SSTable
        s = builder->Apply(key, value);
      }
    }

    // Now write the last remaining SSTable to storage
    if (s.ok() && builder->GetSize() > 0) {
      if (ZnsConfig::compaction_allow_deferring_writes) {
        s = FlushSSTable(&builder, edit, metas_[metas_.size() - 1]);
      } else {
        s = FlushSSTable(&builder, edit, &meta);
      }
      if (!s.ok()) {
        TROPODB_ERROR("ERROR: Compaction: Could not flush last SSTable\n");
      }
    }
  }

  // Shutdown deffered thread
  {
    if (ZnsConfig::compaction_allow_deferring_writes) {
      deferred_.mutex_.Lock();
      deferred_.last_ = true;
      deferred_.new_task_.SignalAll();
      while (!deferred_.done_) {
        deferred_.new_task_.Wait();
        deferred_.mutex_.Unlock();
      }
      TROPODB_DEBUG("Deferred quiting \n");
      for (int i = metas_.size() - 1; i >= 0; i--) {
        delete metas_[i];
      }
    }
  }

  // Cleanup
  delete merger;
  return s;
}
}  // namespace ROCKSDB_NAMESPACE
