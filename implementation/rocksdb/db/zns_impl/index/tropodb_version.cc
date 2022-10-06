// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/zns_impl/index/tropodb_version.h"

#include "db/zns_impl/tropodb_config.h"
#include "db/zns_impl/index/tropodb_version_set.h"
#include "db/zns_impl/table/iterators/merging_iterator.h"
#include "db/zns_impl/table/iterators/sstable_ln_iterator.h"
#include "db/zns_impl/table/zns_sstable.h"
#include "db/zns_impl/utils/tropodb_logger.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

ZnsVersion::ZnsVersion() { Clear(); }

ZnsVersion::ZnsVersion(ZnsVersionSet* vset)
    : vset_(vset),
      next_(this),
      prev_(this),
      compaction_score_(-1),
      compaction_level_(ZnsConfig::level_count + 1),
      debug_nr_(0) {}

ZnsVersion::~ZnsVersion() {
  assert(refs_ == 0);
  // Remove from linked list
  prev_->next_ = next_;
  next_->prev_ = prev_;
  // Drop all refs
  for (uint8_t level = 0; level < ZnsConfig::level_count; level++) {
    for (size_t i = 0; i < ss_[level].size(); i++) {
      SSZoneMetaData* m = ss_[level][i];
      assert(m->refs > 0);
      m->refs--;
      if (m->refs <= 0) {
        delete m;
      }
    }
  }
  TROPODB_DEBUG("DEBUG: Removed version structure %lu\n", debug_nr_);
}

// TODO: Remove?
void ZnsVersion::Clear() {}

Status ZnsVersion::Get(const ReadOptions& options, const LookupKey& lkey,
                       std::string* value) {
  Status call_status;
  EntryStatus entry_status;
  const Comparator* ucmp = vset_->icmp_.user_comparator();
  ZNSSSTableManager* znssstable = vset_->znssstable_;
  znssstable->Ref();
  Slice key = lkey.user_key();
  Slice internal_key = lkey.internal_key();

  // Look in L0
  {
    // For some reason SSTables are not always sorted correctly.
    // Solve by finding all SSTables in range and then sorting them on recency.
    // TODO: ensure it is already in order elsewhere
    std::vector<SSZoneMetaData*> tmp;
    tmp.reserve(ss_[0].size());
    for (size_t i = ss_[0].size(); i != 0; --i) {
      SSZoneMetaData* z = ss_[0][i - 1];
      if (ucmp->Compare(key, z->smallest.user_key()) >= 0 &&
          ucmp->Compare(key, z->largest.user_key()) <= 0) {
        tmp.push_back(z);
      }
    }
    if (!tmp.empty()) {
      std::sort(tmp.begin(), tmp.end(),
                [](SSZoneMetaData* a, SSZoneMetaData* b) {
                  if (a->L0.number > b->L0.number) {
                    return true;
                  } else if (a->L0.number < b->L0.number) {
                    return false;
                  } else {
                    return a->number > b->number;
                  }
                });
      // Look for entry in sorted entries
      for (uint32_t i = 0; i < tmp.size(); i++) {
        // Look for value in L0 SSTable (table will get cached)
        call_status = vset_->table_cache_->Get(
            options, *tmp[i], 0, internal_key, value, &entry_status);
        if (call_status.ok()) {
          // Not in this SSTable, move on
          if (entry_status == EntryStatus::notfound) {
            continue;
          }
          // Entry found, clean and return
          znssstable->Unref();
          return entry_status == EntryStatus::found
                     ? call_status
                     : Status::NotFound("Entry deleted");
        }
      }
    }
  }

  // Look in LN
  {
    for (uint8_t level = 1; level < ZnsConfig::level_count; ++level) {
      size_t sstable_nrs = ss_[level].size();
      // level is empty
      if (sstable_nrs == 0) continue;

      // In LN only ONE table can overlap for each level, find this table
      uint32_t index = ZNSSSTableManager::FindSSTableIndex(
          vset_->icmp_.user_comparator(), ss_[level], internal_key);
      // No SSTable in range
      if (index >= sstable_nrs) {
        continue;
      }

      // Look if entry is in this SSTable (table will get cached)
      const SSZoneMetaData& m = *ss_[level][index];
      if (ucmp->Compare(key, m.smallest.user_key()) >= 0 &&
          ucmp->Compare(key, m.largest.user_key()) <= 0) {
        call_status = vset_->table_cache_->Get(options, m, level, internal_key,
                                               value, &entry_status);
        if (call_status.ok()) {
          // Key is not in this SSTable, move on
          if (entry_status == EntryStatus::notfound) {
            continue;
          }
          // Entry found, clean and return
          znssstable->Unref();
          return entry_status == EntryStatus::found
                     ? call_status
                     : Status::NotFound("Entry deleted");
        }
      }
    }
  }

  // Entry has not been found
  znssstable->Unref();
  return Status::NotFound("No matching table");
}

Iterator* ZnsVersion::GetLNIterator(void* arg, const Slice& file_value,
                                    const Comparator* cmp) {
  return reinterpret_cast<ZNSSSTableManager*>(arg)->GetLNIterator(file_value,
                                                                  cmp);
}

void ZnsVersion::GetOverlappingInputs(uint8_t level, const InternalKey* begin,
                                      const InternalKey* end,
                                      std::vector<SSZoneMetaData*>* inputs) {
  assert(level >= 0);
  assert(level < ZnsConfig::level_count);
  inputs->clear();
  Slice user_begin, user_end;
  if (begin != nullptr) {
    user_begin = begin->user_key();
  }
  if (end != nullptr) {
    user_end = end->user_key();
  }
  const Comparator* user_cmp = vset_->icmp_.user_comparator();
  for (size_t i = 0; i < ss_[level].size();) {
    SSZoneMetaData* m = ss_[level][i++];
    const Slice sstable_start = m->smallest.user_key();
    const Slice sstable_limit = m->largest.user_key();
    if (begin != nullptr && user_cmp->Compare(sstable_limit, user_begin) < 0) {
      // "m" is completely before specified range; skip it
    } else if (end != nullptr &&
               user_cmp->Compare(sstable_start, user_end) > 0) {
      // "m" is completely after specified range; skip it
    } else {
      inputs->push_back(m);
      if (level == 0) {
        // Level-0 files may overlap each other.  So check if the newly
        // added file has expanded the range.  If so, restart search.
        if (begin != nullptr &&
            user_cmp->Compare(sstable_start, user_begin) < 0) {
          user_begin = sstable_start;
          inputs->clear();
          i = 0;
        } else if (end != nullptr &&
                   user_cmp->Compare(sstable_limit, user_end) > 0) {
          user_end = sstable_limit;
          inputs->clear();
          i = 0;
        }
      }
    }
  }
}

// FIXME: Do not use this function! It is broken and will not be maintained
void ZnsVersion::AddIterators(const ReadOptions& options,
                              std::vector<Iterator*>* iters) {
  // Merge all level zero files together since they may overlap
  for (size_t i = 0; i < ss_[0].size(); i++) {
    iters->push_back(vset_->znssstable_->NewIterator(
        0, *ss_[0][i], vset_->icmp_.user_comparator()));
  }

  // For levels > 0, we can use a concatenating iterator that sequentially
  // walks through the non-overlapping files in the level, opening them
  // lazily.
  for (int level = 1; level < ZnsConfig::level_count; level++) {
    if (!ss_[level].empty()) {
      iters->push_back(
          new LNIterator(new LNZoneIterator(vset_->icmp_.user_comparator(),
                                            &ss_[level], level),
                         &GetLNIterator, vset_->znssstable_,
                         vset_->icmp_.user_comparator(), nullptr));
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE
