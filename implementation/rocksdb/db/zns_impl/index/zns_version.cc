// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/zns_impl/index/zns_version.h"

#include "db/zns_impl/config.h"
#include "db/zns_impl/index/zns_version_set.h"
#include "db/zns_impl/table/iterators/merging_iterator.h"
#include "db/zns_impl/table/iterators/sstable_ln_iterator.h"
#include "db/zns_impl/table/zns_sstable.h"
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
  // Drop refs
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
  // printf("Removed version %lu\n", debug_nr_);
}

void ZnsVersion::Clear() {}

Status ZnsVersion::Get(const ReadOptions& options, const LookupKey& lkey,
                       std::string* value) {
  Status s;
  EntryStatus status;
  const Comparator* ucmp = vset_->icmp_.user_comparator();
  ZNSSSTableManager* znssstable = vset_->znssstable_;
  Slice key = lkey.user_key();
  Slice internal_key = lkey.internal_key();
  znssstable->Ref();

  // int in_range = -1;

  // L0 (no sorting of L0 yet, because it is an append-only log. Earlier zones
  // are guaranteed to be older). So start from end to begin.
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
    // in_range = 0;
    std::sort(tmp.begin(), tmp.end(), [](SSZoneMetaData* a, SSZoneMetaData* b) {
      return a->number > b->number;
    });
    for (uint32_t i = 0; i < tmp.size(); i++) {
      s = vset_->table_cache_->Get(options, *tmp[i], 0, internal_key, value,
                                   &status);
      if (s.ok()) {
        if (status == EntryStatus::notfound) {
          continue;
        }
        znssstable->Unref();
        return status == EntryStatus::found ? s
                                            : Status::NotFound("Entry deleted");
      }
    }
  }

  // Other levels
  for (uint8_t level = 1; level < ZnsConfig::level_count; ++level) {
    size_t num_ss = ss_[level].size();
    if (num_ss == 0) continue;
    uint32_t index = ZNSSSTableManager::FindSSTableIndex(
        vset_->icmp_.user_comparator(), ss_[level], internal_key);
    if (index >= num_ss) {
      continue;
    }
    const SSZoneMetaData& m = *ss_[level][index];
    if (ucmp->Compare(key, m.smallest.user_key()) >= 0 &&
        ucmp->Compare(key, m.largest.user_key()) <= 0) {
      // in_range = level;
      s = vset_->table_cache_->Get(options, m, level, internal_key, value,
                                   &status);
      // s = znssstable->Get(level, vset_->icmp_, internal_key, value, m,
      // &status);
      if (s.ok()) {
        if (status != EntryStatus::found) {
          continue;
        }
        znssstable->Unref();
        return status == EntryStatus::found ? s
                                            : Status::NotFound("Entry deleted");
      }
    }
  }
  // printf("not found, but was in range %d\n", in_range);
  znssstable->Unref();
  return Status::NotFound("No matching table");
}

Iterator* ZnsVersion::GetLNIterator(void* arg, const Slice& file_value,
                                    const Comparator* cmp) {
  ZNSSSTableManager* zns = reinterpret_cast<ZNSSSTableManager*>(arg);
  SSZoneMetaData meta;
  meta.LN.lba_regions = DecodeFixed8(file_value.data());
  for (size_t i = 0; i < meta.LN.lba_regions; i++) {
    meta.LN.lbas[i] = DecodeFixed64(file_value.data() + 1 + 16 * i);
    meta.LN.lba_region_sizes[i] = DecodeFixed64(file_value.data() + 9 + 16 * i);
  }
  uint64_t lba_count =
      DecodeFixed64(file_value.data() + 1 + 16 * meta.LN.lba_regions);
  uint8_t level =
      DecodeFixed8(file_value.data() + 9 + 16 * meta.LN.lba_regions);
  meta.lba_count = lba_count;
  Iterator* iterator = zns->NewIterator(level, std::move(meta), cmp);
  return iterator;
}

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
      iters->push_back(new LNIterator(
          new LNZoneIterator(vset_->icmp_.user_comparator(), &ss_[level],
                             level),
          &GetLNIterator, vset_->znssstable_, vset_->icmp_.user_comparator()));
    }
  }
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
    const Slice file_start = m->smallest.user_key();
    const Slice file_limit = m->largest.user_key();
    if (begin != nullptr && user_cmp->Compare(file_limit, user_begin) < 0) {
      // "f" is completely before specified range; skip it
    } else if (end != nullptr && user_cmp->Compare(file_start, user_end) > 0) {
      // "f" is completely after specified range; skip it
    } else {
      inputs->push_back(m);
      if (level == 0) {
        // Level-0 files may overlap each other.  So check if the newly
        // added file has expanded the range.  If so, restart search.
        if (begin != nullptr && user_cmp->Compare(file_start, user_begin) < 0) {
          user_begin = file_start;
          inputs->clear();
          i = 0;
        } else if (end != nullptr &&
                   user_cmp->Compare(file_limit, user_end) > 0) {
          user_end = file_limit;
          inputs->clear();
          i = 0;
        }
      }
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE
