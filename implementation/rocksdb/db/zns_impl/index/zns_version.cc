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
      compaction_level_(ZnsConfig::level_count + 1) {}

ZnsVersion::~ZnsVersion() {
  printf("Deleting version structure.\n");
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
}

void ZnsVersion::Clear() {}

Status ZnsVersion::Get(const ReadOptions& options, const LookupKey& lkey,
                       std::string* value) {
  Status s;
  const Comparator* ucmp = vset_->icmp_.user_comparator();
  ZNSSSTableManager* znssstable = vset_->znssstable_;
  Slice key = lkey.user_key();
  Slice internal_key = lkey.internal_key();
  znssstable->Ref();

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
  EntryStatus status;
  if (!tmp.empty()) {
    std::sort(tmp.begin(), tmp.end(), [](SSZoneMetaData* a, SSZoneMetaData* b) {
      return a->number > b->number;
    });
    for (uint32_t i = 0; i < tmp.size(); i++) {
      s = vset_->table_cache_->Get(options, tmp[i], 0, internal_key, value,
                                   &status);
      // s = znssstable->Get(0, vset_->icmp_, internal_key, value, tmp[i],
      //                     &status);
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
    uint32_t index = FindSS(vset_->icmp_, ss_[level], internal_key);
    if (index >= num_ss) {
      continue;
    }
    SSZoneMetaData* m = ss_[level][index];
    if (ucmp->Compare(key, m->smallest.user_key()) >= 0 &&
        ucmp->Compare(key, m->largest.user_key()) <= 0) {
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

  znssstable->Unref();
  return Status::NotFound("No matching table");
}
}  // namespace ROCKSDB_NAMESPACE
