// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/zns_impl/index/zns_version.h"

#include "db/zns_impl/index/zns_version_set.h"
#include "db/zns_impl/table/iterators/merging_iterator.h"
#include "db/zns_impl/table/iterators/sstable_ln_iterator.h"
#include "db/zns_impl/table/zns_sstable.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

ZnsVersion::ZnsVersion() { Clear(); }

ZnsVersion::ZnsVersion(ZnsVersionSet* vset) : vset_(vset) {}

ZnsVersion::~ZnsVersion() {
  printf("Deleting version structure.\n");
  assert(refs_ == 0);
  for (int level = 0; level < 7; level++) {
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

Status ZnsVersion::Get(const ReadOptions& options, const Slice& key,
                       std::string* value) {
  Status s;
  const Comparator* ucmp = vset_->icmp_.user_comparator();
  ZNSSSTableManager* znssstable = vset_->znssstable_;
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
      s = znssstable->Get(0, key, value, tmp[i], &status);
      if (s.ok()) {
        znssstable->Unref();
        if (status != EntryStatus::found) {
          return Status::NotFound();
        }
        return s;
      }
    }
  }

  // Other levels
  for (int level = 1; level < 7; ++level) {
    size_t num_ss = ss_[level].size();
    if (num_ss == 0) continue;
    // TODO: binary search
    for (size_t i = ss_[level].size(); i != 0; --i) {
      uint32_t index = FindSS(vset_->icmp_, ss_[level], key);
      if (index >= num_ss) {
        continue;
      }
      SSZoneMetaData* z = ss_[level][index];
      if (ucmp->Compare(key, z->smallest.user_key()) >= 0 &&
          ucmp->Compare(key, z->largest.user_key()) <= 0) {
        s = znssstable->Get(level, key, value, z, &status);
        if (s.ok()) {
          znssstable->Unref();
          if (status != EntryStatus::found) {
            return Status::NotFound();
          }
          return s;
        }
      }
    }
  }

  znssstable->Unref();
  return Status::NotFound();
}
}  // namespace ROCKSDB_NAMESPACE
