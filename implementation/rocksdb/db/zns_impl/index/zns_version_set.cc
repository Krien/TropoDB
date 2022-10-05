// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/zns_impl/index/zns_version_set.h"

#include "db/zns_impl/config.h"
#include "db/zns_impl/index/zns_compaction.h"
#include "db/zns_impl/index/zns_version.h"
#include "db/zns_impl/index/zns_version_edit.h"
#include "db/zns_impl/table/iterators/merging_iterator.h"
#include "db/zns_impl/table/iterators/sstable_ln_iterator.h"
#include "db/zns_impl/table/zns_sstable.h"
#include "port/port.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "db/zns_impl/utils/tropodb_logger.h"

namespace ROCKSDB_NAMESPACE {
ZnsVersionSet::ZnsVersionSet(const InternalKeyComparator& icmp,
                             ZNSSSTableManager* znssstable,
                             ZnsManifest* manifest, const uint64_t lba_size,
                             uint64_t zone_cap, ZnsTableCache* table_cache,
                             Env* env)
    : dummy_versions_(this),
      current_(nullptr),
      icmp_(icmp),
      znssstable_(znssstable),
      manifest_(manifest),
      lba_size_(lba_size),
      zone_cap_(zone_cap),
      ss_number_(0),
      logged_(false),
      table_cache_(table_cache),
      env_(env) {
  AppendVersion(new ZnsVersion(this));
};

ZnsVersionSet::~ZnsVersionSet() {
  current_->Unref();
  assert(dummy_versions_.next_ == &dummy_versions_);
}

void ZnsVersionSet::AppendVersion(ZnsVersion* v) {
  assert(v->Getref() == 0);
  assert(v != current_);
  if (current_ != nullptr) {
    current_->Unref();
  }
  current_ = v;
  v->Ref();

  // Append to linked list
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
  // TODO: Add some define around, this should not be in prod.
  static uint64_t debug_number = 0;
  debug_number++;
  v->debug_nr_ = debug_number;
  // printf("added version %lu \n", debug_number);
}

void ZnsVersionSet::GetLiveZones(const uint8_t level,
                                 std::set<uint64_t>& live) {
  for (ZnsVersion* v = dummy_versions_.next_; v != &dummy_versions_;
       v = v->next_) {
    v->Ref();
    const std::vector<SSZoneMetaData*>& metas = v->ss_[level];
    for (auto& meta : metas) {
      live.insert(meta->number);
    }
    v->Unref();
  }
}

void ZnsVersionSet::GetSaveDeleteRange(const uint8_t level,
                                       std::pair<uint64_t, uint64_t>* range) {
  *range = std::make_pair<uint64_t, uint64_t>(0, 0);
  bool first = true;
  for (ZnsVersion* v = dummy_versions_.next_; v != &dummy_versions_;
       v = v->next_) {
    // printf("V %lu %lu %lu %lu\n", v->debug_nr_, v->ss_deleted_range_.first,
    //        v->ss_deleted_range_.second, v->ss_[0].size());
    // There can be a couple of cases:
    //  1. The version does not have a deleted range (0,0). skip
    //  2. The version starts at a different range than current_. Immediately
    //  return, we can not delete. This happens when a reader uses an old
    //  version.
    //  3. The version has a smaller range. Pick this one as we can only delete
    //  the smallest range.
    if (current_->ss_deleted_range_.first != v->ss_deleted_range_.first &&
        v->ss_deleted_range_.first != 0) {
      *range = std::make_pair<uint64_t, uint64_t>(0, 0);
      return;
    }
    if (first || v->ss_deleted_range_.second < range->second) {
      *range = v->ss_deleted_range_;
      first = false;
      // printf("potential %lu %lu \n", v->ss_deleted_range_.first,
      //        v->ss_deleted_range_.second);
    }
  }
}

Status ZnsVersionSet::ReclaimStaleSSTablesL0(port::Mutex* mutex_,
                                             port::CondVar* cond) {
  // printf("reclaiming....\n");
  Status s = Status::OK();
  ZnsVersionEdit edit;

  // Reclaim L0
  // Get all of the files that can be deleted as no reader uses it.
  std::set<uint64_t> live_zones;
  GetLiveZones(0, live_zones);
  std::vector<SSZoneMetaData*> new_deleted_ss_l0;
  std::vector<SSZoneMetaData*> tmp;
  for (size_t j = 0; j < current_->ss_d_[0].size(); j++) {
    SSZoneMetaData* todelete = current_->ss_d_[0][j];
    if (live_zones.count(todelete->number) == 0) {
      // printf("Safe to delete %lu %lu %lu\n", todelete->number,
      // todelete->L0.lba,
      //        todelete->lba_count);
      tmp.push_back(todelete);
    } else {
      new_deleted_ss_l0.push_back(todelete);
      // printf("we can not delete %lu\n", todelete->number);
    }
  }
  // Sort on circular log order
  printf("Safe to delete %lu \n", tmp.size());
  if (!tmp.empty()) {
    // in_range = 0;
    std::sort(tmp.begin(), tmp.end(), [](SSZoneMetaData* a, SSZoneMetaData* b) {
      return a->number < b->number;
    });
    // safe, but ONLY because L0 thread is the only one adding and deleting from
    // L0.
    mutex_->Unlock();
    s = znssstable_->DeleteL0Table(tmp, new_deleted_ss_l0);
    mutex_->Lock();
    current_->ss_d_[0].clear();
    current_->ss_d_[0] = new_deleted_ss_l0;

    if (!s.ok()) {
      printf("error reclaiming L0\n");
      return s;
    }
  }
  printf("CUR DEL %lu  %lu \n", current_->ss_d_[0].size(),
         current_->ss_[0].size());
  s = LogAndApply(&edit);
  printf("DONE reclaiming \n");
  return s;
}

Status ZnsVersionSet::ReclaimStaleSSTablesLN(port::Mutex* mutex_,
                                             port::CondVar* cond) {
  // printf("reclaiming....\n");
  Status s = Status::OK();
  ZnsVersionEdit edit;
  for (uint8_t i = 1; i < ZnsConfig::level_count; i++) {
    std::set<uint64_t> live_zones;
    std::vector<SSZoneMetaData*> new_deleted;
    std::vector<SSZoneMetaData*> to_delete;
    GetLiveZones(i, live_zones);
    for (size_t j = 0; j < current_->ss_d_[i].size(); j++) {
      // printf("  deleting ln %lu \n", current_->ss_d_[i][j]->number);
      SSZoneMetaData* todelete = current_->ss_d_[i][j];
      if (live_zones.count(todelete->number) != 0) {
        // printf("Alive can not be deleted: %lu \n",
        // current_->ss_d_[i][j]->number);
        new_deleted.push_back(todelete);
      } else {
        to_delete.push_back(todelete);
      }
    }
    // safe, but ONLY because LN thread is the only one adding and deleting from
    // LN.
    mutex_->Unlock();
    for (auto del : to_delete) {
      s = znssstable_->DeleteLNTable(i, *del);
      if (!s.ok()) {
        printf("Error deleting ln table \n");
        return s;
      }
    }
    mutex_->Lock();
    current_->ss_d_[i] = new_deleted;
  }
  s = LogAndApply(&edit);
  // printf("DONE reclaiming \n");
  return s;
}

Status ZnsVersionSet::WriteSnapshot(std::string* snapshot_dst,
                                    ZnsVersion* version) {
  ZnsVersionEdit edit;
  edit.SetComparatorName(icmp_.user_comparator()->Name());
  // compaction stuff
  for (uint8_t level = 0; level < ZnsConfig::level_count; level++) {
    const std::vector<SSZoneMetaData*>& ss = version->ss_[level];
    for (size_t i = 0; i < ss.size(); i++) {
      const SSZoneMetaData& m = *ss[i];
      edit.AddSSDefinition(level, m);
    }
    std::string& compact_ptr = compact_pointer_[level];
    InternalKey ikey;
    ikey.DecodeFrom(compact_ptr);
    edit.SetCompactPointer(level, ikey);
  }
  // Deleted range
  edit.AddDeletedRange(version->ss_deleted_range_);
  // Deleted zones
  for (uint8_t level = 0; level < ZnsConfig::level_count; level++) {
    for (auto del : version->ss_d_[level]) {
      edit.AddDeletedSSTable(level, *del);
    }
  }
  // Fragmented logs
  std::string data = znssstable_->GetRecoveryData();
  Slice sdata = Slice(data.data(), data.size());
  edit.AddFragmentedData(sdata);

  edit.SetLastSequence(last_sequence_);
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
  s = CommitVersion(v, znssstable_);
  // Installing?
  if (s.ok()) {
    AppendVersion(v);
  }
  RecalculateScore();
  return s;
}

void ZnsVersionSet::RecalculateScore() {
  ZnsVersion* v = current_;
  uint8_t best_level = ZnsConfig::level_count + 1;
  double best_score = -1;
  double score = 0;
  // TODO: This is probably a design flaw. This is uninformed and might cause
  // all sorts of holes and early compactions.
  for (size_t i = 1; i < ZnsConfig::level_count - 1; i++) {
    if (static_cast<double>(znssstable_->GetBytesInLevel(current_->ss_[i])) >
        ZnsConfig::ss_compact_treshold[i]) {
      score =
          (static_cast<double>(znssstable_->GetBytesInLevel(current_->ss_[i])) /
           ZnsConfig::ss_compact_treshold[i]) *
          ZnsConfig::ss_compact_modifier[i];
    } else {
      score = 0;
    }
    if (score > best_score) {
      best_score = score;
      best_level = i;
      if (best_level > 0) {
        // printf("Score %f from level %d %lu %lu %f \n", best_score,
        // best_level,
        //        current_->ss_[i].size(),
        //        znssstable_->GetBytesInLevel(current_->ss_[i]),
        //        ZnsConfig::ss_compact_treshold[i]);
      }
    }
  }
  v->compaction_level_ = best_level;
  v->compaction_score_ = best_score;
}

Status ZnsVersionSet::CommitVersion(ZnsVersion* v, ZNSSSTableManager* man) {
  Status s;
  // Setup version (for now CoW)
  std::string version_body;
  s = WriteSnapshot(&version_body, v);
  std::string version_data;
  PutVarint32(&version_data, static_cast<uint32_t>(ZnsCommitTag::kEdit));
  PutLengthPrefixedSlice(&version_data, version_body);
  // Padding
  std::string closer;
  PutVarint32(&closer, static_cast<uint32_t>(ZnsCommitTag::kClosing));
  // Write
  Slice result = version_data.append(closer);
  uint64_t current_lba;
  s = manifest_->NewManifest(result);
  if (s.ok()) {
    s = manifest_->SetCurrent();
  }
  if (!s.ok()) {
    printf("Error commiting\n");
  }
  return s;
}

// Stores the minimal range that covers all entries in inputs in
// *smallest, *largest.
// REQUIRES: inputs is not empty
void ZnsVersionSet::GetRange(const std::vector<SSZoneMetaData*>& inputs,
                             InternalKey* smallest, InternalKey* largest) {
  assert(!inputs.empty());
  smallest->Clear();
  largest->Clear();
  for (size_t i = 0; i < inputs.size(); i++) {
    SSZoneMetaData* m = inputs[i];
    if (i == 0) {
      *smallest = m->smallest;
      *largest = m->largest;
    } else {
      if (icmp_.Compare(m->smallest, *smallest) < 0) {
        *smallest = m->smallest;
      }
      if (icmp_.Compare(m->largest, *largest) > 0) {
        *largest = m->largest;
      }
    }
  }
}

void ZnsVersionSet::GetRange2(const std::vector<SSZoneMetaData*>& inputs1,
                              const std::vector<SSZoneMetaData*>& inputs2,
                              InternalKey* smallest, InternalKey* largest) {
  std::vector<SSZoneMetaData*> all = inputs1;
  all.insert(all.end(), inputs2.begin(), inputs2.end());
  GetRange(all, smallest, largest);
}

// Finds the largest key in a vector of files. Returns true if files is not
// empty.
bool FindLargestKey(const InternalKeyComparator& icmp,
                    const std::vector<SSZoneMetaData*>& ss,
                    InternalKey* largest_key) {
  if (ss.empty()) {
    return false;
  }
  *largest_key = ss[0]->largest;
  for (size_t i = 1; i < ss.size(); ++i) {
    SSZoneMetaData* m = ss[i];
    if (icmp.Compare(m->largest, *largest_key) > 0) {
      *largest_key = m->largest;
    }
  }
  return true;
}

// Finds minimum file b2=(l2, u2) in level file for which l2 > u1 and
// user_key(l2) = user_key(u1)
SSZoneMetaData* FindSmallestBoundarySS(
    const InternalKeyComparator& icmp,
    const std::vector<SSZoneMetaData*>& level_ss,
    const InternalKey& largest_key) {
  const Comparator* user_cmp = icmp.user_comparator();
  SSZoneMetaData* smallest_boundary_ss = nullptr;
  for (size_t i = 0; i < level_ss.size(); ++i) {
    SSZoneMetaData* m = level_ss[i];
    if (icmp.Compare(m->smallest, largest_key) > 0 &&
        user_cmp->Compare(m->smallest.user_key(), largest_key.user_key()) ==
            0) {
      if (smallest_boundary_ss == nullptr ||
          icmp.Compare(m->smallest, smallest_boundary_ss->smallest) < 0) {
        smallest_boundary_ss = m;
      }
    }
  }
  return smallest_boundary_ss;
}

void AddBoundaryInputs(const InternalKeyComparator& icmp,
                       const std::vector<SSZoneMetaData*>& level_ss,
                       std::vector<SSZoneMetaData*>* compaction_ss) {
  InternalKey largest_key;

  // Quick return if compaction_files is empty.
  if (!FindLargestKey(icmp, *compaction_ss, &largest_key)) {
    return;
  }

  bool continue_searching = true;
  while (continue_searching) {
    SSZoneMetaData* smallest_boundary_ss =
        FindSmallestBoundarySS(icmp, level_ss, largest_key);

    // If a boundary file was found advance largest_key, otherwise we're done.
    if (smallest_boundary_ss != NULL) {
      compaction_ss->push_back(smallest_boundary_ss);
      largest_key = smallest_boundary_ss->largest;
    } else {
      continue_searching = false;
    }
  }
}

// static int64_t TotalLbas(const std::vector<SSZoneMetaData*>& ss) {
//   int64_t sum = 0;
//   for (size_t i = 0; i < ss.size(); i++) {
//     sum += ss[i]->lba_count;
//   }
//   return sum;
// }

// static int64_t ExpandedCompactionLbaSizeLimit(uint64_t lba_size) {
//   return 25 * (((ZnsConfig::max_bytes_sstable_ + lba_size - 1) / lba_size) *
//                lba_size);
// }

void ZnsVersionSet::SetupOtherInputs(ZnsCompaction* c, uint64_t max_lba_c) {
  const uint8_t level = c->first_level_;
  InternalKey smallest, largest;

  AddBoundaryInputs(icmp_, current_->ss_[level], &c->targets_[0]);
  GetRange(c->targets_[0], &smallest, &largest);

  current_->GetOverlappingInputs(level + 1, &smallest, &largest,
                                 &c->targets_[1]);
  AddBoundaryInputs(icmp_, current_->ss_[level + 1], &c->targets_[1]);

  // Get entire range covered by compaction
  InternalKey all_start, all_limit;
  GetRange2(c->targets_[0], c->targets_[1], &all_start, &all_limit);

  // See if we can grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up.
  // if (!c->targets_[1].empty()) {
  //   std::vector<SSZoneMetaData*> expanded0;
  //   current_->GetOverlappingInputs(level, &all_start, &all_limit,
  //   &expanded0); AddBoundaryInputs(icmp_, current_->ss_[level], &expanded0);
  //   const int64_t inputs0_size = TotalLbas(c->targets_[0]);
  //   const int64_t inputs1_size = TotalLbas(c->targets_[1]);
  //   const int64_t expanded0_size = TotalLbas(expanded0);
  //   printf("EX %lu %lu %lu %lu %lu\n", expanded0.size(),
  //   c->targets_[0].size(),
  //          inputs1_size, expanded0_size,
  //          ExpandedCompactionLbaSizeLimit(lba_size_));
  //   if (expanded0.size() > c->targets_[0].size() &&
  //       inputs1_size + expanded0_size <
  //           ExpandedCompactionLbaSizeLimit(lba_size_)) {
  //     InternalKey new_start, new_limit;
  //     GetRange(expanded0, &new_start, &new_limit);
  //     std::vector<SSZoneMetaData*> expanded1;
  //     current_->GetOverlappingInputs(level + 1, &new_start, &new_limit,
  //                                    &expanded1);
  //     AddBoundaryInputs(icmp_, current_->ss_[level + 1], &expanded1);
  //     if (expanded1.size() == c->targets_[1].size()) {
  //       smallest = new_start;
  //       largest = new_limit;
  //       c->targets_[0] = expanded0;
  //       c->targets_[1] = expanded1;
  //       GetRange2(c->targets_[0], c->targets_[1], &all_start, &all_limit);
  //     }
  //   }
  // }

  if (level + 2 < ZnsConfig::level_count) {
    current_->GetOverlappingInputs(level + 2, &all_start, &all_limit,
                                   &c->grandparents_);
  }

  compact_pointer_[level] = largest.Encode().ToString();
  c->edit_.SetCompactPointer(level, largest);
}

bool ZnsVersionSet::OnlyNeedDeletes(uint8_t level) {
  bool only_need = current_->ss_[level].size() == 0 ||
                   (level > 0 && znssstable_->GetFractionFilled(level) > 0.85);
  if (only_need) {
    TROPODB_DEBUG("ONLY %u %lu %lu \n", level, current_->ss_[level].size(),
           current_->ss_d_[level].size());
  }
  return only_need;
}

ZnsCompaction* ZnsVersionSet::PickCompaction(
    uint8_t level, const std::vector<SSZoneMetaData*>& busy) {
  ZnsCompaction* c;

  c = new ZnsCompaction(this, level, env_);
  c->busy_ = false;

  // printf("Compacting from <%d because score is %f, from %f>\n", level,
  //        current_->compaction_score_,
  //        znssstable_->GetFractionFilled(level));

  // We must make sure that the compaction will not be too big!
  uint64_t max_lba_c = znssstable_->SpaceRemainingLN();
  max_lba_c = max_lba_c > ZnsConfig::max_lbas_compaction_l0
                  ? ZnsConfig::max_lbas_compaction_l0
                  : max_lba_c;

  // Always pick the tail on L0
  uint64_t L0index;
  if (level == 0) {
    size_t l0_log_prio = 0;
    uint64_t space_rem = znssstable_->SpaceRemainingL0(l0_log_prio);
    for (size_t i = 1; i < ZnsConfig::lower_concurrency; i++) {
      if (znssstable_->SpaceRemainingL0(i) < space_rem) {
        l0_log_prio = i;
        space_rem = znssstable_->SpaceRemainingL0(i);
      }
    }
    uint64_t number = 0;
    uint64_t index = 0;
    bool number_picked = false;
    for (size_t i = 0; i < current_->ss_[level].size(); i++) {
      SSZoneMetaData* m = current_->ss_[level][i];
      if (m->L0.log_number == l0_log_prio &&
          (m->number < number || !number_picked)) {
        number = m->number;
        index = i;
        number_picked = true;
      }
    }
    if (!number_picked) {
      for (size_t i = 0; i < current_->ss_[level].size(); i++) {
        SSZoneMetaData* m = current_->ss_[level][i];
        if (m->L0.log_number != l0_log_prio &&
            (m->number < number || !number_picked)) {
          number = m->number;
          index = i;
          number_picked = true;
        }
      }
    }
    if (!number_picked) {
      printf("Compacting from empty level?\n");
      return c;
    } else {
      c->targets_[0].push_back(current_->ss_[level][index]);
    }
    L0index = number;
    // Go to compaction pointer on LN
  } else {
    for (size_t i = 0; i < current_->ss_[level].size(); i++) {
      SSZoneMetaData* m = current_->ss_[level][i];
      if (compact_pointer_[level].empty() ||
          icmp_.Compare(m->largest.Encode(), compact_pointer_[level]) > 0) {
        if (level == 1) {
          bool skip = false;
          for (const auto& m2 : busy) {
            if (m2->number == m->number) {
              skip = true;
              break;
            }
          }
          if (skip) {
            continue;
          }
        }
        c->targets_[0].push_back(m);
        max_lba_c -= m->lba_count;
        break;
      }
    }
    if (c->targets_[0].empty()) {
      // Wrap-around to the beginning of the key space
      if (level == 1) {
        for (size_t i = 0; i < current_->ss_[level].size(); i++) {
          SSZoneMetaData* m = current_->ss_[level][i];
          bool done = false;
          for (const auto& m2 : busy) {
            if (m2->number != m->number) {
              done = true;
              c->targets_[0].push_back(m);
              break;
            }
          }
          if (busy.size() == 0) {
            c->targets_[0].push_back(m);
            done = true;
          }
          if (done) {
            break;
          }
        }
        if (c->targets_[0].empty()) {
          c->busy_ = true;
          return c;
        }
      } else if (current_->ss_[level].size() > 0) {
        c->targets_[0].push_back(current_->ss_[level][0]);
        max_lba_c -= current_->ss_[level][0]->lba_count;
      } else {
        // This should not happen
        printf("Compacting from empty level?\n");
        return c;
      }
    }
  }

  c->version_ = current_;
  c->version_->Ref();

  // Files in level 0 may overlap each other, so pick up all overlapping ones
  // (if it fits in the next level...)
  // TEMP V disable, should be level==0, but this messes with deletes of the
  // circular log
  if (level == 0) {
    InternalKey smallest, largest;
    GetRange(c->targets_[0], &smallest, &largest);
    // Note that the next call will discard the file we placed in
    // c->inputs_[0] earlier and replace it with an overlapping set
    // which will include the picked file.
    std::vector<SSZoneMetaData*> overlapping;
    current_->GetOverlappingInputs(0, &smallest, &largest, &overlapping);
    // c->targets_[0].clear();
    // max_lba_c = znssstable_->SpaceRemaining(level + 1);
    // max_lba_c = max_lba_c > 800000 ? 800000 : max_lba_c;
    for (auto target : overlapping) {
      if (target->number == L0index) {
        continue;
      }
      if (target->lba_count > max_lba_c) {
        break;
      }
      c->targets_[0].push_back(target);
      max_lba_c -= target->lba_count;
      // printf("expanding... %lu \n", max_lba_c);
    }

    assert(!c->targets_[0].empty());
  }

  SetupOtherInputs(c, max_lba_c);
  printf("Compact from %u, with size %lu/%lu(%lud) %lu/%lu(%lud \n", level,
         c->targets_[0].size(), current_->ss_[level].size(),
         current_->ss_d_[level].size(), c->targets_[1].size(),
         current_->ss_[level + 1].size(), current_->ss_d_[level + 1].size());
  return c;
}

Status ZnsVersionSet::RemoveObsoleteZones(ZnsVersionEdit* edit) {
  Status s = Status::OK();
  for (const auto& deleted : edit->deleted_ss_) {
    table_cache_->Evict(deleted.second);
  }
  return s;
}

Status ZnsVersionSet::DecodeFrom(const Slice& src, ZnsVersionEdit* edit) {
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
        // No longer supported
        return Status::Corruption();
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
  std::string manifest_data;
  ZnsVersionEdit edit;
  s = manifest_->Recover();
  if (s.ok()) {
    s = manifest_->ReadManifest(&manifest_data);
    if (!s.ok()) {
      printf("error reading manifest \n");
      return s;
    }
  }

  if (s.ok()) {
    s = DecodeFrom(manifest_data, &edit);
    if (!s.ok()) {
      printf("Corrupt manifest \n");
      return s;
    }
  }

  // Recover log functionalities for L0 to LN.
  if (edit.has_fragmented_data_) {
    s = znssstable_->Recover(edit.fragmented_data_);
  } else {
    s = znssstable_->Recover("");
  }

  // Install recovered edit
  if (s.ok()) {
    s = LogAndApply(&edit);
  } else {
    printf("Corrupt fragmented data \n");
  }

  if (edit.has_last_sequence_) {
    last_sequence_ = edit.last_sequence_;
  }
  if (edit.has_next_ss_number) {
    ss_number_ = edit.ss_number;
  }

  if (!s.ok()) {
    printf("Error setting edit to current \n");
    return s;
  }

  // Setup numbers, temporary hack...
  if (ss_number_ == 0) {
    for (uint8_t i = 0; i < ZnsConfig::level_count; i++) {
      std::vector<SSZoneMetaData*>& m = current_->ss_[i];
      for (size_t j = 0; j < m.size(); j++) {
        uint64_t cur_ss_number = ss_number_;
        uint64_t new_ss_number =
            cur_ss_number > m[j]->number ? cur_ss_number : m[j]->number + 1;
        ss_number_ = new_ss_number;
      }
    }
  }
  if (ss_number_l0_ == 0) {
    for (uint8_t i = 0; i < 1; i++) {
      std::vector<SSZoneMetaData*>& m = current_->ss_[i];
      for (size_t j = 0; j < m.size(); j++) {
        uint64_t cur_ss_number = ss_number_l0_;
        uint64_t new_ss_number = cur_ss_number > m[j]->L0.number
                                     ? cur_ss_number
                                     : m[j]->L0.number + 1;
        ss_number_l0_ = new_ss_number;
      }
    }
  }
  return Status::OK();
}

std::string ZnsVersionSet::DebugString() {
  std::string result;
  for (uint8_t i = 0; i < ZnsConfig::level_count; i++) {
    std::vector<SSZoneMetaData*>& m = current_->ss_[i];
    result.append("\tLevel " + std::to_string(i) + ": " +
                  std::to_string(m.size()) + " tables \n");
  }
  return result;
}

}  // namespace ROCKSDB_NAMESPACE
