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
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
ZnsVersionSet::ZnsVersionSet(const InternalKeyComparator& icmp,
                             ZNSSSTableManager* znssstable,
                             ZnsManifest* manifest, const uint64_t lba_size,
                             ZnsTableCache* table_cache)
    : dummy_versions_(this),
      current_(nullptr),
      icmp_(icmp),
      znssstable_(znssstable),
      manifest_(manifest),
      lba_size_(lba_size),
      ss_number_(0),
      logged_(false),
      table_cache_(table_cache) {
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
}

void ZnsVersionSet::GetLiveZoneRange(const uint8_t level,
                                     std::pair<uint64_t, uint64_t>* range) {
  *range = std::make_pair<uint64_t, uint64_t>(0, 0);
  bool ran_set = false;
  for (ZnsVersion* v = dummy_versions_.next_; v != &dummy_versions_;
       v = v->next_) {
    const std::vector<SSZoneMetaData*>& metas = v->ss_[level];
    std::pair temp_ran = std::make_pair<uint64_t, uint64_t>(0, 0);
    znssstable_->GetRange(level, metas, &temp_ran);
    if (!ran_set) {
      *range = temp_ran;
      ran_set = true;
      // printf("range %lu %lu \n", ran.first, ran.second);
    } else if (temp_ran.first != temp_ran.second) {
      range->first = temp_ran.first;
    }
  }
}

Status ZnsVersionSet::ReclaimStaleSSTables() {
  printf("reclaiming....\n");
  Status s = Status::OK();
  std::pair<uint64_t, uint64_t> range;
  ZnsVersionEdit edit;
  for (size_t i = 0; i < ZnsConfig::level_count; i++) {
    if (current_->ss_d_[i].first == 0) {
      continue;
    }
    GetLiveZoneRange(i, &range);
    s = znssstable_->SetValidRangeAndReclaim(i, current_->ss_d_[i].second,
                                             range.second);
    edit.AddDeletedRange(i, std::make_pair(0, 0));
    if (!s.ok()) {
      return s;
    }
  }
  s = LogAndApply(&edit);
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
      const SSZoneMetaData* m = ss[i];
      edit.AddSSDefinition(level, m->number, m->lba, m->lba_count, m->numbers,
                           m->smallest, m->largest);
    }
  }
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
  double score;
  for (size_t i = 0; i < ZnsConfig::level_count - 1; i++) {
    score =
        znssstable_->GetFractionFilled(i) / ZnsConfig::ss_compact_treshold[i];
    if (score > best_score) {
      best_score = score;
      best_level = i;
      // printf("Score %f from level %d\n", best_score, best_level);
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
  s = manifest_->GetCurrentWriteHead(&current_lba);
  s = manifest_->NewManifest(result);
  if (s.ok()) {
    s = manifest_->SetCurrent(current_lba);
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

static int64_t TotalLbas(const std::vector<SSZoneMetaData*>& ss) {
  int64_t sum = 0;
  for (size_t i = 0; i < ss.size(); i++) {
    sum += ss[i]->lba_count;
  }
  return sum;
}

static int64_t ExpandedCompactionLbaSizeLimit(uint64_t lba_size) {
  return 25 * (((ZnsConfig::max_bytes_sstable_ + lba_size - 1) / lba_size) *
               lba_size);
}

void ZnsVersionSet::SetupOtherInputs(ZnsCompaction* c) {
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
  if (!c->targets_[1].empty()) {
    std::vector<SSZoneMetaData*> expanded0;
    current_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0);
    AddBoundaryInputs(icmp_, current_->ss_[level], &expanded0);
    const int64_t inputs0_size = TotalLbas(c->targets_[0]);
    const int64_t inputs1_size = TotalLbas(c->targets_[1]);
    const int64_t expanded0_size = TotalLbas(expanded0);
    if (expanded0.size() > c->targets_[0].size() &&
        inputs1_size + expanded0_size <
            ExpandedCompactionLbaSizeLimit(lba_size_)) {
      InternalKey new_start, new_limit;
      GetRange(expanded0, &new_start, &new_limit);
      std::vector<SSZoneMetaData*> expanded1;
      current_->GetOverlappingInputs(level + 1, &new_start, &new_limit,
                                     &expanded1);
      AddBoundaryInputs(icmp_, current_->ss_[level + 1], &expanded1);
      if (expanded1.size() == c->targets_[1].size()) {
        smallest = new_start;
        largest = new_limit;
        c->targets_[0] = expanded0;
        c->targets_[1] = expanded1;
        GetRange2(c->targets_[0], c->targets_[1], &all_start, &all_limit);
      }
    }
  }

  compact_pointer_[level] = largest.Encode().ToString();
  c->edit_.SetCompactPointer(level, largest);
}

ZnsCompaction* ZnsVersionSet::PickCompaction() {
  ZnsCompaction* c;
  uint8_t level;

  level = current_->compaction_level_;
  c = new ZnsCompaction(this, level);

  printf("Compacting from <%d because score is %f, from %f>\n", level,
         current_->compaction_score_, znssstable_->GetFractionFilled(level));

  // Pick the first file that comes after compact_pointer_[level]
  for (size_t i = 0; i < current_->ss_[level].size(); i++) {
    SSZoneMetaData* m = current_->ss_[level][i];
    if (compact_pointer_[level].empty() ||
        icmp_.Compare(m->largest.Encode(), compact_pointer_[level]) > 0) {
      c->targets_[0].push_back(m);
      break;
    }
  }
  if (c->targets_[0].empty()) {
    // Wrap-around to the beginning of the key space
    c->targets_[0].push_back(current_->ss_[level][0]);
  }
  c->version_ = current_;
  c->version_->Ref();

  // Files in level 0 may overlap each other, so pick up all overlapping ones
  if (level == 0) {
    InternalKey smallest, largest;
    GetRange(c->targets_[0], &smallest, &largest);
    // Note that the next call will discard the file we placed in
    // c->inputs_[0] earlier and replace it with an overlapping set
    // which will include the picked file.
    current_->GetOverlappingInputs(0, &smallest, &largest, &c->targets_[0]);
    assert(!c->targets_[0].empty());
  }

  SetupOtherInputs(c);

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
  s = manifest_->Recover();
  std::string manifest_data;
  s = manifest_->ReadManifest(&manifest_data);

  s = znssstable_->Recover();
  if (!s.ok()) {
    return s;
  }

  ZnsVersionEdit edit;
  s = DecodeFrom(manifest_data, &edit);
  if (!s.ok()) {
    return s;
  }

  // Install recovered edit
  if (s.ok()) {
    LogAndApply(&edit);
  }
  if (edit.has_last_sequence_) {
    last_sequence_ = edit.last_sequence_;
  }
  if (edit.has_next_ss_number) {
    ss_number_ = edit.ss_number;
  }

  // Setup numbers, temporary hack...
  if (ss_number_ == 0) {
    for (uint8_t i = 0; i < ZnsConfig::level_count; i++) {
      std::vector<SSZoneMetaData*>& m = current_->ss_[i];
      for (size_t j = 0; j < m.size(); j++) {
        ss_number_ = ss_number_ > m[j]->number ? ss_number_ : m[j]->number + 1;
      }
    }
  }
  return Status::OK();
}

std::string ZnsVersionSet::DebugString() {
  std::string result;
  for (uint8_t i = 0; i < ZnsConfig::level_count; i++) {
    std::vector<SSZoneMetaData*>& m = current_->ss_[i];
    result.append("\t" + std::to_string(i) + ": " + std::to_string(m.size()) +
                  "\n");
  }
  return result;
}

}  // namespace ROCKSDB_NAMESPACE
