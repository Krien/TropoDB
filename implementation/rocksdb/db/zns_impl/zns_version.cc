#include "db/zns_impl/zns_version.h"

#include <iostream>

#include "db/zns_impl/table/iterators/merging_iterator.h"
#include "db/zns_impl/table/iterators/sstable_ln_iterator.h"
#include "db/zns_impl/table/zns_sstable.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

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

class ZnsVersionSet::Builder {
 private:
  // Helper to sort by v->files_[file_number].smallest
  struct BySmallestKey {
    const InternalKeyComparator* internal_comparator;

    bool operator()(SSZoneMetaData* m1, SSZoneMetaData* m2) const {
      int r = internal_comparator->Compare(m1->smallest, m2->smallest);
      if (r != 0) {
        return (r < 0);
      } else {
        // Break ties by file number
        return (m1->number < m2->number);
      }
    }
  };

  typedef std::set<SSZoneMetaData*, BySmallestKey> ZoneSet;
  struct LevelState {
    std::set<uint64_t> deleted_ss;
    ZoneSet* added_ss;
  };

  ZnsVersionSet* vset_;
  ZnsVersion* base_;
  LevelState levels_[7];

 public:
  // Initialize a builder with the files from *base and other info from *vset
  Builder(ZnsVersionSet* vset, ZnsVersion* base) : vset_(vset), base_(base) {
    base_->Ref();
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < 7; level++) {
      levels_[level].added_ss = new ZoneSet(cmp);
    }
  }

  ~Builder() {
    for (int level = 0; level < 7; level++) {
      const ZoneSet* added = levels_[level].added_ss;
      std::vector<SSZoneMetaData*> to_unref;
      to_unref.reserve(added->size());
      for (ZoneSet::const_iterator it = added->begin(); it != added->end();
           ++it) {
        to_unref.push_back(*it);
      }
      delete added;
      for (uint32_t i = 0; i < to_unref.size(); i++) {
        SSZoneMetaData* f = to_unref[i];
        f->refs--;
        if (f->refs <= 0) {
          delete f;
        }
      }
    }
    base_->Unref();
  }

  // Apply all of the edits in *edit to the current state.
  void Apply(const ZnsVersionEdit* edit) {
    // TODO Update compaction pointers
    // Delete files
    for (const auto& deleted_ss_set_kvp : edit->deleted_ss_) {
      const int level = deleted_ss_set_kvp.first;
      const uint64_t number = deleted_ss_set_kvp.second;
      levels_[level].deleted_ss.insert(number);
    }

    // Add new files
    for (size_t i = 0; i < edit->new_ss_.size(); i++) {
      const int level = edit->new_ss_[i].first;
      SSZoneMetaData* m = new SSZoneMetaData(edit->new_ss_[i].second);
      m->refs = 1;
      levels_[level].deleted_ss.erase(m->number);
      levels_[level].added_ss->insert(m);
    }
  }

  // Save the current state in *v.
  void SaveTo(ZnsVersion* v) {
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < 7; level++) {
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.
      const std::vector<SSZoneMetaData*>& base_ss = base_->ss_[level];
      std::vector<SSZoneMetaData*>::const_iterator base_iter = base_ss.begin();
      std::vector<SSZoneMetaData*>::const_iterator base_end = base_ss.end();
      const ZoneSet* added_ss = levels_[level].added_ss;
      v->ss_[level].reserve(base_ss.size() + added_ss->size());
      for (const auto& added : *added_ss) {
        // Add all smaller files listed in base_
        for (std::vector<SSZoneMetaData*>::const_iterator bpos =
                 std::upper_bound(base_iter, base_end, added, cmp);
             base_iter != bpos; ++base_iter) {
          MaybeAddZone(v, level, *base_iter);
        }

        MaybeAddZone(v, level, added);
      }

      // Add remaining base files
      for (; base_iter != base_end; ++base_iter) {
        MaybeAddZone(v, level, *base_iter);
      }

#ifndef NDEBUG
      // Make sure there is no overlap in levels > 0
      if (level > 0) {
        for (uint32_t i = 1; i < v->ss_[level].size(); i++) {
          const InternalKey& prev_end = v->ss_[level][i - 1]->largest;
          const InternalKey& this_begin = v->ss_[level][i]->smallest;
          if (vset_->icmp_.Compare(prev_end, this_begin) >= 0) {
            std::fprintf(stderr, "overlapping ranges in same level %s vs. %s\n",
                         prev_end.DebugString(true).c_str(),
                         this_begin.DebugString(true).c_str());
            std::abort();
          }
        }
      }
#endif
    }
  }

  void MaybeAddZone(ZnsVersion* v, int level, SSZoneMetaData* f) {
    if (levels_[level].deleted_ss.count(f->number) > 0) {
      // File is deleted: do nothing
    } else {
      std::vector<SSZoneMetaData*>* ss = &v->ss_[level];
      if (level > 0 && !ss->empty()) {
        // Must not overlap
        assert(vset_->icmp_.Compare((*ss)[ss->size() - 1]->largest,
                                    f->smallest) < 0);
      }
      f->refs++;
      ss->push_back(f);
    }
  }
};

Status ZnsVersionSet::WriteSnapshot(std::string* snapshot_dst) {
  ZnsVersionEdit edit;
  edit.SetComparatorName(icmp_.user_comparator()->Name());
  // compaction stufff
  for (int level = 0; level < 7; level++) {
    const std::vector<SSZoneMetaData*>& ss = current_->ss_[level];
    for (size_t i = 0; i < ss.size(); i++) {
      const SSZoneMetaData* m = ss[i];
      edit.AddSSDefinition(level, m->number, m->lba, m->lba_count, m->numbers,
                           m->smallest, m->largest);
    }
  }
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
  std::string snapshot = "";
  if (!logged_) {
    s = WriteSnapshot(&snapshot);
    if (s.ok()) {
      logged_ = true;
    }
  }

  // MANIFEST STUFF...
  uint64_t current_lba;
  s = manifest_->GetCurrentWriteHead(&current_lba);
  if (!s.ok()) {
    return s;
  }
  edit->SetComparatorName(icmp_.user_comparator()->Name());
  std::string record;
  edit->EncodeTo(&record);
  s = manifest_->NewManifest(snapshot + record);
  if (s.ok()) {
    s = manifest_->SetCurrent(current_lba);
  }
  // Installing?
  if (s.ok()) {
    AppendVersion(v);
  }
  return s;
}

void ZnsVersionSet::AppendVersion(ZnsVersion* v) {
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != nullptr) {
    current_->Unref();
  }
  current_ = v;
  v->Ref();
}

Iterator* ZnsVersionSet::GetSSIterator(void* arg, const ReadOptions& options,
                                       const Slice& ss_value) {
  ZNSSSTableManager* ssman = reinterpret_cast<ZNSSSTableManager*>(arg);
  SSZoneMetaData* meta = new SSZoneMetaData;
  meta->lba = DecodeFixed64(ss_value.data());
  meta->lba_count = DecodeFixed64(ss_value.data() + 8);
  return ssman->NewIterator(1, meta);
}

// Iterator* ZnsVersionSet::MakeInputIterator() {
//   const std::vector<SSZoneMetaData*>& l0ss = current_->ss_[0];
//   const std::vector<SSZoneMetaData*>& l1ss = current_->ss_[1];
//   Iterator** list = new Iterator*[l0ss.size() + l1ss.size()];
//   int num = 0;
//   // l0
//   for (size_t i=0; i<l0ss.size(); i++) {
//     list[num++] = znssstable_->NewIterator(0, l0ss[i]);
//   }
//   ReadOptions options;
//   if (l1ss.size() > 0) {
//     list[num++] = NewTwoLevelIterator(
//       new ZnsVersion::SSNumIterator(icmp_, &l1ss),
//       znssstable_, options
//     );
//   }
//   Iterator* result = NewMergingIterator(&icmp_, list, num);
//   return result;
// }

static Iterator* GetLNIterator(void* arg, const Slice& file_value) {
  ZNSSSTableManager* zns = reinterpret_cast<ZNSSSTableManager*>(arg);
  SSZoneMetaData* meta = new SSZoneMetaData();
  uint64_t lba_start = DecodeFixed64(file_value.data());
  uint64_t lba_count = DecodeFixed64(file_value.data() + 8);
  return zns->NewIterator(1, meta);
}

Iterator* ZnsVersionSet::MakeCompactionIterator() {
  // 1 for each SStable in L0, 1 for each later level
  size_t iterators_needed = current_->ss_[0].size();
  iterators_needed += 1;
  Iterator** iterators = new Iterator*[iterators_needed];
  size_t iterator_index = 0;
  // L0
  {
    const std::vector<SSZoneMetaData*>& l0ss = current_->ss_[0];
    std::vector<SSZoneMetaData*>::const_iterator base_iter = l0ss.begin();
    std::vector<SSZoneMetaData*>::const_iterator base_end = l0ss.end();
    for (; base_iter != base_end; ++base_iter) {
      iterators[iterator_index++] = znssstable_->NewIterator(0, *base_iter);
    }
  }
  // LN
  {
    iterators[iterator_index++] =
        new LNIterator(new LNZoneIterator(icmp_, &current_->ss_[1]),
                       &GetLNIterator, znssstable_);
  }
  return NewMergingIterator(&icmp_, iterators, iterators_needed);
}

Status ZnsVersionSet::Compact(ZnsVersionEdit* edit) {
  printf("Starting compaction..\n");
  Status s = Status::OK();
  // TODO: drastic fix, this is inefficient, out of place and wrong...
  {
    for (int i = 0; i <= 0; i++) {
      std::vector<SSZoneMetaData*>::const_iterator base_iter =
          current_->ss_[i].begin();
      std::vector<SSZoneMetaData*>::const_iterator base_end =
          current_->ss_[i].end();
      for (; base_iter != base_end; ++base_iter) {
        edit->RemoveSSDefinition(i, (*base_iter)->number);
        edit->deleted_ss_seq_.push_back(std::make_pair(i, *base_iter));
      }
    }
  }
  {
    SSZoneMetaData meta;
    meta.number = NewSSNumber();
    SSTableBuilder* builder = znssstable_->NewBuilder(1, &meta);
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

void ZnsVersionEdit::AddSSDefinition(int level, uint64_t number, uint64_t lba,
                                     uint64_t lba_count, uint64_t numbers,
                                     const InternalKey& smallest,
                                     const InternalKey& largest) {
  SSZoneMetaData f;
  f.number = number;
  f.lba = lba;
  f.numbers = numbers;
  f.lba_count = lba_count;
  f.smallest = smallest;
  f.largest = largest;
  new_ss_.push_back(std::make_pair(level, f));
}

void ZnsVersionEdit::EncodeTo(std::string* dst) {
  // comparator
  if (has_comparator_) {
    PutVarint32(dst, static_cast<uint32_t>(VersionTag::kComparator));
    PutLengthPrefixedSlice(dst, comparator_);
  }
  // last sequence
  if (has_last_sequence_) {
    PutVarint32(dst, static_cast<uint32_t>(VersionTag::kLastSequence));
    PutVarint64(dst, last_sequence_);
  }

  // compaction pointers

  // deleted pointers
  for (const auto& deleted_file_kvp : deleted_ss_) {
    PutVarint32(dst, static_cast<uint32_t>(VersionTag::kDeletedFile));
    PutVarint32(dst, deleted_file_kvp.first);   // level
    PutVarint64(dst, deleted_file_kvp.second);  // lba
  }

  // new files
  for (size_t i = 0; i < new_ss_.size(); i++) {
    const SSZoneMetaData& m = new_ss_[i].second;
    PutVarint32(dst, static_cast<uint32_t>(VersionTag::kNewFile));
    PutVarint32(dst, new_ss_[i].first);  // level
    PutVarint64(dst, m.number);
    PutVarint64(dst, m.lba);
    PutVarint64(dst, m.numbers);
    PutVarint64(dst, m.lba_count);
    PutLengthPrefixedSlice(dst, m.smallest.Encode());
    PutLengthPrefixedSlice(dst, m.largest.Encode());
  }
}

}  // namespace ROCKSDB_NAMESPACE
