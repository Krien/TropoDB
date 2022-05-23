#include "db/zns_impl/config.h"
#include "db/zns_impl/index/zns_version.h"
#include "db/zns_impl/index/zns_version_set.h"
#include "db/zns_impl/table/zns_zonemetadata.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
ZnsVersionSet::Builder::Builder(ZnsVersionSet* vset, ZnsVersion* base)
    : vset_(vset), base_(base) {
  base_->Ref();
  BySmallestKey cmp;
  cmp.internal_comparator = &vset_->icmp_;
  for (uint8_t level = 0; level < ZnsConfig::level_count; level++) {
    levels_[level].added_ss = new ZoneSet(cmp);
  }
  ss_deleted_range_ = vset->current_->ss_deleted_range_;
}

ZnsVersionSet::Builder::~Builder() {
  for (uint8_t level = 0; level < ZnsConfig::level_count; level++) {
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
void ZnsVersionSet::Builder::Apply(const ZnsVersionEdit* edit) {
  // TODO Update compaction pointers
  for (size_t i = 0; i < edit->compact_pointers_.size(); i++) {
    const uint8_t level = edit->compact_pointers_[i].first;
    vset_->compact_pointer_[level] =
        edit->compact_pointers_[i].second.Encode().ToString();
  }

  // Delete files
  for (const auto& deleted_ss_set_kvp : edit->deleted_ss_) {
    const uint8_t level = deleted_ss_set_kvp.first;
    const uint64_t number = deleted_ss_set_kvp.second;
    levels_[level].deleted_ss.insert(number);
  }

  // Deleted range
  if (edit->has_deleted_range_) {
    ss_deleted_range_ = edit->deleted_range_;
  }

  // Add deleted zone regions
  for (size_t i = 0; i < edit->deleted_ss_pers_.size(); i++) {
    const uint8_t level = edit->deleted_ss_pers_[i].first;
    SSZoneMetaData* m = new SSZoneMetaData(edit->deleted_ss_pers_[i].second);
    m->refs = 1;
    levels_[level].deleted_ss_pers.push_back(m);
  }

  // Add new zone regions
  for (size_t i = 0; i < edit->new_ss_.size(); i++) {
    const uint8_t level = edit->new_ss_[i].first;
    SSZoneMetaData* m = new SSZoneMetaData(edit->new_ss_[i].second);
    m->refs = 1;
    levels_[level].deleted_ss.erase(m->number);
    levels_[level].added_ss->insert(m);
  }

  // fragmented data
  fragmented_data_.clear();
  for (auto frag : edit->fragmented_data_) {
    fragmented_data_.push_back(frag);
  }
}

// Save the current state in *v.
void ZnsVersionSet::Builder::SaveTo(ZnsVersion* v) {
  BySmallestKey cmp;
  cmp.internal_comparator = &vset_->icmp_;
  for (uint8_t level = 0; level < ZnsConfig::level_count; level++) {
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

    // Make sure there is no overlap in levels > 0
    if (level > 0) {
      for (size_t i = 1; i < v->ss_[level].size(); i++) {
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

    // TODO: improve or something
    // Add deleted files to level
    for (auto d : base_->ss_d_[level]) {
      v->ss_d_[level].push_back(d);
    }
    for (auto d : levels_[level].deleted_ss_pers) {
      v->ss_d_[level].push_back(d);
    }
  }
  // Add ranges to delete.
  v->ss_deleted_range_ = ss_deleted_range_;
}

void ZnsVersionSet::Builder::MaybeAddZone(ZnsVersion* v, const uint8_t level,
                                          SSZoneMetaData* f) {
  if (levels_[level].deleted_ss.count(f->number) > 0) {
    // File is deleted: do nothing
  } else {
    std::vector<SSZoneMetaData*>* ss = &v->ss_[level];
    if (level > 0 && !ss->empty()) {
      // Must not overlap
      assert(vset_->icmp_.Compare((*ss)[ss->size() - 1]->largest, f->smallest) <
             0);
    }
    f->refs++;
    ss->push_back(f);
  }
}
}  // namespace ROCKSDB_NAMESPACE
