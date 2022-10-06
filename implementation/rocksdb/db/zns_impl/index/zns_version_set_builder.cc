#include "db/zns_impl/tropodb_config.h"
#include "db/zns_impl/index/zns_version.h"
#include "db/zns_impl/index/zns_version_set.h"
#include "db/zns_impl/table/zns_zonemetadata.h"
#include "db/zns_impl/utils/tropodb_logger.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
ZnsVersionSet::Builder::Builder(ZnsVersionSet* vset, ZnsVersion* base)
    : vset_(vset), base_(base) {
  // Unreffed in destructor
  base_->Ref();
  BySmallestKey cmp;
  cmp.internal_comparator = &vset_->icmp_;
  // Add an empty zone set for each level
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
  // TODO: Update compaction pointers more cleanly
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

  // Fragmented data (LN persistency)
  fragmented_data_.clear();
  if (edit->has_fragmented_data_) {
    fragmented_data_ = edit->fragmented_data_;
  }
}

// Save the current state in *v.
void ZnsVersionSet::Builder::SaveTo(ZnsVersion* v) {
  BySmallestKey cmp;
  cmp.internal_comparator = &vset_->icmp_;
  for (uint8_t level = 0; level < ZnsConfig::level_count; level++) {
    // Merge the set of added tables with the set of pre-existing tables.
    // Drop any deleted tables. Store the result in *v.
    const std::vector<SSZoneMetaData*>& base_sstables = base_->ss_[level];
    std::vector<SSZoneMetaData*>::const_iterator base_iter =
        base_sstables.begin();
    std::vector<SSZoneMetaData*>::const_iterator base_end = base_sstables.end();
    const ZoneSet* added_sstables = levels_[level].added_ss;
    // Reserve enough for both
    v->ss_[level].reserve(base_sstables.size() + added_sstables->size());
    // Merge 2-way
    for (const auto& added : *added_sstables) {
      // Add all smaller (keys) tables listed in base_
      for (std::vector<SSZoneMetaData*>::const_iterator bpos =
               std::upper_bound(base_iter, base_end, added, cmp);
           base_iter != bpos; ++base_iter) {
        MaybeAddZone(v, level, *base_iter);
      }
      // Then add THIS table
      MaybeAddZone(v, level, added);
    }
    // Add remaining base tables (that are larger than all added)
    for (; base_iter != base_end; ++base_iter) {
      MaybeAddZone(v, level, *base_iter);
    }

    // Make sure there is no overlap in levels > 0
    if (level > 0) {
      for (size_t i = 1; i < v->ss_[level].size(); i++) {
        const InternalKey& prev_end = v->ss_[level][i - 1]->largest;
        const InternalKey& this_begin = v->ss_[level][i]->smallest;
        if (vset_->icmp_.Compare(prev_end, this_begin) >= 0) {
          TROPODB_ERROR(
              "ERROR: Incorrect SSTables: Overlapping ranges in same level %s "
              "vs. %s\n",
              prev_end.DebugString(true).c_str(),
              this_begin.DebugString(true).c_str());
          std::abort();
        }
      }
    }

    // TODO: improve, this is not clean and a design smell
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
                                          SSZoneMetaData* m) {
  // Only add if the table is not deleted
  if (levels_[level].deleted_ss.count(m->number) > 0) {
    // Table is deleted: do nothing
  } else {
    std::vector<SSZoneMetaData*>* sstables = &v->ss_[level];
    if (level > 0 && !sstables->empty()) {
      // Must not overlap
      assert(vset_->icmp_.Compare((*sstables)[sstables->size() - 1]->largest,
                                  m->smallest) < 0);
    }
    m->refs++;
    sstables->push_back(m);
  }
}
}  // namespace ROCKSDB_NAMESPACE
