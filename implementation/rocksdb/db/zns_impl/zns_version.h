#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_VERSION_EDIT_H
#define ZNS_VERSION_EDIT_H

#include "db/dbformat.h"
#include "db/zns_impl/device_wrapper.h"
#include "db/zns_impl/ref_counter.h"
#include "db/zns_impl/zns_manifest.h"
#include "db/zns_impl/zns_sstable_manager.h"
#include "db/zns_impl/zns_zonemetadata.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
class ZnsVersionEdit;
class ZnsVersion;
class ZnsVersionSet;

enum class VersionTag : uint32_t {
  kComparator = 1,
  kLogNumber = 2,
  kNextFileNumber = 3,
  kLastSequence = 4,
  kCompactPointer = 5,
  kDeletedFile = 6,
  kNewFile = 7,
  // 8 was used for large value refs
  kPrevLogNumber = 9
};

class ZnsVersionEdit {
 public:
  ZnsVersionEdit() { Clear(); }
  ~ZnsVersionEdit() = default;

  void Clear() {
    last_sequence_ = 0;
    has_last_sequence_ = false;
    new_ss_.clear();
    deleted_ss_.clear();
    deleted_ss_seq_.clear();
    has_comparator_ = false;
    comparator_.clear();
  };

  void EncodeTo(std::string* dst);

  void AddSSDefinition(int level, uint64_t lba, uint64_t lba_count,
                       uint64_t numbers, const InternalKey& smallest,
                       const InternalKey& largest);
  void RemoveSSDefinition(int level, uint64_t lba) {
    deleted_ss_.insert(std::make_pair(level, lba));
  }

  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }

 private:
  friend class ZnsVersionSet;

  typedef std::set<std::pair<int, uint64_t>> DeletedFileSet;

  std::vector<std::pair<int, SSZoneMetaData>> new_ss_;
  DeletedFileSet deleted_ss_;
  std::vector<std::pair<int, SSZoneMetaData>> deleted_ss_seq_;
  int last_sequence_;
  bool has_last_sequence_;
  std::string comparator_;
  bool has_comparator_;
};

class ZnsVersion : public RefCounter {
 public:
  void Clear(){};
  Status Get(const ReadOptions& options, const Slice& key, std::string* value);

 private:
  friend class ZnsVersionSet;
  std::vector<SSZoneMetaData*> ss_[7];
  ZnsVersionSet* vset_;

  explicit ZnsVersion(ZnsVersionSet* vset) : vset_(vset) {}
  ZnsVersion() { Clear(); }
  ~ZnsVersion() {
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
};

class ZnsVersionSet {
 public:
  ZnsVersionSet(const InternalKeyComparator& icmp,
                ZNSSSTableManager* znssstable, ZnsManifest* manifest,
                uint64_t lba_size)
      : current_(nullptr),
        icmp_(icmp),
        znssstable_(znssstable),
        manifest_(manifest),
        lba_size_(lba_size),
        logged_(false) {
    AppendVersion(new ZnsVersion());
  };
  ZnsVersionSet(const ZnsVersionSet&) = delete;
  ZnsVersionSet& operator=(const ZnsVersionSet&) = delete;
  ~ZnsVersionSet() { current_->Unref(); }

  Status WriteSnapshot(std::string* snapshot_dst);
  Status LogAndApply(ZnsVersionEdit* edit);

  inline ZnsVersion* current() { return current_; }

  inline uint64_t LastSequence() const { return last_sequence_; }
  inline void SetLastSequence(uint64_t s) {
    assert(s >= last_sequence_);
    last_sequence_ = s;
  }

  inline int NumLevelZones(int level) const {
    assert(level >= 0 && level < 7);
    return current_->ss_[level].size();
  }

  inline int NumLevelBytes(int level) const {
    const std::vector<SSZoneMetaData*>& ss = current_->ss_[level];
    int64_t sum = 0;
    for (size_t i = 0; i < ss.size(); i++) {
      sum += ss[i]->lba_count * lba_size_;
    }
    return sum;
  }

  Status Compact(ZnsVersionEdit* edit) {
    Status s = Status::OK();
    for (size_t i = 0; i < 1; i++) {
      const std::vector<SSZoneMetaData*>& base_ss = current_->ss_[i];
      std::vector<SSZoneMetaData*>::const_iterator base_iter = base_ss.begin();
      std::vector<SSZoneMetaData*>::const_iterator base_end = base_ss.end();
      for (; base_iter != base_end; ++base_iter) {
        SSZoneMetaData* old_meta = *base_iter;
        edit->RemoveSSDefinition(i, old_meta->lba);
        edit->deleted_ss_seq_.push_back(std::make_pair(i, *old_meta));
        SSZoneMetaData new_meta(*old_meta);
        s = znssstable_->RewriteSSTable(&new_meta);
        if (!s.ok()) {
          return s;
        }
        edit->AddSSDefinition(i + 1, new_meta.lba, new_meta.lba_count,
                              new_meta.numbers, new_meta.smallest,
                              new_meta.largest);
      }
    }
    return s;
  }

  Status RemoveObsoleteL0(ZnsVersionEdit* edit) {
    Status s = Status::OK();
    std::vector<std::pair<int, rocksdb::SSZoneMetaData>>& base_ss =
        edit->deleted_ss_seq_;
    std::vector<std::pair<int, rocksdb::SSZoneMetaData>>::const_iterator
        base_iter = base_ss.begin();
    std::vector<std::pair<int, rocksdb::SSZoneMetaData>>::const_iterator
        base_end = base_ss.end();
    for (; base_iter != base_end; ++base_iter) {
      SSZoneMetaData m = (*base_iter).second;
      s = znssstable_->InvalidateSSZone(&m);
      if (!s.ok()) {
        return s;
      }
    }
    return s;
  }

 private:
  class Builder;

  friend class ZnsVersion;

  void AppendVersion(ZnsVersion* v);

  ZnsVersion* current_;
  const InternalKeyComparator icmp_;
  ZNSSSTableManager* znssstable_;
  ZnsManifest* manifest_;
  uint64_t lba_size_;
  uint64_t last_sequence_;
  bool logged_;
};

}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
