#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_VERSION_EDIT_H
#define ZNS_VERSION_EDIT_H

#include "db/dbformat.h"
#include "db/zns_impl/device_wrapper.h"
#include "db/zns_impl/ref_counter.h"
#include "db/zns_impl/zns_sstable_manager.h"
#include "db/zns_impl/zns_zonemetadata.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
class ZnsVersionEdit;
class ZnsVersion;
class ZnsVersionSet;

class ZnsVersionEdit {
 public:
  ZnsVersionEdit() { Clear(); }
  ~ZnsVersionEdit() = default;

  void Clear(){};
  void AddSSDefinition(int level, uint64_t lba, uint64_t lba_count,
                       uint64_t numbers, const InternalKey& smallest,
                       const InternalKey& largest);
  void RemoveSSDefinition(int level, uint64_t lba) {}

 private:
  friend class ZnsVersionSet;
  std::vector<std::pair<int, SSZoneMetaData>> new_ss_;
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
                ZNSSSTableManager* znssstable, uint64_t lba_size)
      : current_(nullptr),
        icmp_(icmp),
        znssstable_(znssstable),
        lba_size_(lba_size) {
    AppendVersion(new ZnsVersion());
  };
  ZnsVersionSet(const ZnsVersionSet&) = delete;
  ZnsVersionSet& operator=(const ZnsVersionSet&) = delete;
  ~ZnsVersionSet() { current_->Unref(); }

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

 private:
  friend class ZnsVersion;
  void AppendVersion(ZnsVersion* v);

  ZnsVersion* current_;
  const InternalKeyComparator icmp_;
  ZNSSSTableManager* znssstable_;
  uint64_t lba_size_;
  uint64_t last_sequence_;
};

}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
