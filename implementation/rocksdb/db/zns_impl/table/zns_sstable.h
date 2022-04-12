#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_SSTABLE_H
#define ZNS_SSTABLE_H

#include "db/zns_impl/device_wrapper.h"
#include "db/zns_impl/qpair_factory.h"
#include "db/zns_impl/ref_counter.h"
#include "db/zns_impl/zns_memtable.h"
#include "db/zns_impl/zns_zonemetadata.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
enum class EntryStatus { found, deleted, notfound };

class ZNSSSTableManager;

class SSTableBuilder {
 public:
  SSTableBuilder(){};
  virtual ~SSTableBuilder(){};
  virtual Status Apply(const Slice& key, const Slice& value) = 0;
  virtual Status Finalise() = 0;
  virtual Status Flush() = 0;
};

class ZnsSSTable {
 public:
  ZnsSSTable(QPairFactory* qpair_factory, const ZnsDevice::DeviceInfo& info,
             const uint64_t min_zone_head, uint64_t max_zone_head);
  virtual ~ZnsSSTable();
  virtual Status ReadSSTable(Slice* sstable, SSZoneMetaData* meta) = 0;
  virtual Status Get(const Slice& key, std::string* value, SSZoneMetaData* meta,
                     EntryStatus* entry) = 0;
  virtual bool EnoughSpaceAvailable(Slice slice) = 0;
  virtual Status InvalidateSSZone(SSZoneMetaData* meta) = 0;
  virtual SSTableBuilder* NewBuilder(SSZoneMetaData* meta) = 0;
  virtual Status WriteSSTable(Slice content, SSZoneMetaData* meta) = 0;
  virtual Iterator* NewIterator(SSZoneMetaData* meta) = 0;

 protected:
  friend class SSTableBuilder;
  void PutKVPair(std::string* dst, const Slice& key, const Slice& value);
  void GeneratePreamble(std::string* dst, uint32_t count);

  // data
  uint64_t zone_head_;
  uint64_t write_head_;
  uint64_t zone_tail_;
  uint64_t write_tail_;
  uint64_t min_zone_head_;
  uint64_t max_zone_head_;
  uint64_t zone_size_;
  uint64_t lba_size_;
  // references
  QPairFactory* qpair_factory_;
  ZnsDevice::QPair** qpair_;
};

// Like a Oroborous, an entire circle without holes.
class L0ZnsSSTable : public ZnsSSTable {
 public:
  L0ZnsSSTable(QPairFactory* qpair_factory, const ZnsDevice::DeviceInfo& info,
               const uint64_t min_zone_head, uint64_t max_zone_head);
  ~L0ZnsSSTable();
  Status FlushMemTable(ZNSMemTable* mem, SSZoneMetaData* meta);
  Status ReadSSTable(Slice* sstable, SSZoneMetaData* meta) override;
  Status Get(const Slice& key, std::string* value, SSZoneMetaData* meta,
             EntryStatus* entry) override;
  bool EnoughSpaceAvailable(Slice slice) override;
  SSTableBuilder* NewBuilder(SSZoneMetaData* meta) override;
  Status InvalidateSSZone(SSZoneMetaData* meta);
  Status WriteSSTable(Slice content, SSZoneMetaData* meta) override;
  Iterator* NewIterator(SSZoneMetaData* meta) override;

 private:
  class Builder;
  friend class ZnsSSTableManagerInternal;
  Status SetWriteAddress(Slice slice);
  Status ConsumeTail(uint64_t begin_lba, uint64_t end_lba);
  bool ValidateReadAddress(SSZoneMetaData* meta);
  static void ParseNext(char** src, Slice* key, Slice* value);
  uint64_t pseudo_write_head_;
  port::Mutex mutex_;
};

// Can have holes.
// class LnZnsSSTable : public ZnsSSTable {

// };

int FindSS(const InternalKeyComparator& icmp,
           const std::vector<SSZoneMetaData*>& ss, const Slice& key);

/**
 * @brief To be used for debugging private variables of ZNSSSTableManager only.
 */
class ZnsSSTableManagerInternal {
 public:
  static inline uint64_t GetZoneHead(L0ZnsSSTable* sstable) {
    return sstable->zone_head_;
  }
  static inline uint64_t GetWriteHead(L0ZnsSSTable* sstable) {
    return sstable->write_head_;
  }
  static inline uint64_t GetMinZoneHead(L0ZnsSSTable* sstable) {
    return sstable->min_zone_head_;
  }
  static inline uint64_t GetMaxZoneHead(L0ZnsSSTable* sstable) {
    return sstable->max_zone_head_;
  }
  static inline uint64_t GetZoneSize(L0ZnsSSTable* sstable) {
    return sstable->zone_size_;
  }
  static inline uint64_t GetLbaSize(L0ZnsSSTable* sstable) {
    return sstable->lba_size_;
  }

  static Status ConsumeTail(L0ZnsSSTable* sstable, uint64_t begin_lba,
                            uint64_t end_lba) {
    return sstable->ConsumeTail(begin_lba, end_lba);
  }
};
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
