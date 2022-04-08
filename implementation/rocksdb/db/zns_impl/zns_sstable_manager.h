#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_SSTABLE_H
#define ZNS_SSTABLE_H

#include "db/zns_impl/device_wrapper.h"
#include "db/zns_impl/qpair_factory.h"
#include "db/zns_impl/ref_counter.h"
#include "db/zns_impl/zns_memtable.h"
#include "db/zns_impl/zns_zonemetadata.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
enum class EntryStatus { found, deleted, notfound };

class ZnsSSTableManagerInternal;

class ZnsSSTable {};

class L0ZnsSSTable : public ZnsSSTable {};

class ZNSSSTableManager : public RefCounter {
 public:
  ZNSSSTableManager(QPairFactory* qpair_factory,
                    const ZnsDevice::DeviceInfo& info,
                    const uint64_t min_zone_head, uint64_t max_zone_head);
  ~ZNSSSTableManager();
  /**
   * @brief Flushes the memtable to "n" lbas on ZNS storage and sets the
   * metadata on success.
   * @warning Please ensure that EnoughSpaceAvailable results in true and that
   * you lock before calling.
   */
  Status FlushMemTable(ZNSMemTable* mem, SSZoneMetaData* meta);
  Status RewriteSSTable(SSZoneMetaData* meta);
  Status ReadSSTable(Slice* sstable, SSZoneMetaData* meta);
  Status Get(const Slice& key, std::string* value, SSZoneMetaData* meta,
             EntryStatus* entry);
  bool EnoughSpaceAvailable(Slice slice);
  Status InvalidateSSZone(SSZoneMetaData* meta);

 private:
  friend class ZnsSSTableManagerInternal;

  void PutKVPair(std::string* dst, const Slice& key, const Slice& value);
  Status GenerateSSTableString(std::string* dst, ZNSMemTable* mem,
                               SSZoneMetaData* meta);
  Status SetWriteAddress(Slice slice);
  Status WriteSSTable(Slice content, SSZoneMetaData* meta);
  bool ValidateReadAddress(SSZoneMetaData* meta);
  Status ConsumeTail(uint64_t begin_lba, uint64_t end_lba);

  // data
  uint64_t zone_head_;
  uint64_t write_head_;
  uint64_t zone_tail_;
  uint64_t write_tail_;
  uint64_t min_zone_head_;
  uint64_t max_zone_head_;
  uint64_t zone_size_;
  uint64_t lba_size_;
  uint64_t pseudo_write_head_;
  // references
  QPairFactory* qpair_factory_;
  ZnsDevice::QPair** qpair_;
};

/**
 * @brief To be used for debugging private variables of ZNSSSTableManager only.
 */
class ZnsSSTableManagerInternal {
 public:
  static inline uint64_t GetZoneHead(ZNSSSTableManager* sstable) {
    return sstable->zone_head_;
  }
  static inline uint64_t GetWriteHead(ZNSSSTableManager* sstable) {
    return sstable->write_head_;
  }
  static inline uint64_t GetMinZoneHead(ZNSSSTableManager* sstable) {
    return sstable->min_zone_head_;
  }
  static inline uint64_t GetMaxZoneHead(ZNSSSTableManager* sstable) {
    return sstable->max_zone_head_;
  }
  static inline uint64_t GetZoneSize(ZNSSSTableManager* sstable) {
    return sstable->zone_size_;
  }
  static inline uint64_t GetLbaSize(ZNSSSTableManager* sstable) {
    return sstable->lba_size_;
  }

  static Status ConsumeTail(ZNSSSTableManager* sstable, uint64_t begin_lba,
                            uint64_t end_lba) {
    return sstable->ConsumeTail(begin_lba, end_lba);
  }
};

}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
