#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef L0_ZNS_SSTABLE_H
#define L0_ZNS_SSTABLE_H

#include "db/zns_impl/memtable/zns_memtable.h"
#include "db/zns_impl/table/zns_sstable.h"
#include "db/zns_impl/table/zns_zonemetadata.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
// Like a Oroborous, an entire circle without holes.
class L0ZnsSSTable : public ZnsSSTable {
 public:
  L0ZnsSSTable(SZD::SZDChannelFactory* channel_factory,
               const SZD::DeviceInfo& info, const uint64_t min_zone_head,
               const uint64_t max_zone_head);
  ~L0ZnsSSTable();
  bool EnoughSpaceAvailable(const Slice& slice) const override;
  SSTableBuilder* NewBuilder(SSZoneMetaData* meta) override;
  Iterator* NewIterator(const SSZoneMetaData& meta,
                        const InternalKeyComparator& icmp) override;
  Status Get(const InternalKeyComparator& icmp, const Slice& key,
             std::string* value, const SSZoneMetaData& meta,
             EntryStatus* entry) override;
  Status FlushMemTable(ZNSMemTable* mem, SSZoneMetaData* meta);
  Status ReadSSTable(Slice* sstable, const SSZoneMetaData& meta) override;
  Status InvalidateSSZone(const SSZoneMetaData& meta) override;
  Status WriteSSTable(const Slice& content, SSZoneMetaData* meta) override;
  void EncodeTo(std::string* dst) const override;
  bool EncodeFrom(Slice* data) override;

 private:
  friend class ZnsSSTableManagerInternal;

  class Builder;

  Status SetWriteAddress(const Slice& slice);
  Status ConsumeTail(const uint64_t begin_lba, const uint64_t end_lba);
  bool ValidateReadAddress(const SSZoneMetaData& meta) const;
  static void ParseNext(char** src, Slice* key, Slice* value);

  uint64_t pseudo_write_head_;
};

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
