#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef LN_ZNS_SSTABLE_H
#define LN_ZNS_SSTABLE_H

#include "db/zns_impl/qpair_factory.h"
#include "db/zns_impl/table/zns_sstable.h"
#include "db/zns_impl/zns_memtable.h"
#include "db/zns_impl/zns_zonemetadata.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
class LNZnsSSTable : public ZnsSSTable {
 public:
  LNZnsSSTable(QPairFactory* qpair_factory, const ZnsDevice::DeviceInfo& info,
               const uint64_t min_zone_head, uint64_t max_zone_head);
  ~LNZnsSSTable();
  bool EnoughSpaceAvailable(Slice slice) override;
  SSTableBuilder* NewBuilder(SSZoneMetaData* meta) override;
  Iterator* NewIterator(SSZoneMetaData* meta) override;
  Status Get(const InternalKeyComparator& icmp, const Slice& key,
             std::string* value, SSZoneMetaData* meta,
             EntryStatus* entry) override;
  Status FlushMemTable(ZNSMemTable* mem, SSZoneMetaData* meta);
  Status ReadSSTable(Slice* sstable, SSZoneMetaData* meta) override;
  Status InvalidateSSZone(SSZoneMetaData* meta);
  Status WriteSSTable(Slice content, SSZoneMetaData* meta) override;
  void EncodeTo(std::string* dst) override;
  bool EncodeFrom(Slice* data) override;

 private:
  class Builder;

  Status SetWriteAddress(Slice slice);
  Status ConsumeTail(uint64_t begin_lba, uint64_t end_lba);
  bool ValidateReadAddress(SSZoneMetaData* meta);
  static void ParseNext(char** src, Slice* key, Slice* value);

  uint64_t pseudo_write_head_;
  port::Mutex mutex_;
};
}  // namespace ROCKSDB_NAMESPACE

#endif
#endif
