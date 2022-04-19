#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_SSTABLE_MANAGER_H
#define ZNS_SSTABLE_MANAGER_H

#include "db/zns_impl/io/device_wrapper.h"
#include "db/zns_impl/io/qpair_factory.h"
#include "db/zns_impl/memtable/zns_memtable.h"
#include "db/zns_impl/ref_counter.h"
#include "db/zns_impl/table/l0_zns_sstable.h"
#include "db/zns_impl/table/ln_zns_sstable.h"
#include "db/zns_impl/table/zns_sstable.h"
#include "db/zns_impl/table/zns_zonemetadata.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
class ZnsSSTableManagerInternal;
class ZNSSSTableManager : public RefCounter {
 public:
  ZNSSSTableManager(QPairFactory* qpair_factory,
                    const ZnsDevice::DeviceInfo& info,
                    std::pair<uint64_t, uint64_t> ranges[7]);
  ~ZNSSSTableManager();

  bool EnoughSpaceAvailable(size_t level, Slice slice);
  Status FlushMemTable(ZNSMemTable* mem, SSZoneMetaData* meta);
  Status CopySSTable(size_t l1, size_t l2, SSZoneMetaData* meta);
  Status WriteSSTable(size_t l, Slice content, SSZoneMetaData* meta);
  Status ReadSSTable(size_t level, Slice* sstable, SSZoneMetaData* meta);
  Status Get(size_t level, const InternalKeyComparator& icmp, const Slice& key,
             std::string* value, SSZoneMetaData* meta, EntryStatus* entry);
  Status InvalidateSSZone(size_t level, SSZoneMetaData* meta);
  L0ZnsSSTable* GetL0SSTableLog();
  Iterator* NewIterator(size_t level, SSZoneMetaData* meta);
  SSTableBuilder* NewBuilder(size_t level, SSZoneMetaData* meta);
  // Used for persistency
  void EncodeTo(std::string* dst);
  Status DecodeFrom(const Slice& data);

 private:
  // wals
  ZnsSSTable* sstable_wal_level_[7];
  // references
  QPairFactory* qpair_factory_;
};
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
