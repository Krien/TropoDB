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
class ZNSSSTableManager : public RefCounter {
 public:
  ZNSSSTableManager(QPairFactory* qpair_factory,
                    const ZnsDevice::DeviceInfo& info,
                    const uint64_t min_zone_head, uint64_t max_zone_head);
  ~ZNSSSTableManager();
  Status FlushMemTable(ZNSMemTable* mem, SSZoneMetaData* meta);
  Status Get(const Slice& key, std::string* value, SSZoneMetaData* meta);
  Status ReadSSTable(Slice* sstable, SSZoneMetaData* meta);

 private:
  // data
  uint64_t zone_head_;
  uint64_t write_head_;
  uint64_t min_zone_head_;
  uint64_t max_zone_head_;
  uint64_t zone_size_;
  uint64_t lba_size_;
  // references
  QPairFactory* qpair_factory_;
  ZnsDevice::QPair** qpair_;
};
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif