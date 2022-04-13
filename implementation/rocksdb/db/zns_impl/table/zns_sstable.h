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

int FindSS(const InternalKeyComparator& icmp,
           const std::vector<SSZoneMetaData*>& ss, const Slice& key);
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
