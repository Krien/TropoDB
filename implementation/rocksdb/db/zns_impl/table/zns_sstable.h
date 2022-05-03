#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_SSTABLE_H
#define ZNS_SSTABLE_H

#include "db/zns_impl/io/szd_port.h"
#include "db/zns_impl/memtable/zns_memtable.h"
#include "db/zns_impl/ref_counter.h"
#include "db/zns_impl/table/zns_zonemetadata.h"
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
  virtual uint64_t GetSize() const = 0;
};

class ZnsSSTable {
 public:
  ZnsSSTable(SZD::SZDChannelFactory* channel_factory,
             const SZD::DeviceInfo& info, const uint64_t min_zone_head,
             const uint64_t max_zone_head);
  virtual ~ZnsSSTable();
  virtual Status ReadSSTable(Slice* sstable, const SSZoneMetaData& meta) = 0;
  virtual Status Get(const InternalKeyComparator& icmp, const Slice& key,
                     std::string* value, const SSZoneMetaData& meta,
                     EntryStatus* entry) = 0;
  virtual bool EnoughSpaceAvailable(const Slice& slice) const = 0;
  virtual Status InvalidateSSZone(const SSZoneMetaData& meta) = 0;
  virtual SSTableBuilder* NewBuilder(SSZoneMetaData* meta) = 0;
  virtual Status WriteSSTable(const Slice& content, SSZoneMetaData* meta) = 0;
  virtual Iterator* NewIterator(const SSZoneMetaData& meta,
                                const InternalKeyComparator& icmp) = 0;
  virtual Status Recover() = 0;
  virtual uint64_t GetTail() const = 0;
  virtual uint64_t GetHead() const = 0;

 protected:
  friend class SSTableBuilder;

  // const after init
  const uint64_t min_zone_head_;
  const uint64_t max_zone_head_;
  const uint64_t zone_size_;
  const uint64_t lba_size_;
  const uint64_t mdts_;
  // references
  SZD::SZDChannelFactory* channel_factory_;
  SZD::SZDBuffer buffer_;
};

}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
