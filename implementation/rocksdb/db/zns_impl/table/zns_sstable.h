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
  virtual void EncodeTo(std::string* dst) const = 0;
  virtual bool EncodeFrom(Slice* data) = 0;
  inline uint64_t GetTail() const { return write_tail_; }
  inline uint64_t GetHead() const { return write_head_; }

 protected:
  friend class SSTableBuilder;
  static void PutKVPair(std::string* dst, const Slice& key, const Slice& value);
  static void GeneratePreamble(std::string* dst,
                               const std::vector<uint32_t>& kv_pair_offsets_);

  // Log
  uint64_t zone_head_;
  uint64_t write_head_;
  uint64_t zone_tail_;
  uint64_t write_tail_;
  // const after init
  const uint64_t min_zone_head_;
  const uint64_t max_zone_head_;
  const uint64_t zone_size_;
  const uint64_t lba_size_;
  const uint64_t mdts_;
  // references
  SZD::SZDChannelFactory* channel_factory_;
  SZD::SZDChannel* channel_;
  SZD::SZDBuffer buffer_;
};

size_t FindSS(const InternalKeyComparator& icmp,
              const std::vector<SSZoneMetaData*>& ss, const Slice& key);
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
