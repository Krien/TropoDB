#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_SSTABLE_MANAGER_H
#define ZNS_SSTABLE_MANAGER_H

#include "db/zns_impl/config.h"
#include "db/zns_impl/io/szd_port.h"
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
  ZNSSSTableManager(
      SZD::SZDChannelFactory* channel_factory, const SZD::DeviceInfo& info,
      const std::pair<uint64_t, uint64_t> ranges[ZnsConfig::level_count]);
  ~ZNSSSTableManager();

  bool EnoughSpaceAvailable(const size_t level, const Slice& slice) const;
  Status FlushMemTable(ZNSMemTable* mem, SSZoneMetaData* meta) const;
  Status CopySSTable(const size_t level1, const size_t level2,
                     SSZoneMetaData* meta) const;
  Status WriteSSTable(const size_t level, const Slice& content,
                      SSZoneMetaData* meta) const;
  Status ReadSSTable(const size_t level, Slice* sstable,
                     const SSZoneMetaData& meta) const;
  Status Get(const size_t level, const InternalKeyComparator& icmp,
             const Slice& key, std::string* value, const SSZoneMetaData& meta,
             EntryStatus* entry) const;
  Status InvalidateSSZone(const size_t level, const SSZoneMetaData& meta) const;
  Status SetValidRangeAndReclaim(const size_t level, const uint64_t tail,
                                 const uint64_t head) const;
  L0ZnsSSTable* GetL0SSTableLog() const;
  Iterator* NewIterator(const size_t level, const SSZoneMetaData& meta,
                        const InternalKeyComparator& icmp) const;
  SSTableBuilder* NewBuilder(const size_t level, SSZoneMetaData* meta) const;
  // Used for persistency
  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& data);
  // Used for compaction
  double GetFractionFilled(const size_t level) const;
  // Used for cleaning
  void GetDefaultRange(const size_t level,
                       std::pair<uint64_t, uint64_t>* range) const;
  void GetRange(const size_t level, const std::vector<SSZoneMetaData*>& metas,
                std::pair<uint64_t, uint64_t>* range) const;

 private:
  // wals
  ZnsSSTable* sstable_wal_level_[ZnsConfig::level_count];
  // references
  SZD::SZDChannelFactory* channel_factory_;
  std::pair<uint64_t, uint64_t> ranges_[ZnsConfig::level_count];
};
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
