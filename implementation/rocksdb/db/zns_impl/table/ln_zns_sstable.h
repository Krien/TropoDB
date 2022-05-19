#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef LN_ZNS_SSTABLE_H
#define LN_ZNS_SSTABLE_H

#include "db/zns_impl/memtable/zns_memtable.h"
#include "db/zns_impl/table/zns_sstable.h"
#include "db/zns_impl/table/zns_sstable_builder.h"
#include "db/zns_impl/table/zns_zonemetadata.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// LN can be different regions of different sizes, so we need indirection...
struct SSZoneMetaDataLN : public SSZoneMetaData {
  SSZoneMetaDataLN() : lba_regions(0) {}
  uint8_t lba_regions;           // Number of start lbas (legal from 1 to 8)
  uint64_t lbas[8];              // start lbas (can be multiple, up to 8)
  uint64_t lba_region_sizes[8];  // Size in zones of an lbas region
  bool DecodeFrom(Slice* input) override;
  void Encode(std::string* dst) const override;
};

class LNZnsSSTable : public ZnsSSTable {
 public:
  LNZnsSSTable(SZD::SZDChannelFactory* channel_factory_,
               const SZD::DeviceInfo& info, const uint64_t min_zone_nr,
               const uint64_t max_zone_nr);
  ~LNZnsSSTable();
  bool EnoughSpaceAvailable(const Slice& slice) const override;
  SSTableBuilder* NewBuilder(SSZoneMetaData* meta) override;
  Iterator* NewIterator(const SSZoneMetaData* meta,
                        const Comparator* cmp) override;
  Status Get(const InternalKeyComparator& icmp, const Slice& key,
             std::string* value, const SSZoneMetaData* meta,
             EntryStatus* entry) override;
  Status ReadSSTable(Slice* sstable, const SSZoneMetaData* meta) override;
  Status InvalidateSSZone(const SSZoneMetaData* meta) override;
  Status WriteSSTable(const Slice& content, SSZoneMetaData* meta) override;
  Status Recover() override;
  uint64_t GetTail() const override { return 0; }
  uint64_t GetHead() const override { return 0; }

 private:
  SZD::SZDFragmentedLog log_;
  port::Mutex mutex_;  // TODO: find a way to remove the mutex...
};
}  // namespace ROCKSDB_NAMESPACE

#endif
#endif
