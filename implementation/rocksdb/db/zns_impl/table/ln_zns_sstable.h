#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef LN_ZNS_SSTABLE_H
#define LN_ZNS_SSTABLE_H

#include "db/zns_impl/memtable/zns_memtable.h"
#include "db/zns_impl/table/zns_sstable.h"
#include "db/zns_impl/table/zns_zonemetadata.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
class LNZnsSSTable : public ZnsSSTable {
 public:
  LNZnsSSTable(SZD::SZDChannelFactory* channel_factory_,
               const SZD::DeviceInfo& info, const uint64_t min_zone_head,
               const uint64_t max_zone_head);
  ~LNZnsSSTable();
  bool EnoughSpaceAvailable(const Slice& slice) const override;
  SSTableBuilder* NewBuilder(SSZoneMetaData* meta) override;
  Iterator* NewIterator(const SSZoneMetaData& meta,
                        const InternalKeyComparator& icmp) const override;
  Status Get(const InternalKeyComparator& icmp, const Slice& key,
             std::string* value, const SSZoneMetaData& meta,
             EntryStatus* entry) const override;
  Status FlushMemTable(ZNSMemTable* mem, SSZoneMetaData* meta);
  Status ReadSSTable(Slice* sstable, const SSZoneMetaData& meta) const override;
  Status InvalidateSSZone(const SSZoneMetaData& meta) override;
  Status WriteSSTable(const Slice& content, SSZoneMetaData* meta) override;
  void EncodeTo(std::string* dst) const override;
  bool EncodeFrom(Slice* data) override;

 private:
  class Builder;

  Status SetWriteAddress(const Slice& slice);
  Status ConsumeTail(const uint64_t begin_lba, const uint64_t end_lba);
  bool ValidateReadAddress(const SSZoneMetaData& meta) const;
  static void ParseNext(char** src, Slice* key, Slice* value);

  uint64_t pseudo_write_head_;
};
}  // namespace ROCKSDB_NAMESPACE

#endif
#endif
