#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef LN_ZNS_SSTABLE_H
#define LN_ZNS_SSTABLE_H

#include "db/zns_impl/config.h"
#include "db/zns_impl/memtable/zns_memtable.h"
#include "db/zns_impl/table/zns_sstable.h"
#include "db/zns_impl/table/zns_sstable_builder.h"
#include "db/zns_impl/table/zns_zonemetadata.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class LNZnsSSTable : public ZnsSSTable {
 public:
  LNZnsSSTable(SZD::SZDChannelFactory* channel_factory_,
               const SZD::DeviceInfo& info, const uint64_t min_zone_nr,
               const uint64_t max_zone_nr);
  ~LNZnsSSTable();
  bool EnoughSpaceAvailable(const Slice& slice) const override;
  uint64_t SpaceAvailable() const override;
  SSTableBuilder* NewBuilder(SSZoneMetaData* meta) override;
  SSTableBuilder* NewLNBuilder(SSZoneMetaData* meta);
  Iterator* NewIterator(const SSZoneMetaData& meta,
                        const Comparator* cmp) override;
  Status Get(const InternalKeyComparator& icmp, const Slice& key,
             std::string* value, const SSZoneMetaData& meta,
             EntryStatus* entry) override;
  Status ReadSSTable(Slice* sstable, const SSZoneMetaData& meta) override;
  Status InvalidateSSZone(const SSZoneMetaData& meta) override;
  Status WriteSSTable(const Slice& content, SSZoneMetaData* meta) override;
  Status WriteSSTable(const Slice& content, SSZoneMetaData* meta,
                      uint8_t writer);
  Status Recover() override;
  Status Recover(const std::string& from);
  std::string Encode();
  uint64_t GetTail() const override { return 0; }
  uint64_t GetHead() const override { return 0; }

  inline ZNSDiagnostics GetDiagnostics() const {
    struct ZNSDiagnostics diag = {
        .name_ = "LN",
        .bytes_written_ = log_.GetBytesWritten(),
        .append_operations_counter_ = log_.GetAppendOperationsCounter(),
        .bytes_read_ = log_.GetBytesRead(),
        .read_operations_counter_ = log_.GetReadOperationsCounter(),
        .zones_erased_counter_ = log_.GetZonesResetCounter(),
        .zones_erased_ = log_.GetZonesReset(),
        .append_operations_ = log_.GetAppendOperations()};
    return diag;
  }

 private:
  uint8_t request_read_queue();
  void release_read_queue(uint8_t reader);

  SZD::SZDFragmentedLog log_;
  port::Mutex mutex_;  // TODO: find a way to remove the mutex...
  port::CondVar cv_;
  std::array<uint8_t, ZnsConfig::number_of_concurrent_LN_readers> read_queue_;
};
}  // namespace ROCKSDB_NAMESPACE

#endif
#endif
