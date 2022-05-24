#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef L0_ZNS_SSTABLE_H
#define L0_ZNS_SSTABLE_H

#include "db/zns_impl/memtable/zns_memtable.h"
#include "db/zns_impl/persistence/zns_committer.h"
#include "db/zns_impl/table/zns_sstable.h"
#include "db/zns_impl/table/zns_sstable_builder.h"
#include "db/zns_impl/table/zns_zonemetadata.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
namespace ROCKSDB_NAMESPACE {

static constexpr uint8_t number_of_concurrent_readers = 4;

// Like a Oroborous, an entire circle without holes.
class L0ZnsSSTable : public ZnsSSTable {
 public:
  L0ZnsSSTable(SZD::SZDChannelFactory* channel_factory,
               const SZD::DeviceInfo& info, const uint64_t min_zone_nr,
               const uint64_t max_zone_nr);
  ~L0ZnsSSTable();
  bool EnoughSpaceAvailable(const Slice& slice) const override;
  uint64_t SpaceAvailable() const override;
  SSTableBuilder* NewBuilder(SSZoneMetaData* meta) override;
  Iterator* NewIterator(const SSZoneMetaData& meta,
                        const Comparator* cmp) override;
  Status Get(const InternalKeyComparator& icmp, const Slice& key,
             std::string* value, const SSZoneMetaData& meta,
             EntryStatus* entry) override;
  Status FlushMemTable(ZNSMemTable* mem, SSZoneMetaData* meta);
  Status ReadSSTable(Slice* sstable, const SSZoneMetaData& meta) override;
  Status InvalidateSSZone(const SSZoneMetaData& meta) override;
  Status WriteSSTable(const Slice& content, SSZoneMetaData* meta) override;
  Status Recover() override;
  uint64_t GetTail() const override { return log_.GetWriteTail(); }
  uint64_t GetHead() const override { return log_.GetWriteHead(); }

  inline ZNSDiagnostics GetDiagnostics() const override {
    struct ZNSDiagnostics diag = {.name_ = "L0",
                                  .bytes_written_ = log_.GetBytesWritten(),
                                  .bytes_read_ = log_.GetBytesRead(),
                                  .zones_erased_ = log_.GetZonesReset()};
    return diag;
  }

 private:
  friend class ZnsSSTableManagerInternal;

  uint8_t request_read_queue();
  void release_read_queue();

  SZD::SZDCircularLog log_;
  ZnsCommitter committer_;
  // light queue inevitable as we can have ONE reader accesssed by ONE thread
  // concurrently.
  port::Mutex mutex_;
  port::CondVar cv_;
  uint8_t read_queue_;
};

/**
 * @brief To be used for debugging private variables of ZNSSSTableManager only.
 */
class ZnsSSTableManagerInternal {
 public:
  static inline uint64_t GetMinZoneHead(L0ZnsSSTable* sstable) {
    return sstable->min_zone_head_;
  }
  static inline uint64_t GetMaxZoneHead(L0ZnsSSTable* sstable) {
    return sstable->max_zone_head_;
  }
  static inline uint64_t GetZoneSize(L0ZnsSSTable* sstable) {
    return sstable->zone_cap_;
  }
  static inline uint64_t GetLbaSize(L0ZnsSSTable* sstable) {
    return sstable->lba_size_;
  }
};

}  // namespace ROCKSDB_NAMESPACE

#endif
#endif
