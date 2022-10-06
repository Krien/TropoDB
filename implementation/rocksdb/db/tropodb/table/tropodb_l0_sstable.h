#pragma once
#ifdef TROPODB_PLUGIN_ENABLED
#ifndef L0_ZNS_SSTABLE_H
#define L0_ZNS_SSTABLE_H

#include "db/tropodb/tropodb_config.h"
#include "db/tropodb/memtable/tropodb_memtable.h"
#include "db/tropodb/persistence/tropodb_committer.h"
#include "db/tropodb/table/tropodb_sstable.h"
#include "db/tropodb/table/tropodb_sstable_builder.h"
#include "db/tropodb/table/tropodb_zonemetadata.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
namespace ROCKSDB_NAMESPACE {

// Like a Oroborous, an entire circle without holes.
class TropoL0SSTable : public TropoSSTable {
 public:
  TropoL0SSTable(SZD::SZDChannelFactory* channel_factory,
               const SZD::DeviceInfo& info, const uint64_t min_zone_nr,
               const uint64_t max_zone_nr);
  ~TropoL0SSTable();
  bool EnoughSpaceAvailable(const Slice& slice) const override;
  uint64_t SpaceAvailable() const override;
  TropoSSTableBuilder* NewBuilder(SSZoneMetaData* meta) override;
  Iterator* NewIterator(const SSZoneMetaData& meta,
                        const Comparator* cmp) override;
  Status Get(const InternalKeyComparator& icmp, const Slice& key,
             std::string* value, const SSZoneMetaData& meta,
             EntryStatus* entry) override;
  Status FlushMemTable(TropoMemtable* mem, std::vector<SSZoneMetaData>& metas,
                       uint8_t parallel_number);
  Status ReadSSTable(Slice* sstable, const SSZoneMetaData& meta) override;
  Status TryInvalidateSSZones(const std::vector<SSZoneMetaData*>& metas,
                              std::vector<SSZoneMetaData*>& remaining_metas);
  Status InvalidateSSZone(const SSZoneMetaData& meta) override;
  Status WriteSSTable(const Slice& content, SSZoneMetaData* meta) override;
  Status Recover() override;
  uint64_t GetTail() const override { return log_.GetWriteTail(); }
  uint64_t GetHead() const override { return log_.GetWriteHead(); }

  inline TropoDiagnostics GetDiagnostics() const override {
    struct TropoDiagnostics diag = {
        .name_ = "L0",
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
  friend class TropoSSTableManagerInternal;

  uint8_t request_read_queue();
  void release_read_queue(uint8_t reader);

  SZD::SZDCircularLog log_;
#ifdef USE_COMMITTER
  TropoCommitter committer_;
#endif
  uint64_t zasl_;
  uint64_t lba_size_;
  uint64_t zone_size_;
  // light queue inevitable as we can have ONE reader accesssed by ONE thread
  // concurrently.
  port::Mutex mutex_;
  port::CondVar cv_;
  std::array<uint8_t, TropoDBConfig::number_of_concurrent_L0_readers> read_queue_;
};

/**
 * @brief To be used for debugging private variables of TropoSSTableManager only.
 */
class TropoSSTableManagerInternal {
 public:
  static inline uint64_t GetMinZoneHead(TropoL0SSTable* sstable) {
    return sstable->min_zone_head_;
  }
  static inline uint64_t GetMaxZoneHead(TropoL0SSTable* sstable) {
    return sstable->max_zone_head_;
  }
  static inline uint64_t GetZoneSize(TropoL0SSTable* sstable) {
    return sstable->zone_cap_;
  }
  static inline uint64_t GetLbaSize(TropoL0SSTable* sstable) {
    return sstable->lba_size_;
  }
};

}  // namespace ROCKSDB_NAMESPACE

#endif
#endif
