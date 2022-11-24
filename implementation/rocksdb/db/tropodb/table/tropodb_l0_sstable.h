#pragma once
#ifdef TROPODB_PLUGIN_ENABLED
#ifndef TROPODB_L0_SSTABLE_H
#define TROPODB_L0_SSTABLE_H

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

struct DeferredFlush {
  port::Mutex mutex_;
  port::CondVar new_task_;  // In case deferred is waiting for main
  bool last_;        // Signal that we are done and deferred should be gone
  bool done_;        // Signal that deferred is done
  std::vector<TropoSSTableBuilder*> deferred_builds_; // All deferred builders to use for compaction
  uint8_t index_;
  std::vector<SSZoneMetaData>* metas_;
  DeferredFlush() : new_task_(&mutex_), last_(false), done_(false), index_(0), metas_(nullptr) {
    deferred_builds_.clear();
  }
};

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
                       uint8_t parallel_number, Env* env);
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
  
  // Timing
  inline TimingCounter GetFlushPreparePerfCounter() { return flush_prepare_perf_counter_; }
  inline TimingCounter GetFlushMergePerfCounter() { return flush_merge_perf_counter_; }
  inline TimingCounter GetFlushHeaderPerfCounter() { return flush_header_perf_counter_; }
  inline TimingCounter GetFlushWritePerfCounter() { return flush_write_perf_counter_; }
  inline TimingCounter GetFlushFinishPerfCounter() { return flush_finish_perf_counter_; }

 private:
  friend class TropoSSTableManagerInternal;
  static void DeferFlushWrite(void* deferred_flush);
  Status FlushSSTable(TropoSSTableBuilder** builder, std::vector<SSZoneMetaData*>& new_metas, std::vector<SSZoneMetaData>& metas);

  uint8_t request_read_queue();
  void release_read_queue(uint8_t reader);

  SZD::SZDCircularLog log_;
  uint64_t zasl_;
  uint64_t lba_size_;
  uint64_t zone_size_;
  // light queue inevitable as we can have ONE reader accesssed by ONE thread
  // concurrently.
  port::Mutex mutex_;
  port::CondVar cv_;
  std::array<uint8_t, TropoDBConfig::number_of_concurrent_L0_readers> read_queue_;
  // deferred
  DeferredFlush deferred_;
  // timing
  SystemClock* const clock_;
  TimingCounter flush_prepare_perf_counter_;
  TimingCounter flush_merge_perf_counter_;
  TimingCounter flush_header_perf_counter_;
  TimingCounter flush_write_perf_counter_;
  TimingCounter flush_finish_perf_counter_;
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
