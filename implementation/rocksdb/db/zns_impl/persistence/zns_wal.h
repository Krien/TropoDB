#pragma once
#ifdef TROPODB_PLUGIN_ENABLED
#ifndef ZNS_WAL_H
#define ZNS_WAL_H

#include "db/zns_impl/tropodb_config.h"
#include "db/zns_impl/utils/tropodb_diagnostics.h"
#include "db/zns_impl/io/szd_port.h"
#include "db/zns_impl/memtable/tropodb_memtable.h"
#include "db/zns_impl/persistence/zns_committer.h"
#include "db/zns_impl/ref_counter.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {
/**
 * @brief
 *
 */
class ZNSWAL : public RefCounter {
 public:
  ZNSWAL(SZD::SZDChannelFactory* channel_factory, const SZD::DeviceInfo& info,
         const uint64_t min_zone_nr, const uint64_t max_zone_nr,
         const uint8_t number_of_writers,
         SZD::SZDChannel** borrowed_write_channel = nullptr);
  // No copying or implicits
  ZNSWAL(const ZNSWAL&) = delete;
  ZNSWAL& operator=(const ZNSWAL&) = delete;
  ~ZNSWAL();

#ifdef WAL_BUFFERED
  // Sync buffer from heap to I/O
  Status DataSync();
  // Append data to buffer or I/O if it is full
  Status BufferedAppend(const Slice& data);
#endif
  // Append data to storage (does not guarantee persistence)
  Status DirectAppend(const Slice& data);
  // Append data to WAL
  Status Append(const Slice& data, uint64_t seq);
  // Ensure WAL in heap buffer/DMA buffer is persisted to storage
  Status Sync();
// Replay all changes present in this WAL to the memtable
#ifdef WAL_UNORDERED
  Status ReplayUnordered(ZNSMemTable* mem, SequenceNumber* seq);
#else
  Status ReplayOrdered(ZNSMemTable* mem, SequenceNumber* seq);
#endif
  Status Replay(ZNSMemTable* mem, SequenceNumber* seq);
  // Closes the WAL gracefully (sync, free buffers)
  Status Close();
  Status Reset();
  Status Recover();
  inline bool Empty() { return log_.Empty(); }
  inline uint64_t SpaceAvailable() const { return log_.SpaceAvailable(); }
  inline size_t SpaceNeeded(const size_t size) {
#ifdef WAL_BUFFERED
    return committer_.SpaceNeeded(size + pos_ + 2 * sizeof(uint64_t));
#else
    return committer_.SpaceNeeded(size + 2 * sizeof(uint64_t));
#endif
  }
  inline size_t SpaceNeeded(const Slice& data) {
    return committer_.SpaceNeeded(data.size());
  }
  inline bool SpaceLeft(const Slice& data) {
    return committer_.SpaceEnough(SpaceNeeded(data));
  }
  inline bool SpaceLeft(const size_t size) {
    return committer_.SpaceEnough(SpaceNeeded(size));
  }
  inline ZNSDiagnostics GetDiagnostics() const {
    struct ZNSDiagnostics diag = {
        .name_ = "WAL",
        .bytes_written_ = log_.GetBytesWritten(),
        .append_operations_counter_ = log_.GetAppendOperationsCounter(),
        .bytes_read_ = log_.GetBytesRead(),
        .read_operations_counter_ = log_.GetReadOperationsCounter(),
        .zones_erased_counter_ = log_.GetZonesResetCounter(),
        .zones_erased_ = log_.GetZonesReset(),
        .append_operations_ = log_.GetAppendOperations()};
    return diag;
  }
  inline Status MarkInactive() { return FromStatus(log_.MarkInactive()); }

  // Timing
  inline TimingCounter GetTotalAppendPerfCounter() { return total_append_perf_counter_; }
  inline TimingCounter GetStorageAppendPerfCounter() { return storage_append_perf_counter_; }
  inline TimingCounter GetReplayPerfCounter() { return replay_perf_counter_; }
  inline TimingCounter GetRecoveryPerfCounter() { return recovery_perf_counter_; }
  inline TimingCounter GetResetPerfCounter() { return reset_perf_counter_; }

 private:
  // references
  SZD::SZDChannelFactory* channel_factory_;
  SZD::SZDOnceLog log_;
  ZnsCommitter committer_;
#ifdef WAL_BUFFERED
  // buffer
  bool buffered_;
  const size_t buffsize_;
  char* buf_;
  size_t pos_;
#endif
#ifdef WAL_UNORDERED
  uint32_t sequence_nr_;
#endif

  // Timing
  SystemClock* const clock_;
  TimingCounter storage_append_perf_counter_;
  TimingCounter total_append_perf_counter_;
  TimingCounter replay_perf_counter_;
  TimingCounter recovery_perf_counter_;
  TimingCounter reset_perf_counter_;
};
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
