#pragma once
#ifdef TROPODB_PLUGIN_ENABLED
#ifndef TROPODB_WAL_H
#define TROPODB_WAL_H

#include "db/tropodb/tropodb_config.h"
#include "db/tropodb/utils/tropodb_diagnostics.h"
#include "db/tropodb/io/szd_port.h"
#include "db/tropodb/memtable/tropodb_memtable.h"
#include "db/tropodb/persistence/tropodb_committer.h"
#include "db/tropodb/ref_counter.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {
/**
 * @brief
 *
 */
class TropoWAL : public RefCounter {
 public:
  TropoWAL(SZD::SZDChannelFactory* channel_factory, const SZD::DeviceInfo& info,
         const uint64_t min_zone_nr, const uint64_t max_zone_nr,
         const bool use_buffer_, const bool allow_unordered_,
         SZD::SZDChannel* borrowed_write_channel = nullptr);
  // No copying or implicits
  TropoWAL(const TropoWAL&) = delete;
  TropoWAL& operator=(const TropoWAL&) = delete;
  ~TropoWAL();

  // Append data to WAL
  Status Append(const Slice& data, uint64_t seq);
  // Sync buffer from heap to I/O
  Status DataSync();
  // Ensure WAL in heap buffer/DMA buffer is persisted to storage
  Status Sync();
// Replay all changes present in this WAL to the memtable
  Status Replay(TropoMemtable* mem, SequenceNumber* seq);
  // Closes the WAL gracefully (sync, free buffers)
  Status Close();
  Status Reset();
  Status Recover();

  // status
  inline bool Empty() { return log_.Empty(); }
  inline uint64_t SpaceAvailable() const { return log_.SpaceAvailable(); }
  inline size_t SpaceNeeded(const size_t size) {
    return committer_.SpaceNeeded(size + buff_pos_ + 2 * sizeof(uint64_t));
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
  inline TropoDiagnostics GetDiagnostics() const {
    struct TropoDiagnostics diag = {
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
  // Append data to buffer or I/O if it is full
  Status BufferedAppend(const Slice& data);
  // Append data to storage (does not guarantee persistence)
  Status DirectAppend(const Slice& data);
  Status ReplayUnordered(TropoMemtable* mem, SequenceNumber* seq);
  Status ReplayOrdered(TropoMemtable* mem, SequenceNumber* seq);

  // references
  SZD::SZDChannelFactory* channel_factory_;
  SZD::SZDOnceLog log_;
  TropoCommitter committer_;
  // buffer
  bool buffered_;
  const size_t buffsize_;
  char* buff_;
  size_t buff_pos_;
  // unordered
  bool unordered_;
  uint32_t sequence_nr_;
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
