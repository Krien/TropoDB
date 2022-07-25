#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_WAL_H
#define ZNS_WAL_H

#define WAL_BUFFERED
#define WAL_UNORDERED

#include "db/zns_impl/diagnostics.h"
#include "db/zns_impl/io/szd_port.h"
#include "db/zns_impl/memtable/zns_memtable.h"
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

  inline Status Reset() {
    uint64_t before = clock_->NowMicros();
    Status s = FromStatus(log_.ResetAll());
#ifdef WAL_UNORDERED
    sequence_nr_ = 0;
#endif
    uint64_t value = clock_->NowMicros() - before;
    reset_nummers_ += 1;
    reset_time_ += value;
    reset_time_squares_ += value * value;
    return s;
  }
  inline Status Recover() {
    uint64_t before = clock_->NowMicros();
    Status s = FromStatus(log_.RecoverPointers());
    recovery_time_ = clock_->NowMicros() - before;
    return s;
  }
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
  inline uint64_t TimeSpendWaitingOnStorageNumber() { return num_; }
  inline uint64_t TimeSpendWaitingOnStorage() { return sum_; }
  inline uint64_t TimeSpendWaitingOnStorageSquared() { return sum_squares_; }
  inline uint64_t TimeSpendWaitingOnStorageNumberTotal() { return num_total_; }
  inline uint64_t TimeSpendWaitingOnStorageTotal() { return sum_total_; }
  inline uint64_t TimeSpendWaitingOnStorageSquaredTotal() {
    return sum_squares_total_;
  }
  inline uint64_t TimeSpendWaitingOnResets() { return reset_time_; }
  inline uint64_t TimeSpendWaitingOnResetsSquared() {
    return reset_time_squares_;
  }
  inline uint64_t TimeSpendWaitingOnResetsNumber() { return reset_nummers_; }
  inline uint64_t TimeSpendReplaying() { return replay_time_; }
  inline uint64_t TimeSpendRecovering() { return recovery_time_; }

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
  uint64_t num_{0};
  uint64_t sum_{0};
  uint64_t sum_squares_{0};
  uint64_t num_total_{0};
  uint64_t sum_total_{0};
  uint64_t sum_squares_total_{0};
  uint64_t replay_time_{0};
  uint64_t recovery_time_{0};
  uint64_t reset_time_{0};
  uint64_t reset_time_squares_{0};
  uint64_t reset_nummers_{0};
};
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif

// OBSOLETE, uses memory buffer, which we do not want
// Status ObsoleteDirectAppend(const Slice& data);
// Status ObsoleteAppend(const Slice& data);
// Status ObsoleteSync();
// Status ObsoleteReplay(ZNSMemTable* mem, SequenceNumber* seq);
