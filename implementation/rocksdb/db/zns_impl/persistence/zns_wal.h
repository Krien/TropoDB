#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_WAL_H
#define ZNS_WAL_H

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
         const uint64_t min_zone_nr, const uint64_t max_zone_nr);
  // No copying or implicits
  ZNSWAL(const ZNSWAL&) = delete;
  ZNSWAL& operator=(const ZNSWAL&) = delete;
  ~ZNSWAL();

  Status DirectAppend(const Slice& data, uint64_t seq);
  Status Sync();
  Status Replay(ZNSMemTable* mem, SequenceNumber* seq);

  // OBSOLETE, uses memory buffer, which we do not want
  // Status ObsoleteDirectAppend(const Slice& data);
  // Status ObsoleteAppend(const Slice& data);
  // Status ObsoleteSync();
  // Status ObsoleteReplay(ZNSMemTable* mem, SequenceNumber* seq);

  Status Close();

  inline Status Reset() { return FromStatus(log_.ResetAll()); }
  inline Status Recover() { return FromStatus(log_.RecoverPointers()); }
  inline bool Empty() { return log_.Empty(); }
  inline uint64_t SpaceAvailable() const { return log_.SpaceAvailable(); }
  inline bool SpaceLeft(const Slice& data) {
    return log_.SpaceLeft(data.size());
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

 private:
  // buffer
  // const size_t buffsize_;
  // WriteBatch batch_;
  // char* buf_;
  // size_t pos_;
  // references
  SZD::SZDChannelFactory* channel_factory_;
  SZD::SZDOnceLog log_;
  ZnsCommitter committer_;
};
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
