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

  Status DirectAppend(const Slice& data);
  Status Append(const Slice& data);
  Status Close();
  Status Sync();

  inline Status Reset() {
    pos_ = 0;
    return FromStatus(log_.ResetAll());
  }
  inline Status Recover() { return FromStatus(log_.RecoverPointers()); }
  inline bool Empty() { return log_.Empty() && pos_ == 0; }
  inline bool SpaceLeft(const Slice& data) {
    return log_.SpaceLeft(data.size() + pos_);
  }
  inline ZNSDiagnostics GetDiagnostics() const {
    struct ZNSDiagnostics diag = {
        .name_ = "WAL",
        .bytes_written_ = log_.GetBytesWritten(),
        .append_operations_ = log_.GetAppendOperations(),
        .bytes_read_ = log_.GetBytesRead(),
        .read_operations_ = log_.GetReadOperations(),
        .zones_erased_ = log_.GetZonesReset()};
    return diag;
  }

  Status Replay(ZNSMemTable* mem, SequenceNumber* seq);

 private:
  // buffer
  const size_t buffsize_;
  WriteBatch batch_;
  char* buf_;
  size_t pos_;
  // references
  SZD::SZDChannelFactory* channel_factory_;
  SZD::SZDOnceLog log_;
  ZnsCommitter committer_;
};
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
