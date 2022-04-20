#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_WAL_MANAGER_H
#define ZNS_WAL_MANAGER_H

#include "db/zns_impl/io/device_wrapper.h"
#include "db/zns_impl/io/qpair_factory.h"
#include "db/zns_impl/memtable/zns_memtable.h"
#include "db/zns_impl/persistence/zns_committer.h"
#include "db/zns_impl/persistence/zns_wal.h"
#include "db/zns_impl/ref_counter.h"
#include "port/port.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {
class ZnsWALManager : public RefCounter {
 public:
  ZnsWALManager(QPairFactory* qpair_factory, const ZnsDevice::DeviceInfo& info,
                const uint64_t min_zone_head, uint64_t max_zone_head,
                size_t wal_count);
  // No copying or implicits
  ZnsWALManager(const ZnsWALManager&) = delete;
  ZnsWALManager& operator=(const ZnsWALManager&) = delete;
  ~ZnsWALManager();

  bool WALAvailable();
  Status NewWAL(port::Mutex* mutex_, ZNSWAL** wal);
  Status MarkWALBacked(port::Mutex* mutex_);
  Status ResetOldWALs(port::Mutex* mutex_);
  Status Recover(ZNSMemTable* mem, SequenceNumber* seq);

 private:
  std::vector<ZNSWAL*> wals;
  size_t wal_head_;
  size_t wal_tail_;
  size_t wal_count_;
};
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif