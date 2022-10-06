#pragma once
#ifdef TROPODB_PLUGIN_ENABLED
#ifndef ZNS_WAL_MANAGER_H
#define ZNS_WAL_MANAGER_H

#include "db/tropodb/tropodb_config.h"
#include "db/tropodb/io/szd_port.h"
#include "db/tropodb/memtable/tropodb_memtable.h"
#include "db/tropodb/persistence/tropodb_committer.h"
#include "db/tropodb/persistence/tropodb_wal.h"
#include "db/tropodb/ref_counter.h"
#include "port/port.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {
template <std::size_t N>
class TropoWALManager : public RefCounter {
 public:
  TropoWALManager(SZD::SZDChannelFactory* channel_factory,
                const SZD::DeviceInfo& info, const uint64_t min_zone_nr,
                const uint64_t max_zone_nr);
  // No copying or implicits
  TropoWALManager(const TropoWALManager&) = delete;
  TropoWALManager& operator=(const TropoWALManager&) = delete;
  ~TropoWALManager();

  bool WALAvailable();
  TropoWAL* GetCurrentWAL(port::Mutex* mutex_);
  Status NewWAL(port::Mutex* mutex_, TropoWAL** wal);
  Status ResetOldWALs(port::Mutex* mutex_);
  Status Recover(TropoMemtable* mem, SequenceNumber* seq);

  std::vector<TropoDiagnostics> IODiagnostics();
  std::vector<std::pair<std::string, const TimingCounter>> GetAdditionalWALStatistics();

 private:
  std::array<TropoWAL*, N> wals_;
#ifdef WAL_MANAGER_MANAGES_CHANNELS
  SZD::SZDChannelFactory* channel_factory_;
  SZD::SZDChannel** write_channels_;
#endif
  size_t wal_head_;
  size_t wal_tail_;
  TropoWAL* current_wal_;
};
}  // namespace ROCKSDB_NAMESPACE
#include "db/tropodb/persistence/tropodb_wal_manager.ipp"
#endif
#endif