#pragma once
#ifdef TROPODB_PLUGIN_ENABLED
#ifndef ZNS_WAL_MANAGER_IPP
#define ZNS_WAL_MANAGER_IPP

#include <iomanip>
#include <iostream>

#include "db/write_batch_internal.h"
#include "db/zns_impl/tropodb_config.h"
#include "db/zns_impl/io/szd_port.h"
#include "db/zns_impl/memtable/zns_memtable.h"
#include "db/zns_impl/persistence/zns_committer.h"
#include "db/zns_impl/persistence/zns_wal.h"
#include "db/zns_impl/persistence/zns_wal_manager.h"
#include "db/zns_impl/utils/tropodb_logger.h"
#include "port/port.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/write_batch.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {
template <std::size_t N>
ZnsWALManager<N>::ZnsWALManager(SZD::SZDChannelFactory* channel_factory,
                                const SZD::DeviceInfo& info,
                                const uint64_t min_zone_nr,
                                const uint64_t max_zone_nr)
    :
#ifdef WAL_MANAGER_MANAGES_CHANNELS
      channel_factory_(channel_factory),
#endif
      wal_head_(0),
      wal_tail_(N - 1),
      current_wal_(nullptr) {
  assert((max_zone_nr - min_zone_nr) % N == 0);
  uint64_t wal_range = (max_zone_nr - min_zone_nr) / N;
  assert(wal_range % info.zone_cap_ == 0);
  uint64_t wal_walker = min_zone_nr;

#ifdef WAL_MANAGER_MANAGES_CHANNELS
  // WalManager is the boss of the channels. Prevents stale channels.
  write_channels_ = new SZD::SZDChannel*[ZnsConfig::wal_concurrency];
  channel_factory_->Ref();
  for (size_t i = 0; i < ZnsConfig::wal_concurrency; ++i) {
    channel_factory_->register_channel(&write_channels_[i], min_zone_nr,
                                       max_zone_nr, ZnsConfig::wal_preserve_dma,
#ifdef WAL_UNORDERED
                                       ZnsConfig::wal_iodepth
#else
                                       1
#endif
    );
  }
#endif
  for (size_t i = 0; i < N; ++i) {
    ZNSWAL* newwal =
        new ZNSWAL(channel_factory, info, wal_walker, wal_walker + wal_range,
#ifdef WAL_MANAGER_MANAGES_CHANNELS
                   ZnsConfig::wal_concurrency, write_channels_
#else
                   ZnsConfig::wal_concurrency / N, nullptr
#endif
        );
    newwal->Ref();
    wals_[i] = newwal;
    wal_walker += wal_range;
  }
}

template <std::size_t N>
ZnsWALManager<N>::~ZnsWALManager() {
  for (auto i = wals_.begin(); i != wals_.end(); ++i) {
    if ((*i) != nullptr) {
      (*i)->Sync();
      (*i)->Unref();
    }
  }
#ifdef WAL_MANAGER_MANAGES_CHANNELS
  for (size_t i = 0; i < ZnsConfig::wal_concurrency; ++i) {
    channel_factory_->unregister_channel(write_channels_[i]);
  }
  channel_factory_->Unref();
#endif
}

template <std::size_t N>
bool ZnsWALManager<N>::WALAvailable() {
  // not allowed to happen
  if (wal_head_ == wal_tail_) {
    assert(false);
    return false;
    // [vvT..Hvvvv]
  } else if (wal_head_ > wal_tail_) {
    return N > wal_head_ || wal_tail_ > 0;
  } else {
    return wal_tail_ > wal_head_ + 1;
  }
}

template <std::size_t N>
Status ZnsWALManager<N>::NewWAL(port::Mutex* mutex_, ZNSWAL** wal) {
  mutex_->AssertHeld();
  if (!WALAvailable()) {
    return Status::Busy();
  }
  if (current_wal_ != nullptr) {
    current_wal_->Close();
  }
  current_wal_ = wals_[wal_head_];
  if (!current_wal_->Empty()) {
    assert(false);
    TROPODB_ERROR("ERROR: WAL: New WAL is not empty\n");
  }
  wal_head_++;
  if (wal_head_ == N) {
    wal_head_ = 0;
  }
  *wal = current_wal_;
  return Status::OK();
}

template <std::size_t N>
ZNSWAL* ZnsWALManager<N>::GetCurrentWAL(port::Mutex* mutex_) {
  mutex_->AssertHeld();
  if (current_wal_ != nullptr) {
    return current_wal_;
  }
  // no current
  if ((wal_head_ == 0 && wal_tail_ == N - 1) ||
      (wal_head_ != 0 && wal_head_ - 1 == wal_tail_)) {
    NewWAL(mutex_, &current_wal_);
  } else {
    current_wal_ = wals_[wal_head_ == 0 ? N - 1 : wal_head_ - 1];
  }
  return current_wal_;
}

template <std::size_t N>
Status ZnsWALManager<N>::ResetOldWALs(port::Mutex* mutex_) {
  mutex_->AssertHeld();
  if (wal_tail_ > wal_head_) {
    while ((wal_tail_ < N && wal_head_ > 0) || (wal_tail_ < N - 1)) {
      if (wals_[wal_tail_]->Getref() > 1) {
        return Status::OK();
      }
      wals_[wal_tail_]->Reset();
      wal_tail_++;
    }
  }
  if (wal_tail_ == N) {
    wal_tail_ = 0;
  }
  // +2 because wal_head_ -1 can be filled.
  while (wal_head_ > wal_tail_ + 2) {
    if (wals_[wal_tail_]->Getref() > 1) {
      return Status::OK();
    }
    wals_[wal_tail_]->Reset();
    wal_tail_++;
  }
  return Status::OK();
}

template <std::size_t N>
Status ZnsWALManager<N>::Recover(ZNSMemTable* mem, SequenceNumber* seq) {
  Status s = Status::OK();
  // Recover WAL pointers
  for (auto i = wals_.begin(); i != wals_.end(); i++) {
    s = (*i)->Recover();
    if (!s.ok()) return s;
  }
  // Find head and tail of manager
  bool first_non_empty = false;
  bool first_empty_after_non_empty = false;
  for (size_t i = 0; i < wals_.size(); i++) {
    if (!wals_[i]->Empty() && !first_non_empty) {
      // potential tail or head
      first_non_empty = true;
      wal_head_ = i + 1;
      if (i > 0) {
        wal_tail_ = i;
      }

    }
    else if (!wals_[i]->Empty() && first_empty_after_non_empty) {
      // a gap in the middle?
      wal_tail_ = i;
      break;

    }
    else if (!wals_[i]->Empty() && first_non_empty) {
      // the head is moving one further
      wal_head_ = i + 1;
    }  
    else if (wals_[i]->Empty() && !first_empty_after_non_empty &&
             first_non_empty) {
      // head can not move further
      first_empty_after_non_empty = true;
    }
  }
  if (wal_head_ >= N) {
    wal_head_ = 0;
  }

  // Replay from head to tail, to be sure replay all for now...
  size_t i = wal_head_ == 0 ? N - 1 : wal_head_ - 1;
  for (; i != wal_head_; i = i == 0 ? N - 1 : i - 1) {
    s = wals_[i]->Replay(mem, seq);
    if (!s.ok()) return s;
  }
  s = wals_[wal_head_]->Replay(mem, seq);
  return s;
}

template <std::size_t N>
std::vector<ZNSDiagnostics> ZnsWALManager<N>::IODiagnostics() {
  std::vector<ZNSDiagnostics> diags;
#ifdef WAL_MANAGER_MANAGES_CHANNELS
  ZNSDiagnostics diag;
  diag.name_ = "WALS";
  diag.append_operations_counter_ = 0;
  diag.bytes_written_ = 0;
  diag.bytes_read_ = 0;
  diag.read_operations_counter_ = 0;
  diag.zones_erased_counter_ = 0;
  for (size_t i = 0; i < ZnsConfig::wal_concurrency; i++) {
    diag.append_operations_counter_ +=
        write_channels_[i]->GetAppendOperationsCounter();
    diag.bytes_written_ += write_channels_[i]->GetBytesWritten();
    if (i == 0) {
      diag.append_operations_ = write_channels_[i]->GetAppendOperations();
    } else {
      std::vector<uint64_t> tmp = write_channels_[i]->GetAppendOperations();
      std::vector<uint64_t> tmp2 = std::vector<uint64_t>(tmp.size(), 0);
      std::transform(diag.append_operations_.begin(),
                     diag.append_operations_.end(), tmp.begin(), tmp2.begin(),
                     std::plus<uint64_t>());
      diag.append_operations_ = tmp2;
    }
  }

  for (size_t i = 0; i < N; i++) {
    ZNSDiagnostics waldiag = wals_[i]->GetDiagnostics();
    diag.bytes_read_ += waldiag.bytes_read_;
    diag.read_operations_counter_ += waldiag.read_operations_counter_;
    diag.zones_erased_counter_ += waldiag.zones_erased_counter_;
    if (i == 0) {
      diag.zones_erased_ = waldiag.zones_erased_;
    } else {
      std::vector<uint64_t> tmp(diag.zones_erased_);
      tmp.insert(tmp.end(), waldiag.zones_erased_.begin(),
                 waldiag.zones_erased_.end());
      diag.zones_erased_ = tmp;
    }
  }
  diags.push_back(diag);
#else
  for (size_t i = 0; i < N; i++) {
    ZNSDiagnostics diag = wals_[i]->GetDiagnostics();
    diag.name_ = "WAL" + std::to_string(i);
    diags.push_back(diag);
  }
#endif
  return diags;
}

template <std::size_t N>
std::vector<std::pair<std::string, const TimingCounter>> ZnsWALManager<N>::GetAdditionalWALStatistics() {
  TimingCounter storage_append_perf_counter;
  TimingCounter total_append_perf_counter;
  TimingCounter replay_perf_counter;
  TimingCounter recovery_perf_counter;
  TimingCounter reset_perf_counter;

  // Merge the perf counters (this is safe)
  for (size_t i = 0; i < N; i++) {
    storage_append_perf_counter += wals_[i]->GetStorageAppendPerfCounter();
    total_append_perf_counter += wals_[i]->GetTotalAppendPerfCounter();
    replay_perf_counter += wals_[i]->GetReplayPerfCounter();
    recovery_perf_counter += wals_[i]->GetRecoveryPerfCounter();
    reset_perf_counter += wals_[i]->GetResetPerfCounter();
  }

  return {
    {"WAL Appends", total_append_perf_counter}
    ,{"SZD Appends", storage_append_perf_counter}
    ,{"Replays", replay_perf_counter}
    ,{"Resets", reset_perf_counter}
    ,{"Recovery", recovery_perf_counter}
  };
}

}  // namespace ROCKSDB_NAMESPACE

#endif
#endif
