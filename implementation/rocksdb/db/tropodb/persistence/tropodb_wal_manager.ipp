#pragma once
#ifdef TROPODB_PLUGIN_ENABLED
#ifndef TROPODB_WAL_MANAGER_IPP
#define TROPODB_WAL_MANAGER_IPP

#include <iomanip>
#include <iostream>

#include "db/tropodb/io/szd_port.h"
#include "db/tropodb/memtable/tropodb_memtable.h"
#include "db/tropodb/persistence/tropodb_committer.h"
#include "db/tropodb/persistence/tropodb_wal.h"
#include "db/tropodb/persistence/tropodb_wal_manager.h"
#include "db/tropodb/tropodb_config.h"
#include "db/tropodb/utils/tropodb_logger.h"
#include "db/write_batch_internal.h"
#include "port/port.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/write_batch.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {
template <std::size_t N>
TropoWALManager<N>::TropoWALManager(SZD::SZDChannelFactory* channel_factory,
                                    const SZD::DeviceInfo& info,
                                    const uint64_t min_zone_nr,
                                    const uint64_t max_zone_nr)
    : channel_factory_(channel_factory),
      wal_head_(0),
      wal_tail_(N - 1),
      current_wal_(nullptr) {
  assert((max_zone_nr - min_zone_nr) % N == 0);
  uint64_t wal_range = (max_zone_nr - min_zone_nr) / N;
  assert(wal_range % info.zone_cap_ == 0);
  uint64_t wal_walker = min_zone_nr;

  // WalManager is the boss of the channels. Prevents stale channels.
  write_channels_ = new SZD::SZDChannel*[1];
  channel_factory_->Ref();
  channel_factory_->register_channel(
      &write_channels_[0], min_zone_nr, max_zone_nr,
      TropoDBConfig::wal_preserve_dma, TropoDBConfig::wal_iodepth);
  for (size_t i = 0; i < N; ++i) {
    TropoWAL* newwal = new TropoWAL(channel_factory, info, wal_walker,
                                    wal_walker + wal_range, 1, write_channels_);
    newwal->Ref();
    wals_[i] = newwal;
    wal_walker += wal_range;
  }
}

template <std::size_t N>
TropoWALManager<N>::~TropoWALManager() {
  for (auto i = wals_.begin(); i != wals_.end(); ++i) {
    if ((*i) != nullptr) {
      (*i)->Sync();
      (*i)->Unref();
    }
  }
  for (size_t i = 0; i < 1; ++i) {
    channel_factory_->unregister_channel(write_channels_[0]);
  }
  channel_factory_->Unref();
}

template <std::size_t N>
bool TropoWALManager<N>::WALAvailable() {
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
Status TropoWALManager<N>::NewWAL(port::Mutex* mutex_, TropoWAL** wal) {
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
    TROPO_LOG_ERROR("ERROR: WAL: New WAL is not empty\n");
  }
  wal_head_++;
  if (wal_head_ == N) {
    wal_head_ = 0;
  }
  *wal = current_wal_;
  return Status::OK();
}

template <std::size_t N>
TropoWAL* TropoWALManager<N>::GetCurrentWAL(port::Mutex* mutex_) {
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
Status TropoWALManager<N>::ResetOldWALs(port::Mutex* mutex_) {
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
Status TropoWALManager<N>::Recover(TropoMemtable* mem, SequenceNumber* seq) {
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

    } else if (!wals_[i]->Empty() && first_empty_after_non_empty) {
      // a gap in the middle?
      wal_tail_ = i;
      break;

    } else if (!wals_[i]->Empty() && first_non_empty) {
      // the head is moving one further
      wal_head_ = i + 1;
    } else if (wals_[i]->Empty() && !first_empty_after_non_empty &&
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
std::vector<TropoDiagnostics> TropoWALManager<N>::IODiagnostics() {
  std::vector<TropoDiagnostics> diags;
  TropoDiagnostics diag;
  diag.name_ = "WALS";
  diag.append_operations_counter_ = 0;
  diag.bytes_written_ = 0;
  diag.bytes_read_ = 0;
  diag.read_operations_counter_ = 0;
  diag.zones_erased_counter_ = 0;

  diag.append_operations_counter_ +=
      write_channels_[0]->GetAppendOperationsCounter();
  diag.bytes_written_ += write_channels_[0]->GetBytesWritten();
  diag.append_operations_ = write_channels_[0]->GetAppendOperations();

  for (size_t i = 0; i < N; i++) {
    TropoDiagnostics waldiag = wals_[i]->GetDiagnostics();
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
  return diags;
}

template <std::size_t N>
std::vector<std::pair<std::string, const TimingCounter>>
TropoWALManager<N>::GetAdditionalWALStatistics() {
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

  return {{"WAL Appends", total_append_perf_counter},
          {"SZD Appends", storage_append_perf_counter},
          {"Replays", replay_perf_counter},
          {"Resets", reset_perf_counter},
          {"Recovery", recovery_perf_counter}};
}

}  // namespace ROCKSDB_NAMESPACE

#endif
#endif
