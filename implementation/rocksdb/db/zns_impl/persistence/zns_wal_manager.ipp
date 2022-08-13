#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_WAL_MANAGER_IPP
#define ZNS_WAL_MANAGER_IPP

#include <iomanip>
#include <iostream>

#include "db/write_batch_internal.h"
#include "db/zns_impl/config.h"
#include "db/zns_impl/io/szd_port.h"
#include "db/zns_impl/memtable/zns_memtable.h"
#include "db/zns_impl/persistence/zns_committer.h"
#include "db/zns_impl/persistence/zns_wal.h"
#include "db/zns_impl/persistence/zns_wal_manager.h"
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
#ifndef WAL_MANAGER_MANAGES_CHANNELS
    std::cout << std::left << "WAL" << std::setw(12) << i << std::right
              << std::setw(25) << wal_walker << std::setw(25)
              << wal_walker + wal_range << "\n";
#endif
    newwal->Ref();
    wals_[i] = newwal;
    wal_walker += wal_range;
  }
#ifdef WAL_MANAGER_MANAGES_CHANNELS
  std::cout << std::left << "WALS" << std::setw(11) << "" << std::right
            << std::setw(25) << min_zone_nr << std::setw(25) << max_zone_nr
            << "\n";
#endif
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
    printf("Fatal error, old WAL found\n");
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
    // potential tail or head
    if (!wals_[i]->Empty() && !first_non_empty) {
      first_non_empty = true;
      wal_head_ = i + 1;
      if (i > 0) {
        wal_tail_ = i;
      }

    }  // a gap in the middle?
    else if (!wals_[i]->Empty() && first_empty_after_non_empty) {
      wal_tail_ = i;
      break;

    }  // the head is moving one further
    else if (!wals_[i]->Empty() && first_non_empty) {
      wal_head_ = i + 1;
    }  // head can not move further
    else if (wals_[i]->Empty() && !first_empty_after_non_empty &&
             first_non_empty) {
      first_empty_after_non_empty = true;
    }
  }
  if (wal_head_ >= N) {
    wal_head_ = 0;
  }
  // printf("WAL manager - HEAD: %ld TAIL: %ld\n", wal_head_, wal_tail_);

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
void ZnsWALManager<N>::PrintAdditionalWALStatistics() {
  uint64_t time_waiting_storage = 0;
  uint64_t time_waiting_storage_squared = 0;
  uint64_t time_waiting_storage_total = 0;
  uint64_t time_waiting_storage_squared_total = 0;
  uint64_t time_waiting_storage_number = 0;
  uint64_t time_waiting_storage_number_total = 0;
  uint64_t time_spent_replaying = 0;
  uint64_t time_spend_recovering = 0;
  uint64_t time_waiting_resets = 0;
  uint64_t time_waiting_resets_numbers = 0;
  uint64_t time_waiting_resets_squares = 0;

  for (size_t i = 0; i < N; i++) {
    time_waiting_storage += wals_[i]->TimeSpendWaitingOnStorage();
    time_waiting_storage_squared +=
        wals_[i]->TimeSpendWaitingOnStorageSquared();
    time_waiting_storage_total += wals_[i]->TimeSpendWaitingOnStorageTotal();
    time_waiting_storage_squared_total +=
        wals_[i]->TimeSpendWaitingOnStorageSquaredTotal();
    time_waiting_storage_number += wals_[i]->TimeSpendWaitingOnStorageNumber();
    time_waiting_storage_number_total +=
        wals_[i]->TimeSpendWaitingOnStorageNumberTotal();
    time_spent_replaying += wals_[i]->TimeSpendReplaying();
    time_spend_recovering += wals_[i]->TimeSpendRecovering();
    time_waiting_resets += wals_[i]->TimeSpendWaitingOnResets();
    time_waiting_resets_numbers += wals_[i]->TimeSpendWaitingOnResetsNumber();
    time_waiting_resets_squares += wals_[i]->TimeSpendWaitingOnResetsSquared();
  }
  double avg = static_cast<double>(time_waiting_storage) /
               static_cast<double>(time_waiting_storage_number);
  double variance =
      std::sqrt(static_cast<double>(
                    time_waiting_storage_squared * time_waiting_storage_number -
                    time_waiting_storage * time_waiting_storage) /
                static_cast<double>(time_waiting_storage_number *
                                    time_waiting_storage_number));
  printf("WAL statistics: \n");
  printf(
      "\tWAL operations on storage: %lu, AVG time (μs): %.4f, StdDev (μs): "
      "%.2f \n",
      time_waiting_storage_number, avg, variance);

  avg = static_cast<double>(time_waiting_storage_total) /
        static_cast<double>(time_waiting_storage_number_total);
  variance =
      std::sqrt(static_cast<double>(time_waiting_storage_squared_total *
                                        time_waiting_storage_number_total -
                                    time_waiting_storage_total *
                                        time_waiting_storage_total) /
                static_cast<double>(time_waiting_storage_number_total *
                                    time_waiting_storage_number_total));
  printf(
      "\tWAL operations total: %lu, AVG time (μs) %.4f, StdDev (μs): %.2f \n",
      time_waiting_storage_number_total, avg, variance);

  avg = static_cast<double>(time_waiting_resets) /
        static_cast<double>(time_waiting_resets_numbers);
  variance =
      std::sqrt(static_cast<double>(time_waiting_resets_squares *
                                        time_waiting_resets_numbers -
                                    time_waiting_resets * time_waiting_resets) /
                static_cast<double>(time_waiting_resets_numbers *
                                    time_waiting_resets_numbers));
  printf(
      "\tWAL reset operations total: %lu, AVG time (μs) %.4f, StdDev (μs): "
      "%.2f \n",
      time_waiting_resets_numbers, avg, variance);

  printf(
      "\tWAL time spent on replaying (μs): %lu, recovering zone heads (μs): "
      "%lu \n",
      time_spent_replaying, time_spend_recovering);
}

}  // namespace ROCKSDB_NAMESPACE

#endif
#endif
