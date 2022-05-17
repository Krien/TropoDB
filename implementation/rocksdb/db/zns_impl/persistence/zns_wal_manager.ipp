#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_WAL_MANAGER_IPP
#define ZNS_WAL_MANAGER_IPP

#include "db/write_batch_internal.h"
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
    : wal_head_(0), wal_tail_(N - 1), current_wal_(nullptr) {
  assert((max_zone_nr - min_zone_nr) % N == 0);
  uint64_t wal_range = (max_zone_nr - min_zone_nr) / N;
  assert(wal_range % info.zone_size == 0);
  uint64_t wal_walker = min_zone_nr;

  for (size_t i = 0; i < N; ++i) {
    ZNSWAL* newwal =
        new ZNSWAL(channel_factory, info, wal_walker, wal_walker + wal_range);
    printf("WAL range %lu %lu\n", wal_walker, wal_walker + wal_range);
    newwal->Ref();
    wals_[i] = newwal;
    wal_walker += wal_range;
  }
}

template <std::size_t N>
ZnsWALManager<N>::~ZnsWALManager() {
  for (auto i = wals_.begin(); i != wals_.end(); ++i) {
    if ((*i) != nullptr) {
      (*i)->Close();
      (*i)->Unref();
    }
  }
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

}  // namespace ROCKSDB_NAMESPACE

#endif
#endif
