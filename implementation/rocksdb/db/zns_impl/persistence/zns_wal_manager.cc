#include "db/zns_impl/persistence/zns_wal_manager.h"

#include "db/write_batch_internal.h"
#include "db/zns_impl/io/device_wrapper.h"
#include "db/zns_impl/io/zns_utils.h"
#include "db/zns_impl/memtable/zns_memtable.h"
#include "db/zns_impl/persistence/zns_committer.h"
#include "db/zns_impl/persistence/zns_wal.h"
#include "port/port.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/write_batch.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {
ZnsWALManager::ZnsWALManager(QPairFactory* qpair_factory,
                             const ZnsDevice::DeviceInfo& info,
                             const uint64_t min_zone_head,
                             uint64_t max_zone_head, size_t wal_count)
    : wal_head_(0), wal_tail_(wal_count - 1), wal_count_(wal_count) {
  assert((max_zone_head - min_zone_head) % wal_count_ == 0);
  uint64_t wal_range = (max_zone_head - min_zone_head) / wal_count_;
  assert(wal_range % info.zone_size == 0);
  uint64_t wal_walker = min_zone_head;

  for (size_t i = 0; i < wal_count_; ++i) {
    ZNSWAL* newwal =
        new ZNSWAL(qpair_factory, info, wal_walker, wal_walker + wal_range);
    newwal->Ref();
    wals.push_back(newwal);
    wal_walker += wal_range;
  }
}

ZnsWALManager::~ZnsWALManager() {
  printf("WAL manager - HEAD: %ld TAIL: %ld\n", wal_head_, wal_tail_);
  for (auto i = wals.begin(); i != wals.end(); ++i) {
    (*i)->Unref();
  }
}

bool ZnsWALManager::WALAvailable() {
  // not allowed to happen
  if (wal_head_ == wal_tail_) {
    assert(false);
    return false;
    // [vvT..Hvvvv]
  } else if (wal_head_ > wal_tail_) {
    return wal_count_ > wal_head_ || wal_tail_ > 0;
  } else {
    return wal_tail_ > wal_head_ + 1;
  }
}

Status ZnsWALManager::NewWAL(port::Mutex* mutex_, ZNSWAL** wal) {
  mutex_->AssertHeld();
  if (!WALAvailable()) {
    return Status::Busy();
  }
  *wal = wals[wal_head_];
  wal_head_++;
  if (wal_head_ == wal_count_) {
    wal_head_ = 0;
  }
  return Status::OK();
}

Status ZnsWALManager::ResetOldWALs(port::Mutex* mutex_) {
  mutex_->AssertHeld();
  if (wal_tail_ > wal_head_) {
    while ((wal_tail_ < wal_count_ && wal_head_ > 0) ||
           (wal_tail_ < wal_count_ - 1)) {
      if (wals[wal_tail_]->Getref() > 1) {
        return Status::OK();
      }
      wals[wal_tail_]->Reset();
      wal_tail_++;
    }
  }
  if (wal_tail_ == wal_count_) {
    wal_tail_ = 0;
  }
  // +2 because wal_head_ -1 can be filled. 
  while (wal_head_ > wal_tail_ + 2) {
    if (wals[wal_tail_]->Getref() > 1) {
      return Status::OK();
    }
    wals[wal_tail_]->Reset();
    wal_tail_++;
  }
  return Status::OK();
}

Status ZnsWALManager::Recover(ZNSMemTable* mem, SequenceNumber* seq) {
  Status s = Status::OK();
  // Recover WAL pointers
  for (auto i = wals.begin(); i != wals.end(); i++) {
    s = (*i)->Recover();
    if (!s.ok()) return s;
  }
  // Find head and tail of manager
  bool first_non_empty = false;
  bool first_empty_after_non_empty = false;
  for (size_t i = 0; i < wals.size(); i++) {
    if (!wals[i]->Empty() && !first_non_empty) {
      printf("A %ld\n", i);
      first_non_empty = true;
      wal_head_ = i + 1;
      if (i > 0) {
        wal_tail_ = i;
      }
      // potential tail or head
    } else if (!wals[i]->Empty() && first_empty_after_non_empty) {
      wal_tail_ = i;
      break;
      // a gap in the middle?
    } else if (!wals[i]->Empty() && first_non_empty) {
      wal_head_ = i + 1;
      // the head is moving one further
    } else if (wals[i]->Empty() && !first_empty_after_non_empty &&
               first_non_empty) {
      first_empty_after_non_empty = true;
      // head can not move further
    }
  }
  if (wal_head_ >= wal_count_) {
    wal_head_ = 0;
  }
  printf("WAL manager - HEAD: %ld TAIL: %ld\n", wal_head_, wal_tail_);

  // Replay from head to tail, to be sure replay all for now...
  size_t i = wal_head_ == 0 ? wal_count_ - 1 : wal_head_ - 1;
  for (; i != wal_head_; i = i == 0 ? wal_count_ - 1 : i - 1) {
    s = wals[i]->Replay(mem, seq);
    if (!s.ok()) return s;
  }
  s = wals[wal_head_]->Replay(mem, seq);
  return s;
}

}  // namespace ROCKSDB_NAMESPACE