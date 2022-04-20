#include "db/zns_impl/persistence/zns_wal.h"

#include "db/write_batch_internal.h"
#include "db/zns_impl/io/device_wrapper.h"
#include "db/zns_impl/io/zns_utils.h"
#include "db/zns_impl/memtable/zns_memtable.h"
#include "db/zns_impl/persistence/zns_committer.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/write_batch.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {
ZNSWAL::ZNSWAL(QPairFactory* qpair_factory, const ZnsDevice::DeviceInfo& info,
               const uint64_t min_zone_head, uint64_t max_zone_head)
    : zone_head_(min_zone_head),
      write_head_(min_zone_head),
      min_zone_head_(min_zone_head),
      max_zone_head_(max_zone_head),
      zone_size_(info.zone_size),
      lba_size_(info.lba_size),
      qpair_factory_(qpair_factory) {
  assert(zone_head_ < info.lba_cap);
  assert(zone_head_ % info.lba_size == 0);
  assert(qpair_factory_ != nullptr);
  qpair_ = new ZnsDevice::QPair*;
  qpair_factory_->Ref();
  qpair_factory_->register_qpair(qpair_);
  committer_ = new ZnsCommitter(*qpair_, info);
}

ZNSWAL::~ZNSWAL() {
  printf("Deleting WAL.\n");
  delete committer_;
  if (qpair_ != nullptr) {
    qpair_factory_->unregister_qpair(*qpair_);
    delete qpair_;
  }
  qpair_factory_->Unref();
  qpair_factory_ = nullptr;
}

void ZNSWAL::Append(Slice data) {
  Status s = committer_->SafeCommit(data, &write_head_, min_zone_head_,
                                    max_zone_head_);
  zone_head_ = (write_head_ / zone_size_) * zone_size_;
  if (!s.ok()) {
    printf("Error in commit on WAL %s\n", s.getState());
  }
}

Status ZNSWAL::Reset() {
  Status s;
  for (uint64_t slba = min_zone_head_; slba < max_zone_head_;
       slba += lba_size_) {
    s = ZnsDevice::z_reset(*qpair_, slba, false) == 0
            ? Status::OK()
            : Status::IOError("Reset WAL zone error");
    if (!s.ok()) {
      return s;
    }
  }
  s = Status::OK();
  write_head_ = min_zone_head_;
  zone_head_ = min_zone_head_;
  return s;
}

Status ZNSWAL::Recover() {
  uint64_t write_head = min_zone_head_;
  uint64_t zone_head = min_zone_head_, old_zone_head = min_zone_head_;
  for (uint64_t slba = min_zone_head_; slba < max_zone_head_;
       slba += zone_size_) {
    if (ZnsDevice::z_get_zone_head(*qpair_, slba, &zone_head) != 0) {
      return Status::IOError("Error getting zonehead for WAL");
    }
    // head is at last zone that is not empty
    if (zone_head > slba) {
      write_head = zone_head;
    }
    // end has been reached.
    if (write_head > min_zone_head_ && zone_head == slba) {
      break;
    }
  }
  write_head_ = write_head;
  zone_head_ = (write_head_ / zone_size_) * zone_size_;
  return Status::OK();
}

Status ZNSWAL::Replay(ZNSMemTable* mem, SequenceNumber* seq) {
  if (write_head_ == min_zone_head_) {
    return Status::OK();
  }
  Status s = Status::OK();
  WriteOptions wo;
  Slice record;

  committer_->GetCommitReader(min_zone_head_, write_head_);
  while (committer_->SeekCommitReader(&record)) {
    WriteBatch batch;
    s = WriteBatchInternal::SetContents(&batch, record);
    if (!s.ok()) {
      break;
    }
    s = mem->Write(wo, &batch);
    if (!s.ok()) {
      break;
    }
    const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
                                    WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *seq) {
      *seq = last_seq;
    }
  }
  committer_->CloseCommit();
  return s;
}

bool ZNSWAL::Empty() { return write_head_ == min_zone_head_; }

}  // namespace ROCKSDB_NAMESPACE
