#include "db/zns_impl/persistence/zns_wal.h"

#include "db/write_batch_internal.h"
#include "db/zns_impl/io/szd_port.h"
#include "db/zns_impl/memtable/zns_memtable.h"
#include "db/zns_impl/persistence/zns_committer.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/write_batch.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {
ZNSWAL::ZNSWAL(SZD::SZDChannelFactory* channel_factory,
               const SZD::DeviceInfo& info, const uint64_t min_zone_head,
               uint64_t max_zone_head)
    : zone_head_(min_zone_head),
      write_head_(min_zone_head),
      min_zone_head_(min_zone_head),
      max_zone_head_(max_zone_head),
      zone_size_(info.zone_size),
      lba_size_(info.lba_size),
      channel_factory_(channel_factory) {
  assert(zone_head_ < info.lba_cap);
  assert(zone_head_ % info.lba_size == 0);
  assert(channel_factory_ != nullptr);
  channel_factory_->Ref();
  channel_factory_->register_channel(&channel_, min_zone_head_, max_zone_head_);
  committer_ = new ZnsCommitter(channel_, info);
}

ZNSWAL::~ZNSWAL() {
  // printf("Deleting WAL.\n");
  delete committer_;
  if (channel_ != nullptr) {
    channel_factory_->unregister_channel(channel_);
  }
  channel_factory_->Unref();
  channel_factory_ = nullptr;
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
       slba += zone_size_) {
    if (!(s = FromStatus(channel_->ResetZone(slba))).ok()) {
      return s;
    }
  }
  s = Status::OK();
  write_head_ = min_zone_head_;
  zone_head_ = min_zone_head_;
  return s;
}

Status ZNSWAL::Recover() {
  Status s;
  uint64_t write_head = min_zone_head_;
  uint64_t zone_head = min_zone_head_, old_zone_head = min_zone_head_;
  for (uint64_t slba = min_zone_head_; slba < max_zone_head_;
       slba += zone_size_) {
    if (!(s = FromStatus(channel_->ZoneHead(slba, &zone_head))).ok()) {
      return s;
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

bool ZNSWAL::SpaceLeft(const Slice& data) {
  (void)data;
  // TODO: this is not safe and kind of strange, it sets max reliable writesize
  // to 26*lbasize (10k on 512, 92k on 4096)...
  bool space_left = write_head_ + 26 < max_zone_head_;
  return space_left;
}

}  // namespace ROCKSDB_NAMESPACE
