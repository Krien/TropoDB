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
               const SZD::DeviceInfo& info, const uint64_t min_zone_nr,
               const uint64_t max_zone_nr)
    : channel_factory_(channel_factory),
      log_(channel_factory_, info, min_zone_nr, max_zone_nr),
      committer_(&log_, info, true) {
  assert(channel_factory_ != nullptr);
  channel_factory_->Ref();
}

ZNSWAL::~ZNSWAL() { channel_factory_->Unref(); }

Status ZNSWAL::Replay(ZNSMemTable* mem, SequenceNumber* seq) {
  Status s = Status::OK();
  if (log_.Empty()) {
    return s;
  }
  // Used for each batch
  WriteOptions wo;
  Slice record;
  // Iterate over all batches and apply them to the memtable
  committer_.GetCommitReader(log_.GetWriteTail(), log_.GetWriteHead());
  while (committer_.SeekCommitReader(&record)) {
    WriteBatch batch;
    s = WriteBatchInternal::SetContents(&batch, record);
    if (!s.ok()) {
      break;
    }
    s = mem->Write(wo, &batch);
    if (!s.ok()) {
      break;
    }
    // Ensure the sequence number is up to date.
    const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
                                    WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *seq) {
      *seq = last_seq;
    }
  }
  committer_.CloseCommit();
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
