// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

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
    : buffsize_(info.zasl - info.lba_size),
      pos_(0),
      channel_factory_(channel_factory),
      log_(channel_factory_, info, min_zone_nr, max_zone_nr),
      committer_(&log_, info, true) {
  assert(channel_factory_ != nullptr);
  channel_factory_->Ref();
  buf_ = new char[buffsize_];
}

ZNSWAL::~ZNSWAL() {
  Close();
  channel_factory_->Unref();
  delete[] buf_;
}

// See LevelDB env_posix. This is the similar, if not the same.
Status ZNSWAL::Append(const Slice& data) {
  std::string new_data;
  committer_.CommitToString(data, &new_data);

  size_t write_size = new_data.size();
  const char* write_data = new_data.data();

  size_t copy_size = std::min(write_size, buffsize_ - pos_);
  std::memcpy(buf_ + pos_, write_data, copy_size);
  write_data += copy_size;
  write_size -= copy_size;
  pos_ += copy_size;
  if (write_size == 0) {
    return Status::OK();
  }

  // Can't fit in buffer, so need to do at least one write.
  Status s = Sync();
  if (!s.ok()) {
    return s;
  }

  // Small writes go to buffer, large writes are written directly.
  if (write_size < buffsize_) {
    std::memcpy(buf_, write_data, write_size);
    pos_ = write_size;
    return Status::OK();
  }
  return DirectAppend(data);
}

Status ZNSWAL::Close() { return Sync(); }

Status ZNSWAL::Sync() {
  Status s = Status::OK();
  if (pos_ > 0) {
    s = FromStatus(log_.Append(buf_, pos_));
    pos_ = 0;
  }
  return s;
}

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
