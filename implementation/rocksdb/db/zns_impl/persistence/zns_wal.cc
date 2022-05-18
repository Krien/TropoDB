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
    : buffsize_(512),
      pos_(0),
      channel_factory_(channel_factory),
      log_(channel_factory_, info, min_zone_nr, max_zone_nr),
      committer_(&log_, info, true) {
  assert(channel_factory_ != nullptr);
  channel_factory_->Ref();
  buf_ = new char[buffsize_ + 1];
}

ZNSWAL::~ZNSWAL() {
  Close();
  channel_factory_->Unref();
  delete[] buf_;
}

Status ZNSWAL::DirectAppend(const Slice& data) {
  char buf[data.size() + 2 * sizeof(uint32_t)];
  std::memcpy(buf + sizeof(uint32_t), data.data(), data.size());
  EncodeFixed32(buf, data.size() + sizeof(uint32_t));
  EncodeFixed32(buf + data.size() + sizeof(uint32_t), 0);
  return committer_.SafeCommit(Slice(buf, data.size() + 2 * sizeof(uint32_t)));
}

// See LevelDB env_posix. This is similar, but slightly different as we store
// offsets and do not directly use the committed CRC format.
Status ZNSWAL::Append(const Slice& data) {
  size_t write_size = data.size();
  const char* write_data = data.data();

  // [4 bytes for next| N bytes for entry | sentinel ...]
  size_t required_write_size = write_size + 2 * sizeof(uint32_t);
  if (pos_ + required_write_size < buffsize_) {
    EncodeFixed32(buf_ + pos_, pos_ + write_size + sizeof(uint32_t));
    std::memcpy(buf_ + pos_ + sizeof(uint32_t), write_data, write_size);
    pos_ += write_size + sizeof(uint32_t);
    EncodeFixed32(buf_ + pos_, 0);
    return Status::OK();
  }

  // Can't fit in buffer, so need to do at least one write.
  Status s = Sync();
  if (!s.ok()) {
    return s;
  }

  // Small writes go to buffer, large writes are written directly.
  if (pos_ + required_write_size < buffsize_) {
    EncodeFixed32(buf_ + pos_, pos_ + write_size + sizeof(uint32_t));
    std::memcpy(buf_ + pos_ + sizeof(uint32_t), write_data, write_size);
    pos_ += write_size + sizeof(uint32_t);
    EncodeFixed32(buf_ + pos_, 0);
    return Status::OK();
  }
  return DirectAppend(data);
}

Status ZNSWAL::Close() { return Sync(); }

Status ZNSWAL::Sync() {
  Status s = Status::OK();
  if (pos_ > 0) {
    s = committer_.SafeCommit(Slice(buf_, pos_ + sizeof(uint32_t)));
    pos_ = 0;
    memset(buf_, 0, buffsize_);
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
  ZnsCommitReader reader;
  committer_.GetCommitReader(0, log_.GetWriteTail(), log_.GetWriteHead(),
                             &reader);
  while (committer_.SeekCommitReader(reader, &record)) {
    uint32_t pos = sizeof(uint32_t);
    uint32_t upto = DecodeFixed32(record.data());
    if (upto > record.size()) {
      s = Status::Corruption();
      break;
    }
    uint32_t next = DecodeFixed32(record.data() + upto);
    do {
      WriteBatch batch;
      s = WriteBatchInternal::SetContents(
          &batch, Slice(record.data() + pos, upto - pos));
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
      pos = upto + sizeof(uint32_t);
      upto = next;
      if (upto == record.size()) {
        break;
      }
      if (upto > record.size()) {
        s = Status::Corruption();
        break;
      }
      if (next != 0) {
        next = DecodeFixed32(record.data() + next);
      }
    } while (next > upto || (next == 0 && upto != 0));
  }
  committer_.CloseCommit(reader);
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
