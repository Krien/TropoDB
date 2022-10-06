// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/zns_impl/persistence/tropodb_wal.h"

#include <vector>

#include "db/write_batch_internal.h"
#include "db/zns_impl/tropodb_config.h"
#include "db/zns_impl/io/szd_port.h"
#include "db/zns_impl/memtable/tropodb_memtable.h"
#include "db/zns_impl/persistence/tropodb_committer.h"
#include "db/zns_impl/utils/tropodb_logger.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/write_batch.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {
ZNSWAL::ZNSWAL(SZD::SZDChannelFactory* channel_factory,
               const SZD::DeviceInfo& info, const uint64_t min_zone_nr,
               const uint64_t max_zone_nr, const uint8_t number_of_writers,
               SZD::SZDChannel** borrowed_write_channel)
    : channel_factory_(channel_factory),
      log_(channel_factory_, info, min_zone_nr, max_zone_nr,
#ifdef WAL_UNORDERED
           number_of_writers,
#else
           1,
#endif
           borrowed_write_channel),
      committer_(&log_, info, false)
#ifdef WAL_BUFFERED
      ,
      buffered_(ZnsConfig::wal_allow_buffering),
      buffsize_(info.zasl),
      buf_(0),
      pos_(0)
#endif
#ifdef WAL_UNORDERED
      ,
      sequence_nr_(0)
#endif
      ,
      clock_(SystemClock::Default().get()) {
  assert(channel_factory_ != nullptr);
  channel_factory_->Ref();
#ifdef WAL_BUFFERED
  if (buffered_) {
    buf_ = new char[buffsize_];
  }
#endif
}

ZNSWAL::~ZNSWAL() {
  Sync();
  channel_factory_->Unref();
#ifdef WAL_BUFFERED
  if (buffered_) {
    delete[] buf_;
  }
#endif
}

#ifdef WAL_BUFFERED
Status ZNSWAL::BufferedAppend(const Slice& data) {
  Status s = Status::OK();
  size_t sizeleft = buffsize_ - pos_;
  size_t sizeneeded = data.size();
  if (sizeneeded < sizeleft) {
    memcpy(buf_ + pos_, data.data(), sizeneeded);
    pos_ += sizeneeded;
  } else {
    if (pos_ != 0) {
      char buf_copy_[pos_];
      memcpy(buf_copy_, buf_, pos_);
      s = DirectAppend(Slice(buf_copy_, pos_));
      pos_ = 0;
    }
    if (!s.ok()) {
      return s;
    }
    sizeleft = buffsize_ - pos_;
    if (sizeneeded < sizeleft) {
      s = DirectAppend(data);
    } else {
      memcpy(buf_ + pos_, data.data(), sizeneeded);
      pos_ += sizeneeded;
    }
  }
  return s;
}

Status ZNSWAL::DataSync() {
  Status s = Status::OK();
  if (pos_ != 0) {
    s = DirectAppend(Slice(buf_, pos_));
    pos_ = 0;
  }
  return s;
}
#endif

Status ZNSWAL::DirectAppend(const Slice& data) {
  uint64_t before = clock_->NowMicros();
  Status s =
      FromStatus(log_.AsyncAppend(data.data(), data.size(), nullptr, true));
  storage_append_perf_counter_.AddTiming(clock_->NowMicros() - before);
  return s;
}

Status ZNSWAL::Append(const Slice& data, uint64_t seq) {
  uint64_t before = clock_->NowMicros();
  Status s = Status::OK();
  size_t space_needed = SpaceNeeded(data);
  char* out;
#ifdef WAL_UNORDERED
  char buf[data.size() + 2 * sizeof(uint64_t)];
  std::memcpy(buf + 2 * sizeof(uint64_t), data.data(), data.size());
  EncodeFixed64(buf, data.size());
  EncodeFixed64(buf + sizeof(uint64_t), sequence_nr_);
  s = committer_.CommitToCharArray(
      Slice(buf, data.size() + 2 * sizeof(uint64_t)), &out);
  sequence_nr_++;
#else
  char buf[data.size() + sizeof(uint64_t)];
  std::memcpy(buf + sizeof(uint64_t), data.data(), data.size());
  EncodeFixed64(buf, data.size());
  s = committer_.CommitToCharArray(Slice(buf, data.size() + sizeof(uint64_t)),
                                   &out);
#endif
  if (!s.ok()) {
    return s;
  }
#ifdef WAL_BUFFERED
  if (buffered_) {
    s = BufferedAppend(Slice(out, space_needed));
  } else {
#endif
    s = DirectAppend(Slice(out, space_needed));
#ifdef WAL_BUFFERED
  }
#endif
  delete[] out;
  total_append_perf_counter_.AddTiming(clock_->NowMicros() - before);
  return s;
}

Status ZNSWAL::Sync() {
#ifdef WAL_BUFFERED
  DataSync();
#endif
  return FromStatus(log_.Sync());
}

#ifdef WAL_UNORDERED
Status ZNSWAL::ReplayUnordered(ZNSMemTable* mem, SequenceNumber* seq) {
  TROPODB_INFO("INFO: WAL: Replaying\n");
  Status s = Status::OK();
  if (log_.Empty()) {
    TROPODB_INFO("INFO: WAL: <EMPTY> Replayed\n");
    return s;
  }
  // Used for each batch
  WriteOptions wo;
  Slice record;

  // Iterate over all written entries
  ZnsCommitReaderString reader;
  std::vector<std::pair<uint64_t, std::string*> > entries;

  std::string commit_string;
  s = FromStatus(log_.ReadAll(commit_string));
  if (!s.ok()) {
    return s;
  }

  committer_.GetCommitReaderString(&commit_string, &reader);
  uint64_t entries_found = 0;
  while (committer_.SeekCommitReaderString(reader, &record)) {
    uint64_t data_size = DecodeFixed64(record.data());
    uint64_t seq_nr = DecodeFixed64(record.data() + sizeof(uint64_t));
    if (data_size > record.size() - 2 * sizeof(uint64_t)) {
      s = Status::Corruption();
      break;
    }
    std::string* dat = new std::string;
    dat->assign(record.data() + 2 * sizeof(uint64_t), data_size);
    entries.push_back(std::make_pair(seq_nr, dat));
    entries_found++;
  }
  committer_.CloseCommitString(reader);

  // Sort on sequence number
  std::sort(
      entries.begin(), entries.end(),
      [](std::pair<uint64_t, std::string*> a,
         std::pair<uint64_t, std::string*> b) { return a.first < b.first; });

  // Apply changes and wipe entries from memory
  for (auto entry : entries) {
    sequence_nr_ = entry.first;
    std::string* dat = entry.second;
    WriteBatch batch;
    s = WriteBatchInternal::SetContents(&batch, Slice(*dat));
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
  TROPODB_INFO("INFO: WAL: <NOT-EMPTY> Replayed WAL\n");
  return s;
}

#else
Status ZNSWAL::ReplayOrdered(ZNSMemTable* mem, SequenceNumber* seq) {
  TROPODB_INFO("INFO: WAL: Replaying WAL\n");
  Status s = Status::OK();
  if (log_.Empty()) {
    TROPODB_INFO("INFO: WAL: <EMPTY> Replayed\n");
    return s;
  }
  // Used for each batch
  WriteOptions wo;
  Slice record;

  // Iterate over all written entries
  ZnsCommitReaderString reader;
  std::string commit_string;
  s = FromStatus(log_.ReadAll(commit_string));
  if (!s.ok()) {
    return s;
  }

  committer_.GetCommitReaderString(&commit_string, &reader);
  while (committer_.SeekCommitReaderString(reader, &record)) {
    uint64_t data_size = DecodeFixed64(record.data());
    if (data_size > record.size() - sizeof(uint64_t)) {
      s = Status::Corruption();
      break;
    }
    char* dat = new char[data_size];
    WriteBatch batch;
    s = WriteBatchInternal::SetContents(
        &batch, Slice(record.data() + sizeof(uint64_t), data_size));
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
  committer_.CloseCommitString(reader);
  TROPODB_INFO("INFO: WAL: <NOT-EMPTY> Replayed WAL\n");
  return s;
}
#endif

Status ZNSWAL::Reset() {
  uint64_t before = clock_->NowMicros();
  Status s = FromStatus(log_.ResetAll());
#ifdef WAL_UNORDERED
  sequence_nr_ = 0;
#endif
  reset_perf_counter_.AddTiming(clock_->NowMicros() - before);
  return s;
}

Status ZNSWAL::Recover() {
  uint64_t before = clock_->NowMicros();
  Status s = FromStatus(log_.RecoverPointers());
  recovery_perf_counter_.AddTiming(clock_->NowMicros() - before);
  return s;
}

Status ZNSWAL::Replay(ZNSMemTable* mem, SequenceNumber* seq) {
  Status s = Status::OK();
  // This check is also done in both replays, but ensures we do NOT measure it
  // for perf!!
  if (log_.Empty()) {
    return s;
  }
  uint64_t before = clock_->NowMicros();
#ifdef WAL_UNORDERED
  s = ReplayUnordered(mem, seq);
#else
  s = ReplayOrdered(mem, seq);
#endif
  replay_perf_counter_.AddTiming(clock_->NowMicros() - before);
  return s;
}

Status ZNSWAL::Close() {
  Status s = Sync();
  s = MarkInactive();
  return s;
}
}  // namespace ROCKSDB_NAMESPACE
