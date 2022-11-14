// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/tropodb/persistence/tropodb_wal.h"

#include <vector>

#include "db/tropodb/io/szd_port.h"
#include "db/tropodb/memtable/tropodb_memtable.h"
#include "db/tropodb/persistence/tropodb_committer.h"
#include "db/tropodb/tropodb_config.h"
#include "db/tropodb/utils/tropodb_logger.h"
#include "db/write_batch_internal.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/write_batch.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {
TropoWAL::TropoWAL(SZD::SZDChannelFactory* channel_factory,
                   const SZD::DeviceInfo& info, const uint64_t min_zone_nr,
                   const uint64_t max_zone_nr, const uint8_t number_of_writers,
                   SZD::SZDChannel** borrowed_write_channel)
    : channel_factory_(channel_factory),
      log_(channel_factory_, info, min_zone_nr, max_zone_nr, number_of_writers,
           borrowed_write_channel),
      committer_(&log_, info, false),
      buffered_(TropoDBConfig::wal_allow_buffering),
      buffsize_(info.zasl),
      buff_(0),
      buff_pos_(0),
      unordered_(TropoDBConfig::wal_unordered),
      sequence_nr_(0),
      clock_(SystemClock::Default().get()) {
  assert(channel_factory_ != nullptr);
  channel_factory_->Ref();
  if (buffered_) {
    buff_ = new char[buffsize_];
  }
}

TropoWAL::~TropoWAL() {
  Sync();
  if (buffered_) {
    delete[] buff_;
  }
  channel_factory_->Unref();
}

Status TropoWAL::BufferedAppend(const Slice& data) {
  Status s = Status::OK();
  size_t sizeleft = buffsize_ - buff_pos_;
  size_t sizeneeded = data.size();
  if (sizeneeded < sizeleft) {
    memcpy(buff_ + buff_pos_, data.data(), sizeneeded);
    buff_pos_ += sizeneeded;
  } else {
    if (buff_pos_ != 0) {
      char buff_copy_[buff_pos_];
      memcpy(buff_copy_, buff_, buff_pos_);
      s = DirectAppend(Slice(buff_copy_, buff_pos_));
      buff_pos_ = 0;
    }
    if (!s.ok()) {
      return s;
    }
    sizeleft = buffsize_ - buff_pos_;
    if (sizeneeded < sizeleft) {
      s = DirectAppend(data);
    } else {
      memcpy(buff_ + buff_pos_, data.data(), sizeneeded);
      buff_pos_ += sizeneeded;
    }
  }
  return s;
}

Status TropoWAL::DataSync() {
  Status s = Status::OK();
  if (buff_pos_ != 0) {
    s = DirectAppend(Slice(buff_, buff_pos_));
    buff_pos_ = 0;
  }
  return s;
}

Status TropoWAL::Sync() {
  DataSync();
  return FromStatus(log_.Sync());
}

Status TropoWAL::DirectAppend(const Slice& data) {
  uint64_t before = clock_->NowMicros();
  Status s =
      FromStatus(log_.AsyncAppend(data.data(), data.size(), nullptr, true));
  storage_append_perf_counter_.AddTiming(clock_->NowMicros() - before);
  return s;
}

Status TropoWAL::Append(const Slice& data, uint64_t seq) {
  uint64_t before = clock_->NowMicros();
  Status s = Status::OK();
  size_t space_needed = SpaceNeeded(data);
  char* out;
  if (unordered_) {
    char buf[data.size() + 2 * sizeof(uint64_t)];
    std::memcpy(buf + 2 * sizeof(uint64_t), data.data(), data.size());
    EncodeFixed64(buf, data.size());
    EncodeFixed64(buf + sizeof(uint64_t), sequence_nr_);
    s = committer_.CommitToCharArray(
        Slice(buf, data.size() + 2 * sizeof(uint64_t)), &out);
    sequence_nr_++;
  } else {
    char buf[data.size() + sizeof(uint64_t)];
    std::memcpy(buf + sizeof(uint64_t), data.data(), data.size());
    EncodeFixed64(buf, data.size());
    s = committer_.CommitToCharArray(Slice(buf, data.size() + sizeof(uint64_t)),
                                     &out);
  }
  if (!s.ok()) {
    return s;
  }
  if (buffered_) {
    s = BufferedAppend(Slice(out, space_needed));
  } else {
    s = DirectAppend(Slice(out, space_needed));
  }
  delete[] out;
  total_append_perf_counter_.AddTiming(clock_->NowMicros() - before);
  return s;
}

Status TropoWAL::ReplayUnordered(TropoMemtable* mem, SequenceNumber* seq) {
  TROPO_LOG_INFO("INFO: WAL: Replaying\n");
  Status s = Status::OK();
  if (log_.Empty()) {
    TROPO_LOG_INFO("INFO: WAL: <EMPTY> Replayed\n");
    return s;
  }
  // Used for each batch
  WriteOptions wo;
  Slice record;

  // Iterate over all written entries
  TropoCommitReaderString reader;
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
  TROPO_LOG_INFO("INFO: WAL: <NOT-EMPTY> Replayed WAL\n");
  return s;
}

Status TropoWAL::ReplayOrdered(TropoMemtable* mem, SequenceNumber* seq) {
  TROPO_LOG_INFO("INFO: WAL: Replaying WAL\n");
  Status s = Status::OK();
  if (log_.Empty()) {
    TROPO_LOG_INFO("INFO: WAL: <EMPTY> Replayed\n");
    return s;
  }
  // Used for each batch
  WriteOptions wo;
  Slice record;

  // Iterate over all written entries
  TropoCommitReaderString reader;
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
  TROPO_LOG_INFO("INFO: WAL: <NOT-EMPTY> Replayed WAL\n");
  return s;
}

Status TropoWAL::Reset() {
  uint64_t before = clock_->NowMicros();
  Status s = FromStatus(log_.ResetAll());
  sequence_nr_ = 0;
  reset_perf_counter_.AddTiming(clock_->NowMicros() - before);
  return s;
}

Status TropoWAL::Recover() {
  uint64_t before = clock_->NowMicros();
  Status s = FromStatus(log_.RecoverPointers());
  recovery_perf_counter_.AddTiming(clock_->NowMicros() - before);
  return s;
}

Status TropoWAL::Replay(TropoMemtable* mem, SequenceNumber* seq) {
  Status s = Status::OK();
  // This check is also done in both replays, but ensures we do NOT measure it
  // for perf!!
  if (log_.Empty()) {
    return s;
  }
  uint64_t before = clock_->NowMicros();
  s = unordered_ ? ReplayUnordered(mem, seq) : ReplayOrdered(mem, seq);
  replay_perf_counter_.AddTiming(clock_->NowMicros() - before);
  return s;
}

Status TropoWAL::Close() {
  Status s = Sync();
  s = MarkInactive();
  return s;
}
}  // namespace ROCKSDB_NAMESPACE
