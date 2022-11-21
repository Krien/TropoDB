// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/tropodb/persistence/tropodb_wal.h"

#include <vector>
#include <iostream>

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
                   const uint64_t max_zone_nr, const bool use_buffer,
                   const bool group_commits, const bool allow_unordered,
                   SZD::SZDChannel* borrowed_write_channel)
    : channel_factory_(channel_factory),
      log_(channel_factory_, info, min_zone_nr, max_zone_nr,
           borrowed_write_channel),
      committer_(&log_, info, false),
      buffered_(use_buffer),
      buffsize_(info.lba_size * TropoDBConfig::wal_buffered_pages),
      buff_(0),
      buff_pos_(0),
      group_commits_(group_commits),
      unordered_(allow_unordered),
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

Status TropoWAL::LightEncodeAppend(const Slice& data, char** out, size_t* size) {
  Status s = Status::OK();
  if (out == nullptr) {
    return Status::Corruption("Nullptr passed to EncodeAppend");
  }
  if (unordered_) {
    *out = (char*)std::malloc(data.size() + 2 * sizeof(uint64_t) + sizeof("group"));
    std::memcpy(*out, "group",sizeof("group"));
    std::memcpy(*out + 2 * sizeof(uint64_t) + sizeof("group"), data.data(), data.size());
    EncodeFixed64(*out + sizeof("group"), data.size());
    EncodeFixed64(*out + sizeof("group")+ sizeof(uint64_t), sequence_nr_);
    sequence_nr_++;
    *size = data.size() + sizeof("group") + 2 * sizeof(uint64_t);
  } else {
    *out = (char*)std::malloc(data.size() + sizeof(uint64_t) + sizeof("group"));
    std::memcpy(*out, "group",sizeof("group"));
    std::memcpy(*out  + sizeof(uint64_t) + sizeof("group"), data.data(), data.size());
    EncodeFixed64(*out+ sizeof("group"), data.size());
    *size = data.size() + sizeof("group") + sizeof(uint64_t);
  }
  return s;
}

Status TropoWAL::EncodeAppend(const Slice& data, char** out, size_t* size) {
  Status s;
  if (out == nullptr) {
    return Status::Corruption("Nullptr passed to EncodeAppend");
  }
  if (unordered_) {
    char buf[data.size() + 2 * sizeof(uint64_t)];
    std::memcpy(buf + 2 * sizeof(uint64_t), data.data(), data.size());
    EncodeFixed64(buf, data.size());
    EncodeFixed64(buf + sizeof(uint64_t), sequence_nr_);
    s = committer_.CommitToCharArray(
        Slice(buf, data.size() + 2 * sizeof(uint64_t)), out);
    sequence_nr_++;
  } else {
    char buf[data.size() + sizeof(uint64_t)];
    std::memcpy(buf + sizeof(uint64_t), data.data(), data.size());
    EncodeFixed64(buf, data.size());
    s = committer_.CommitToCharArray(Slice(buf, data.size() + sizeof(uint64_t)),
                                     out);
  }
  *size = SpaceNeeded(data);
  return s;
}

Status TropoWAL::DirectAppend(const Slice& data) {
  uint64_t before = clock_->NowMicros();
  Status s =
      FromStatus(log_.AsyncAppend(data.data(), data.size(), nullptr, true));
  submit_sync_append_perf_counter_.AddTiming(clock_->NowMicros() - before);
  return s;
}

Status TropoWAL::SubmitAppend(const Slice& data) {
  uint64_t before = clock_->NowMicros();
  Status s = FromStatus(log_.AsyncAppend(data.data(), data.size(), nullptr, true));
  submit_async_append_perf_counter_.AddTiming(clock_->NowMicros() - before);
  return s;
}

Status TropoWAL::BufferedFlush(const Slice& data) {
  Status s;
  if (group_commits_) {
   char* out;
   size_t space_needed = SpaceNeeded(data);
   s = committer_.CommitToCharArray(data, &out);
   if (!s.ok()) {
     return s;
   }
   s = SubmitAppend(Slice(out, space_needed));
   delete[] out;
  } else {
   s = SubmitAppend(data);
  }
  return s;
}

Status TropoWAL::DataSync() {
  Status s = Status::OK();
  if (buffered_ && buff_pos_ != 0) {
    s = BufferedFlush(Slice(buff_, buff_pos_));
    buff_pos_ = 0;
  }
  return s;
}

Status TropoWAL::Sync() {
  uint64_t before = clock_->NowMicros();
  Status s = DataSync();
  if (!s.ok()) {
    return s;
  }
  s = FromStatus(log_.Sync());
  sync_perf_counter_.AddTiming(clock_->NowMicros() - before);
  return s;
}

Status TropoWAL::BufferedAppend(const Slice& data) {
  uint64_t before = clock_->NowMicros();
  Status s = Status::OK();
  size_t sizeleft = buffsize_ - buff_pos_;
  if (group_commits_) {
    sizeleft--;
  }
  size_t sizeneeded = data.size();
  size_t sizeimpact = sizeneeded;
  if (group_commits_) {
    sizeimpact = committer_.SpaceNeeded(buff_pos_ + sizeneeded);
  }
  // Does it fit in the buffer, the copy to buffer
  if (sizeimpact < sizeleft) {
    memcpy(buff_ + buff_pos_, data.data(), sizeneeded);
    buff_pos_ += sizeneeded;
  } else {
    // If it does not fit, we first flush the buffer
    if (buff_pos_ != 0) {
      s = DataSync();
      if (!s.ok()) {
        return s;
      }
      sizeleft = buffsize_ - buff_pos_;
    }
    // Attempt 2 - If it still does not fit, we simply append directly
    if (sizeimpact < sizeleft) {
      memcpy(buff_ + buff_pos_, data.data(), sizeneeded);
      buff_pos_ += sizeneeded;
    } else {
      s = BufferedFlush(data);
    }
  }
  submit_buffered_append_perf_counter_.AddTiming(clock_->NowMicros() - before);
  return s;
}

Status TropoWAL::Append(const Slice& data, uint64_t seq, bool sync) {
  uint64_t before = clock_->NowMicros();
  Status s = Status::OK();
  size_t space_needed;
  char* out;
  if (sync || !group_commits_) {
    s = EncodeAppend(data, &out, &space_needed);
  } else {
    s = LightEncodeAppend(data, &out, &space_needed);
  }
  if (!s.ok()) {
    return s;
  }
  prepare_append_perf_counter_.AddTiming(clock_->NowMicros() - before);

  if (sync) {
    s = DirectAppend(Slice(out, space_needed));
  } else if (buffered_) {
    s = BufferedAppend(Slice(out, space_needed));
  } else {
    s = SubmitAppend(Slice(out, space_needed));
  }
  delete[] out;
  total_append_perf_counter_.AddTiming(clock_->NowMicros() - before);
  return s;
}

Status TropoWAL::ReplayUnordered(TropoMemtable* mem, SequenceNumber* seq) {
  TROPO_LOG_INFO("INFO: WAL: Replaying unordered WAL\n");
  Status s = Status::OK();
  if (log_.Empty()) {
    TROPO_LOG_INFO("INFO: WAL: <EMPTY> unoredered replayed\n");
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
    size_t data_point = 0;
    do {
      // found end
      if (group_commits_) {
        if (record.size() < data_point + sizeof("group")) {
          break;
        }
        if (memcmp(record.data() + data_point, "group",sizeof("group")) != 0) {
          break;
        }
      }
      data_point += sizeof("group");
      uint64_t data_size = DecodeFixed64(record.data() + data_point);
      uint64_t seq_nr = DecodeFixed64(record.data() + data_point + sizeof(uint64_t));
      if (data_size > record.size() - 2 * sizeof(uint64_t) - data_point) {
        s = Status::Corruption();
        break;
      }
      std::string* dat = new std::string;
      dat->assign(record.data() + data_point + 2 * sizeof(uint64_t), data_size);
      entries.push_back(std::make_pair(seq_nr, dat));
      entries_found++;
      data_point += data_size + 2 * sizeof(uint64_t);
    } while (group_commits_);
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
  TROPO_LOG_INFO("INFO: WAL: <NOT-EMPTY> Replayed unordered WAL\n");
  return s;
}

Status TropoWAL::ReplayOrdered(TropoMemtable* mem, SequenceNumber* seq) {
  TROPO_LOG_INFO("INFO: WAL: Replaying ordered WAL\n");
  Status s = Status::OK();
  if (log_.Empty()) {
    TROPO_LOG_INFO("INFO: WAL: <EMPTY> ordered replayed\n");
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
    size_t data_point = 0;
    do {
      // found end
      if (group_commits_) {
        if (record.size() < data_point + sizeof("group")) {
          break;
        }
        if (memcmp(record.data() + data_point, "group",sizeof("group")) != 0) {
          break;
        }
      }
      data_point += sizeof("group");
      uint64_t data_size = DecodeFixed64(record.data() + data_point);
      if (data_size > record.size() - sizeof(uint64_t) - data_point) {
        s = Status::Corruption();
        break;
      }
      char* dat = new char[data_size];
      WriteBatch batch;
      s = WriteBatchInternal::SetContents(
          &batch, Slice(record.data() + sizeof(uint64_t) + data_point, data_size));
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
      data_point += data_size + sizeof(uint64_t);
    } while(group_commits_);
  }
  committer_.CloseCommitString(reader);
  TROPO_LOG_INFO("INFO: WAL: <NOT-EMPTY> Replayed WAL\n");
  return s;
}

Status TropoWAL::Reset() {
  TROPO_LOG_DEBUG("DEBUG: WAL: Resetting WAL\n");
  uint64_t before = clock_->NowMicros();
  Status s = FromStatus(log_.ResetAll());
  sequence_nr_ = 0;
  reset_perf_counter_.AddTiming(clock_->NowMicros() - before);
  return s;
}

Status TropoWAL::Recover() {
  TROPO_LOG_DEBUG("DEBUG: WAL: Recovering WAL log pointers\n");
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
