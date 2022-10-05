// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/zns_impl/db_impl_zns.h"

namespace ROCKSDB_NAMESPACE {

struct DBImplZNS::Writer {
  explicit Writer(port::Mutex* mu) : batch(nullptr), done(false), cv(mu) {}
  Status status;
  WriteBatch* batch;
  bool done;
  port::CondVar cv;
};

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DBImplZNS::Put(const WriteOptions& options, const Slice& key,
                      const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(options, &batch);
}

Status DBImplZNS::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

Status DBImplZNS::MakeRoomForWrite(size_t size, uint8_t parallel_number) {
  mutex_.AssertHeld();
  Status s;
  bool allow_delay = true;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield error
      s = bg_error_;
      return s;
    }
    if (allow_delay && versions_->NumLevelZones(0) > ZnsConfig::L0_slow_down) {
      // Throttle
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);
      allow_delay = false;
      mutex_.Lock();
    } else if (!mem_[parallel_number]->ShouldScheduleFlush() &&
               wal_[parallel_number]->SpaceLeft(size)) {
      // space left in memory table
      break;
    } else if (imm_[parallel_number] != nullptr) {
      // flush is already scheduled, therefore, wait.
      bg_flush_work_finished_signal_.Wait();
    } else if (versions_->NeedsL0Compaction()) {
      // No more space in L0. Better to wait till compaction is done
      TROPODB_DEBUG("DEBUG: Forcing L0 compaction, no space left\n");
      MaybeScheduleCompactionL0();
      bg_work_l0_finished_signal_.Wait();
    } else if (!wal_man_[parallel_number]->WALAvailable()) {
      // This can not happen in current implementation, but we do treat it.
      // Simply wait for WALs to become available
      TROPODB_DEBUG("DEBUG: out of WALs\n");
      bg_flush_work_finished_signal_.Wait();
    } else {
      // create new WAL
      wal_[parallel_number]->Sync();
      wal_[parallel_number]->Close();
      wal_[parallel_number]->Unref();
      s = wal_man_[parallel_number]->NewWAL(&mutex_, &wal_[parallel_number]);
      if (!s.ok()) {
        TROPODB_ERROR("ERROR: Can not create WAL");
      }
      wal_[parallel_number]->Ref();
      // Hack to prevent needing background operations
#ifdef WALPerfTest
      // Drop all that was in the memtable (NOT PERSISTENT!)
      mem_[parallel_number]->Unref();
      mem_[parallel_number] = new ZNSMemTable(options_, internal_comparator_,
                                              max_write_buffer_size_);
      mem_[parallel_number]->Ref();
      env_->Schedule(&DBImplZNS::BGFlushWork, this, rocksdb::Env::HIGH);
#else
      // Switch to fresh memtable
      imm_[parallel_number] = mem_[parallel_number];
      mem_[parallel_number] = new ZNSMemTable(options_, internal_comparator_,
                                              max_write_buffer_size_);
      mem_[parallel_number]->Ref();
      // Ensure the background knows about these thingss
      MaybeScheduleFlush(parallel_number);
      MaybeScheduleCompactionL0();
#endif
    }
  }
  // We can write
  return Status::OK();
}

WriteBatch* DBImplZNS::BuildBatchGroup(Writer** last_writer,
                                       uint8_t parallel_number) {
  mutex_.AssertHeld();
  assert(!writers_[parallel_number].empty());
  Writer* first = writers_[parallel_number].front();
  WriteBatch* result = first->batch;
  assert(result != nullptr);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size =
      max_write_buffer_size_ - 512;  // TODO: if max_write_buffer_size_ < 512
                                     // then all hell breaks loose!
  // Do not create a wait because of large blobs
  if (max_size > wal_[parallel_number]->SpaceAvailable()) {
    max_size = wal_[parallel_number]->SpaceAvailable();
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_[parallel_number].begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_[parallel_number].end(); ++iter) {
    Writer* w = *iter;
    if (w->batch != nullptr) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_[parallel_number];
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

Status DBImplZNS::Write(const WriteOptions& options, WriteBatch* updates) {
  Status s;

  Writer w(&mutex_);
  w.batch = updates;
  w.done = false;
  MutexLock l(&mutex_);

  // TODO: Striping is NOT the way to go.
  uint8_t striped_index = writer_striper_;
  writer_striper_ = writer_striper_ + 1 == ZnsConfig::lower_concurrency
                        ? 0
                        : writer_striper_ + 1;

  // Add to writer group
  writers_[striped_index].push_back(&w);
  while (!w.done && &w != writers_[striped_index].front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }

  // Wait for space
  s = MakeRoomForWrite(updates == nullptr
                           ? 0
                           : WriteBatchInternal::Contents(updates).size() +
                                 wal_reserved_[striped_index],
                       striped_index);
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  // Write to what is needed
  if (s.ok() && updates != nullptr) {
    // One big batch
    WriteBatch* write_batch = BuildBatchGroup(&last_writer, striped_index);
    WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(write_batch);
    {
      wal_reserved_[striped_index] =
          WriteBatchInternal::Contents(write_batch).size();
      wal_[striped_index]->Ref();
      mutex_.Unlock();
      // WAL
      if (!options.sync) {
        // Async WAL write
        s = wal_[striped_index]->Append(
            WriteBatchInternal::Contents(write_batch), last_sequence + 1);
      } else {
        // Synchronous WAL write
        s = wal_[striped_index]->Append(
            WriteBatchInternal::Contents(write_batch), last_sequence + 1);
        s = s.ok() ? wal_[striped_index]->Sync() : s;
      }
      // write to memtable
      assert(mem_[striped_index] != nullptr);
      if (s.ok()) {
        s = mem_[striped_index]->Write(options, write_batch);
        if (!s.ok()) {
          TROPODB_ERROR("ERROR: memtable error: %s\n", s.getState());
        }
      } else {
        TROPODB_ERROR("ERROR: WAL append error\n");
      }
      mutex_.Lock();
      wal_[striped_index]->Unref();
    }
    if (write_batch == tmp_batch_[striped_index]) {
      tmp_batch_[striped_index]->Clear();
    }
    versions_->SetLastSequence(last_sequence);
  }

  // Writer group coordination
  while (true) {
    Writer* ready = writers_[striped_index].front();
    writers_[striped_index].pop_front();
    if (ready != &w) {
      ready->status = s;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  if (!writers_[striped_index].empty()) {
    writers_[striped_index].front()->cv.Signal();
  }

  return s;
}

Status DBImplZNS::Get(const ReadOptions& options, const Slice& key,
                      std::string* value) {
  MutexLock l(&mutex_);
  Status s;
  value->clear();
  // This is absolutely necessary for locking logic because private pointers
  // can be changed in background work. (snapshotting)
  std::vector<ZNSMemTable*> mem;
  std::vector<ZNSMemTable*> imm;
  for (size_t i = 0; i < ZnsConfig::lower_concurrency; i++) {
    mem.push_back(mem_[i]);
    imm.push_back(imm_[i]);
    mem[i]->Ref();
    if (imm[i] != nullptr) imm[i]->Ref();
  }
  ZnsVersion* current = versions_->current();
  current->Ref();

  // Get on the snapshot
  {
    LookupKey lkey(key, versions_->LastSequence());
    mutex_.Unlock();
    SequenceNumber seq;
    SequenceNumber seq_pot;
    bool found = false;
    std::string tmp;
    // TODO: ALL memtables?
    for (size_t i = 0; i < ZnsConfig::lower_concurrency; i++) {
      if (mem[i]->Get(options, lkey, &tmp, &s, &seq_pot)) {
        if (!found) {
          found = true;
          seq = seq_pot;
          *value = tmp;
        } else if (seq_pot > seq) {
          seq = seq_pot;
          *value = tmp;
        }
      } else if (imm[i] != nullptr &&
                 imm[i]->Get(options, lkey, &tmp, &s, &seq_pot)) {
        if (!found) {
          found = true;
          seq = seq_pot;
          *value = tmp;
        } else if (seq_pot > seq) {
          seq = seq_pot;
          *value = tmp;
        }
      }
    }
    // Look in SSTables
    if (!found) {
      s = current->Get(options, lkey, value);
    }
    mutex_.Lock();
  }

  // Ensures that old data can be removed by dereffing.
  for (size_t i = 0; i < ZnsConfig::lower_concurrency; i++) {
    mem[i]->Unref();
    if (imm[i] != nullptr) imm[i]->Unref();
  }
  current->Unref();

  return s;
}

Status DBImplZNS::Get(const ReadOptions& options,
                      ColumnFamilyHandle* column_family, const Slice& key,
                      PinnableSlice* value, std::string* timestamp) {
  std::string* val = new std::string;
  Status s = Get(options, key, val);
  *value = PinnableSlice(val);
  return s;
}
}  // namespace ROCKSDB_NAMESPACE
