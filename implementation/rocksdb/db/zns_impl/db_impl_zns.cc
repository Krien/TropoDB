// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/zns_impl/db_impl_zns.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <set>
#include <string>
#include <vector>

#include "db/column_family.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "db/zns_impl/config.h"
#include "db/zns_impl/index/zns_compaction.h"
#include "db/zns_impl/index/zns_version.h"
#include "db/zns_impl/index/zns_version_set.h"
#include "db/zns_impl/io/szd_port.h"
#include "db/zns_impl/persistence/zns_manifest.h"
#include "db/zns_impl/persistence/zns_wal.h"
#include "db/zns_impl/persistence/zns_wal_manager.h"
#include "db/zns_impl/table/zns_sstable_manager.h"
#include "db/zns_impl/table/zns_table_cache.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/listener.h"
#include "rocksdb/metadata.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/transaction_log.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/write_buffer_manager.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

const int kNumNonTableCacheFiles = 10;

struct DBImplZNS::Writer {
  explicit Writer(port::Mutex* mu) : batch(nullptr), done(false), cv(mu) {}
  Status status;
  WriteBatch* batch;
  bool done;
  port::CondVar cv;
};

DBImplZNS::DBImplZNS(const DBOptions& options, const std::string& dbname,
                     const bool seq_per_batch, const bool batch_per_txn,
                     bool read_only)
    : options_(options),
      name_(dbname),
      internal_comparator_(BytewiseComparator()),
      env_(options.env),
      // Will be initialised after SPDK
      zns_device_(nullptr),
      channel_factory_(nullptr),
      ss_manager_(nullptr),
      manifest_(nullptr),
      table_cache_(nullptr),
      versions_(nullptr),
      // State
      bg_work_l0_finished_signal_(&mutex_),
      bg_work_finished_signal_(&mutex_),
      bg_flush_work_finished_signal_(&mutex_),
      bg_compaction_l0_scheduled_(false),
      bg_compaction_scheduled_(false),
      shutdown_(false),
      bg_error_(Status::OK()),
      forced_schedule_(false),
      // diag
      clock_(SystemClock::Default().get())
      {
  for (size_t i = 0; i < ZnsConfig::lower_concurrency; i++) {
    wal_man_[i] = nullptr;
    wal_[i] = nullptr;
    mem_[i] = nullptr;
    imm_[i] = nullptr;
    bg_flush_scheduled_[i] = false;
    tmp_batch_[i] = new WriteBatch;
    wal_reserved_[i] = 0;
  }
  for (uint8_t i = 0; i < ZnsConfig::level_count - 1; i++) {
    compactions_[i] = 0;
  }
  env_->SetBackgroundThreads(2, ROCKSDB_NAMESPACE::Env::Priority::HIGH);
  env_->SetBackgroundThreads(5, ROCKSDB_NAMESPACE::Env::Priority::LOW);
}

DBImplZNS::~DBImplZNS() {
  printf("Shutdown \n");
  mutex_.Lock();
  while (bg_compaction_l0_scheduled_ || bg_compaction_scheduled_ ||
         AnyFlushScheduled()) {
    shutdown_ = true;
    printf("busy, wait before closing\n");
    if (bg_compaction_l0_scheduled_) {
      bg_work_l0_finished_signal_.Wait();
    }
    if (bg_compaction_scheduled_) {
      bg_work_finished_signal_.Wait();
    }
    if (AnyFlushScheduled()) {
      bg_flush_work_finished_signal_.Wait();
    }
  }
  for (size_t i = 0; i < ZnsConfig::lower_concurrency; i++) {
    if (wal_[i] != nullptr) {
      wal_[i]->Sync();
    }
  }
  mutex_.Unlock();

  PrintStats();

  if (versions_ != nullptr) delete versions_;
  for (size_t i = 0; i < ZnsConfig::lower_concurrency; i++) {
    if (mem_[i] != nullptr) mem_[i]->Unref();
    if (imm_[i] != nullptr) imm_[i]->Unref();
    if (wal_[i] != nullptr) wal_[i]->Unref();
    if (wal_man_[i] != nullptr) wal_man_[i]->Unref();
    if (tmp_batch_[i] != nullptr) delete tmp_batch_[i];
  }
  if (ss_manager_ != nullptr) ss_manager_->Unref();
  if (manifest_ != nullptr) manifest_->Unref();
  if (table_cache_ != nullptr) delete table_cache_;
  if (channel_factory_ != nullptr) channel_factory_->Unref();
  if (zns_device_ != nullptr) delete zns_device_;
  // printf("exiting \n");
}  // namespace ROCKSDB_NAMESPACE

Status DBImplZNS::ValidateOptions(const DBOptions& db_options) {
  if (db_options.db_paths.size() > 1) {
    return Status::NotSupported("We do not support multiple db paths.");
  }
  // We do not support most other options, but rather we ignore them for now.
  if (!db_options.use_zns_impl) {
    return Status::NotSupported("ZNS must be enabled to use ZNS.");
  }
  return Status::OK();
}

Status DBImplZNS::OpenZNSDevice(const std::string dbname) {
  zns_device_ = new SZD::SZDDevice(dbname);
  Status s;
  s = FromStatus(zns_device_->Init());
  if (!s.ok()) {
    return Status::IOError("Error opening SPDK");
  }
  s = FromStatus(
      zns_device_->Open(this->name_, ZnsConfig::min_zone, ZnsConfig::max_zone));
  if (!s.ok()) {
    return Status::IOError("Error opening ZNS device");
  }
  channel_factory_ = new SZD::SZDChannelFactory(zns_device_->GetDeviceManager(),
                                                ZnsConfig::max_channels);
  channel_factory_->Ref();
  return Status::OK();
}

Status DBImplZNS::ResetZNSDevice() {
  Status s;
  channel_factory_->Ref();
  SZD::SZDChannel* channel;
  s = FromStatus(channel_factory_->register_channel(&channel));
  if (s.ok()) {
    s = FromStatus(channel->ResetAllZones());
  }
  if (s.ok()) {
    s = FromStatus(channel_factory_->unregister_channel(channel));
  }
  channel_factory_->Unref();
  return s.ok() ? Status::OK() : Status::IOError("Error resetting device");
}

Status DBImplZNS::InitDB(const DBOptions& options,
                         const size_t max_write_buffer_size) {
  max_write_buffer_size_ = max_write_buffer_size;
  assert(zns_device_ != nullptr);
  SZD::DeviceInfo device_info;
  zns_device_->GetInfo(&device_info);
  uint64_t zone_head = device_info.min_lba / device_info.zone_size;
  uint64_t zone_step = 0;

  std::cout << "==== Zone division ====\n";
  std::cout << std::left << std::setw(15) << "Structure" << std::right
            << std::setw(25) << "Begin (zone nr)" << std::setw(25)
            << "End (zone nr)"
            << "\n";
  std::cout << std::setfill('_') << std::setw(76) << "\n" << std::setfill(' ');
  zone_step = ZnsConfig::manifest_zones;
  manifest_ = new ZnsManifest(channel_factory_, device_info, zone_head,
                              zone_head + zone_step);
  manifest_->Ref();
  std::cout << std::left << std::setw(15) << "Manifest" << std::right
            << std::setw(25) << zone_head << std::setw(25)
            << zone_head + zone_step << "\n";
  zone_head += zone_step;

  zone_step = ZnsConfig::wal_count * ZnsConfig::zones_foreach_wal;
  zone_step /= ZnsConfig::lower_concurrency;
  for (size_t i = 0; i < ZnsConfig::lower_concurrency; i++) {
    wal_man_[i] = new ZnsWALManager<ZnsConfig::wal_manager_zone_count>(
        channel_factory_, device_info, zone_head, zone_step + zone_head);
    wal_man_[i]->Ref();
    zone_head += zone_step;
  }
  zone_step = device_info.max_lba / device_info.zone_size - zone_head - 1;
  // If only we had access to C++23.
  ss_manager_ =
      ZNSSSTableManager::NewZNSSTableManager(channel_factory_, device_info,
                                             zone_head, zone_head + zone_step)
          .value_or(nullptr);
  if (ss_manager_ == nullptr) {
    return Status::Corruption();
  }
  ss_manager_->Ref();
  std::cout << std::setfill('_') << std::setw(76) << "\n" << std::setfill(' ');
  zone_head = device_info.max_lba / device_info.zone_size;

  for (size_t i = 0; i < ZnsConfig::lower_concurrency; i++) {
    mem_[i] =
        new ZNSMemTable(options, internal_comparator_, max_write_buffer_size_);
    mem_[i]->Ref();
  }

  Options opts(options, ColumnFamilyOptions());
  table_cache_ = new ZnsTableCache(opts, internal_comparator_, 1024 * 1024 * 4,
                                   ss_manager_);

  versions_ = new ZnsVersionSet(internal_comparator_, ss_manager_, manifest_,
                                device_info.lba_size, device_info.zone_cap,
                                table_cache_, this->env_);

  return Status::OK();
}

Status DBImplZNS::InitWAL() {
  mutex_.AssertHeld();
  Status s;
  for (size_t i = 0; i < ZnsConfig::lower_concurrency; i++) {
    // We must force flush all WALs if there is not enough space.
    if (!wal_man_[i]->WALAvailable()) {
      if (mem_[i]->GetInternalSize() > 0) {
        imm_[i] = mem_[i];
        mem_[i] = new ZNSMemTable(options_, internal_comparator_,
                                  max_write_buffer_size_);
        mem_[i]->Ref();
      }
      MaybeScheduleFlush(i);
      while (!wal_man_[i]->WALAvailable()) {
        bg_flush_work_finished_signal_.Wait();
      }
    }
    wal_[i] = wal_man_[i]->GetCurrentWAL(&mutex_);
    wal_[i]->Ref();
  }
  MaybeScheduleCompactionL0();
  MaybeScheduleCompaction(true);
  return s;
}

Status DBImplZNS::Open(
    const DBOptions& db_options, const std::string& name,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, DB** dbptr,
    const bool seq_per_batch, const bool batch_per_txn) {
  Status s;
  s = ValidateOptions(db_options);
  if (!s.ok()) {
    return s;
  }
  // We do not support column families, so we just clear them
  handles->clear();

  size_t max_write_buffer_size = 0;
  for (auto cf : column_families) {
    max_write_buffer_size =
        std::max(max_write_buffer_size, cf.options.write_buffer_size);
  }

  DBImplZNS* impl = new DBImplZNS(db_options, name);
  s = impl->OpenZNSDevice("ZNSLSM");
  if (!s.ok()) return s;
  s = impl->InitDB(db_options, max_write_buffer_size);
  if (!s.ok()) return s;
  // setup WAL (WAL DIR)
  impl->mutex_.Lock();
  s = impl->Recover();
  if (s.ok()) {
    s = impl->InitWAL();
  }
  if (s.ok()) {
    // impl->RemoveObsoleteZones();
    for (size_t i = 0; i < ZnsConfig::lower_concurrency; i++) {
      impl->MaybeScheduleFlush(i);
    }
    impl->MaybeScheduleCompactionL0();
    impl->MaybeScheduleCompaction(false);
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    *dbptr = reinterpret_cast<DB*>(impl);
  } else {
    delete impl;
  }
  return s;
}

bool DBImplZNS::AnyFlushScheduled() {
  for (size_t i = 0; i < ZnsConfig::lower_concurrency; i++) {
    if (bg_flush_scheduled_[i]) {
      return true;
    }
  }
  return false;
}

Status DBImplZNS::Close() {
  mutex_.Lock();
  while (bg_compaction_l0_scheduled_ || bg_compaction_scheduled_ ||
         AnyFlushScheduled()) {
    shutdown_ = true;
    printf("busy, wait before closing\n");
    if (bg_compaction_l0_scheduled_) {
      bg_work_l0_finished_signal_.Wait();
    }
    if (bg_compaction_scheduled_) {
      bg_work_finished_signal_.Wait();
    }
    if (AnyFlushScheduled()) {
      bg_flush_work_finished_signal_.Wait();
    }
  }
  mutex_.Unlock();
  // TODO: close device.
  return Status::OK();
}

Status DBImplZNS::Recover() {
  printf("recovering\n");
  Status s;
  // Recover index structure
  s = versions_->Recover();
  // If there is no version to be recovered, we assume there is no valid DB.
  if (!s.ok()) {
    return options_.create_if_missing ? ResetZNSDevice() : s;
    // TODO: this is not enough when version is corrupt, then device WILL be
    // reset, but metadata still points to corrupt.
  }
  // TODO: currently this still writes a new version... if not an identical
  // one.
  if (options_.error_if_exists) {
    return Status::InvalidArgument("DB already exists");
  }

  // Recover WAL and head
  SequenceNumber old_seq;
  for (size_t i = 0; i < ZnsConfig::lower_concurrency; i++) {
    s = wal_man_[i]->Recover(mem_[i], &old_seq);
  }
  if (!s.ok()) return s;
  versions_->SetLastSequence(old_seq);
  return s;
}

Status DBImplZNS::DestroyDB(const std::string& dbname, const Options& options) {
  // Destroy "all" files from the DB. Since we do not use multitenancy, we
  // might as well reset the device.
  Status s;
  DBImplZNS* impl = new DBImplZNS(options, dbname);
  s = impl->OpenZNSDevice("ZNSLSM");
  if (!s.ok()) return s;
  s = impl->InitDB(options, options.write_buffer_size);
  if (!s.ok()) return s;
  s = impl->ResetZNSDevice();
  if (!s.ok()) return s;
  printf("Reset device\n");
  delete impl;
  return s;
}

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
      mutex_.Unlock();
      // printf("SlowDown reached...\n");
      env_->SleepForMicroseconds(1000);
      allow_delay = false;
      mutex_.Lock();
    } else if (!mem_[parallel_number]->ShouldScheduleFlush() &&
               wal_[parallel_number]->SpaceLeft(size)) {
      // space left in memory table
      break;
    } else if (imm_[parallel_number] != nullptr) {
      // flush is scheduled, wait...
      // printf("Imm already scheduled\n");
      bg_flush_work_finished_signal_.Wait();
    } else if (versions_->NeedsL0Compaction()) {
      // No more space in L0... Better to wait till compaction is done
      printf("Force L0\n");
      MaybeScheduleCompactionL0();
      bg_work_l0_finished_signal_.Wait();
    } else if (!wal_man_[parallel_number]->WALAvailable()) {
      printf("Out of WALs\n");
      bg_flush_work_finished_signal_.Wait();
    } else {
      // create new WAL
      wal_[parallel_number]->Sync();
      wal_[parallel_number]->Close();
      wal_[parallel_number]->Unref();
      s = wal_man_[parallel_number]->NewWAL(&mutex_, &wal_[parallel_number]);
      wal_[parallel_number]->Ref();
#ifdef WALPerfTest
      // Drop all that was in the memtable (NOT PERSISTENT!)
      mem_[parallel_number]->Unref();
      mem_[parallel_number] = new ZNSMemTable(options_, internal_comparator_,
                                              max_write_buffer_size_);
      mem_[parallel_number]->Ref();
      env_->Schedule(&DBImplZNS::BGFlushWork, this, rocksdb::Env::HIGH);
#else
      // printf("Reset WAL\n");
      // Switch to fresh memtable
      imm_[parallel_number] = mem_[parallel_number];
      mem_[parallel_number] = new ZNSMemTable(options_, internal_comparator_,
                                              max_write_buffer_size_);
      mem_[parallel_number]->Ref();
      MaybeScheduleFlush(parallel_number);
      MaybeScheduleCompactionL0();
#endif
    }
  }
  return Status::OK();
}  // namespace ROCKSDB_NAMESPACE

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
  // if (size <= (128 << 10)) {
  //   max_size = size + (128 << 10);
  // }
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

  uint8_t striped_index = writer_striper_;
  writer_striper_ = writer_striper_ + 1 == ZnsConfig::lower_concurrency
                        ? 0
                        : writer_striper_ + 1;
  writers_[striped_index].push_back(&w);
  while (!w.done && &w != writers_[striped_index].front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }

  s = MakeRoomForWrite(updates == nullptr
                           ? 0
                           : WriteBatchInternal::Contents(updates).size() +
                                 wal_reserved_[striped_index],
                       striped_index);
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  // Write to what is needed
  if (s.ok() && updates != nullptr) {
    WriteBatch* write_batch = BuildBatchGroup(&last_writer, striped_index);
    WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(write_batch);
    {
      wal_reserved_[striped_index] =
          WriteBatchInternal::Contents(write_batch).size();
      wal_[striped_index]->Ref();
      mutex_.Unlock();
      // buffering does not really make sense at the moment.
      // later we might decide to implement "FSync" or "ZoneSync", then it
      // should be reinstagated.
      // printf("writing %lu/%lu  %lu\n", mem_->GetInternalSize(),
      //        max_write_buffer_size_,
      //        WriteBatchInternal::Contents(write_batch).size());
      if (!options.sync) {
        s = wal_[striped_index]->Append(
            WriteBatchInternal::Contents(write_batch), last_sequence + 1);
      } else {
        s = wal_[striped_index]->Append(
            WriteBatchInternal::Contents(write_batch), last_sequence + 1);
        s = wal_[striped_index]->Sync();
      }
      // write to memtable
      assert(mem_[striped_index] != nullptr);
      if (s.ok()) {
        s = mem_[striped_index]->Write(options, write_batch);
        if (!s.ok()) {
          printf("Error writing to memtable %s\n", s.getState());
        }
      }
      mutex_.Lock();
      wal_[striped_index]->Unref();
    }
    if (write_batch == tmp_batch_[striped_index]) {
      tmp_batch_[striped_index]->Clear();
    }
    versions_->SetLastSequence(last_sequence);
  }

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
  // can be changed in background work.
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
  LookupKey lkey(key, versions_->LastSequence());
  {
    mutex_.Unlock();
    SequenceNumber seq;
    SequenceNumber seq_pot;
    bool found = false;
    std::string tmp;
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
    if (!found) {
      s = current->Get(options, lkey, value);
    }
    mutex_.Lock();
  }

  // Ensures that old data can be removed.
  for (size_t i = 0; i < ZnsConfig::lower_concurrency; i++) {
    mem[i]->Unref();
    if (imm[i] != nullptr) imm[i]->Unref();
  }

  current->Unref();
  // printf("Gotten \n");

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

Iterator* DBImplZNS::NewIterator(const ReadOptions& options,
                                 ColumnFamilyHandle* column_family) {
  return NULL;
  // mutex_.Lock();
  // SequenceNumber latest_snapshot = versions_->LastSequence();
  // std::vector<Iterator*> list;
  // // Memtables
  // list.push_back(mem_->NewIterator());
  // mem_->Ref();
  // if (imm_ != nullptr) {
  //   list.push_back(imm_->NewIterator());
  //   imm_->Ref();
  // }
  // // Version
  // versions_->current()->AddIterators(options, &list);
  // versions_->current()->Ref();
  // // Join iter
  // Iterator* internal_iter =
  //     NewMergingIterator(&internal_comparator_, &list[0], list.size());
  // mutex_.Unlock();
  // // Go to db iter
  // return NewDBIterator(this, user_comparator(), internal_iter,
  // latest_snapsshot,
  //                      0);
}

int DBImplZNS::NumberLevels(ColumnFamilyHandle* column_family) {
  return ZnsConfig::level_count;
}

const std::string& DBImplZNS::GetName() const { return name_; }

Env* DBImplZNS::GetEnv() const { return env_; }

Options DBImplZNS::GetOptions(ColumnFamilyHandle* column_family) const {
  Options options(options_, ColumnFamilyOptions());
  return options;
}

DBOptions DBImplZNS::GetDBOptions() const { return options_; };

SequenceNumber DBImplZNS::GetLatestSequenceNumber() const {
  return versions_->LastSequence();
}

}  // namespace ROCKSDB_NAMESPACE
