// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/zns_impl/db_impl_zns.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
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
      wal_man_(nullptr),
      versions_(nullptr),
      // Will be initialised even later
      wal_(nullptr),
      mem_(nullptr),
      imm_(nullptr),
      tmp_batch_(new WriteBatch),
      // State
      bg_work_finished_signal_(&mutex_),
      bg_compaction_scheduled_(false),
      bg_error_(Status::OK()),
      forced_schedule_(false) {}

void DBImplZNS::IODiagnostics() {
  printf("IO diagnostics:..........\n");
  struct ZNSDiagnostics totaldiag = {
      .bytes_written_ = 0, .bytes_read_ = 0, .zones_erased_ = 0};
  {
    ZNSDiagnostics diag = wal_man_->IODiagnostics();
    totaldiag.bytes_written_ += diag.bytes_written_;
    totaldiag.bytes_read_ += diag.bytes_read_;
    totaldiag.zones_erased_ += diag.zones_erased_;
  }
  {
    ZNSDiagnostics diag = ss_manager_->IODiagnostics();
    totaldiag.bytes_written_ += diag.bytes_written_;
    totaldiag.bytes_read_ += diag.bytes_read_;
    totaldiag.zones_erased_ += diag.zones_erased_;
  }
  {
    ZNSDiagnostics diag = manifest_->IODiagnostics();
    totaldiag.bytes_written_ += diag.bytes_written_;
    totaldiag.bytes_read_ += diag.bytes_read_;
    totaldiag.zones_erased_ += diag.zones_erased_;
  }
  printf(
      "TOTAL :\n\tWritten %lu bytes\n\tRead %lu bytes\n\tReset %lu "
      "zones\n",
      totaldiag.bytes_written_, totaldiag.bytes_read_, totaldiag.zones_erased_);
  printf(".........................\n");
}

DBImplZNS::~DBImplZNS() {
  mutex_.Lock();
  while (bg_compaction_scheduled_) {
    // printf("busy, wait before closing\n");
    bg_work_finished_signal_.Wait();
  }
  mutex_.Unlock();
  std::cout << versions_->DebugString();

  IODiagnostics();

  if (versions_ != nullptr) delete versions_;
  if (mem_ != nullptr) mem_->Unref();
  if (imm_ != nullptr) imm_->Unref();
  if (tmp_batch_ != nullptr) delete tmp_batch_;
  if (wal_man_ != nullptr) wal_man_->Unref();
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

Status DBImplZNS::InitDB(const DBOptions& options) {
  assert(zns_device_ != nullptr);
  SZD::DeviceInfo device_info;
  zns_device_->GetInfo(&device_info);
  uint64_t zone_head = device_info.min_lba / device_info.zone_size;
  uint64_t zone_step = 0;

  zone_step = ZnsConfig::manifest_zones;
  manifest_ = new ZnsManifest(channel_factory_, device_info, zone_head,
                              zone_head + zone_step);
  manifest_->Ref();
  zone_head += zone_step;

  zone_step = ZnsConfig::wal_count * ZnsConfig::zones_foreach_wal;
  wal_man_ = new ZnsWALManager<ZnsConfig::wal_count>(
      channel_factory_, device_info, zone_head, zone_step + zone_head);
  wal_man_->Ref();
  zone_head += zone_step;

  zone_step = device_info.max_lba / device_info.zone_size - zone_head;
  // If only we had access to C++23.
  ss_manager_ =
      ZNSSSTableManager::NewZNSSTableManager(channel_factory_, device_info,
                                             zone_head, zone_head + zone_step)
          .value_or(nullptr);
  if (ss_manager_ == nullptr) {
    return Status::Corruption();
  }
  ss_manager_->Ref();
  zone_head = device_info.max_lba / device_info.zone_size;

  mem_ = new ZNSMemTable(options, this->internal_comparator_);
  mem_->Ref();

  Options opts(options, ColumnFamilyOptions());
  table_cache_ = new ZnsTableCache(opts, internal_comparator_, 1024 * 1024 * 4,
                                   ss_manager_);

  versions_ = new ZnsVersionSet(internal_comparator_, ss_manager_, manifest_,
                                device_info.lba_size, device_info.zone_size,
                                table_cache_);

  return Status::OK();
}

Status DBImplZNS::InitWAL() {
  mutex_.AssertHeld();
  Status s;
  // We must force flush all WALs if there is not enough space.
  if (!wal_man_->WALAvailable()) {
    if (mem_->GetInternalSize() > 0) {
      imm_ = mem_;
      mem_ = new ZNSMemTable(options_, internal_comparator_);
      mem_->Ref();
    }
    MaybeScheduleCompaction(true);
    while (!wal_man_->WALAvailable()) {
      bg_work_finished_signal_.Wait();
    }
  }
  wal_ = wal_man_->GetCurrentWAL(&mutex_);
  wal_->Ref();
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
  DBImplZNS* impl = new DBImplZNS(db_options, name);
  s = impl->OpenZNSDevice("ZNSLSM");
  if (!s.ok()) return s;
  s = impl->InitDB(db_options);
  if (!s.ok()) return s;
  // setup WAL (WAL DIR)
  impl->mutex_.Lock();
  s = impl->Recover();
  if (s.ok()) {
    s = impl->InitWAL();
  }
  if (s.ok()) {
    // impl->RemoveObsoleteZones();
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

Status DBImplZNS::Close() {
  mutex_.Lock();
  while (bg_compaction_scheduled_) {
    // printf("busy, wait before closing\n");
    bg_work_finished_signal_.Wait();
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
  s = wal_man_->Recover(mem_, &old_seq);
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
  s = impl->InitDB(options);
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

Status DBImplZNS::MakeRoomForWrite(Slice log_entry) {
  mutex_.AssertHeld();
  Status s;
  bool allow_delay = true;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield error
      s = bg_error_;
      return s;
    }
    if (allow_delay && versions_->NeedsFlushing()) {
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);
      allow_delay = false;
      mutex_.Lock();
    } else if (!mem_->ShouldScheduleFlush() && wal_->SpaceLeft(log_entry)) {
      // space left in memory table
      break;
    } else if (imm_ != nullptr) {
      // flush is scheduled, wait...
      bg_work_finished_signal_.Wait();
    } else if (versions_->NeedsFlushing()) {
      printf("waiting for compaction\n");
      MaybeScheduleCompaction(false);
      bg_work_finished_signal_.Wait();
    } else if (!wal_man_->WALAvailable()) {
      bg_work_finished_signal_.Wait();
    } else {
      // create new WAL
      wal_->Sync();
      wal_->Unref();
      s = wal_man_->NewWAL(&mutex_, &wal_);
      wal_->Ref();
      // printf("Reset WAL\n");
      // Switch to fresh memtable
      imm_ = mem_;
      mem_ = new ZNSMemTable(options_, internal_comparator_);
      mem_->Ref();
      MaybeScheduleCompaction(false);
    }
  }
  return Status::OK();
}

WriteBatch* DBImplZNS::BuildBatchGroup(Writer** last_writer) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != nullptr);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {
    max_size = size + (128 << 10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
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
        result = tmp_batch_;
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
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }

  s = MakeRoomForWrite(
      updates == nullptr ? Slice("") : WriteBatchInternal::Contents(updates));
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  // Write to what is needed
  if (s.ok() && updates != nullptr) {
    WriteBatch* write_batch = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(write_batch);
    {
      wal_->Ref();
      mutex_.Unlock();
      // buffering does not really make sense at the moment.
      // later we might decide to implement "FSync" or "ZoneSync", then it
      // should be reinstagated.
      if (true) {
        s = wal_->DirectAppend(WriteBatchInternal::Contents(write_batch));
      } else {
        s = wal_->Append(WriteBatchInternal::Contents(write_batch));
        s = wal_->Sync();
      }
      // write to memtable
      assert(this->mem_ != nullptr);
      if (s.ok()) {
        s = mem_->Write(options, write_batch);
      }
      mutex_.Lock();
      wal_->Unref();
    }
    if (write_batch == tmp_batch_) tmp_batch_->Clear();

    versions_->SetLastSequence(last_sequence);
  }

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = s;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  return s;
}

Status DBImplZNS::Get(const ReadOptions& options, const Slice& key,
                      std::string* value) {
  MutexLock l(&mutex_);
  Status s;
  // This is absolutely necessary for locking logic because private pointers can
  // be changed in background work.
  ZNSMemTable* mem = mem_;
  ZNSMemTable* imm = imm_;
  ZnsVersion* current = versions_->current();
  mem->Ref();
  if (imm != nullptr) imm->Ref();
  current->Ref();
  LookupKey lkey(key, versions_->LastSequence());
  {
    mutex_.Unlock();
    if (mem->Get(options, lkey, value, &s)) {
    } else if (imm != nullptr && imm->Get(options, lkey, value, &s)) {
      printf("read from immutable!\n");
      // Done
    } else {
      s = current->Get(options, lkey, value);
    }
    mutex_.Lock();
  }

  // Ensures that old data can be removed.
  mem->Unref();
  if (imm != nullptr) imm->Unref();
  current->Unref();

  return s;
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
