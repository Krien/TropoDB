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
      // State
      bg_work_finished_signal_(&mutex_),
      bg_compaction_scheduled_(false),
      bg_error_(Status::OK()),
      forced_schedule_(false) {}

DBImplZNS::~DBImplZNS() {
  mutex_.Lock();
  while (bg_compaction_scheduled_) {
    // printf("busy, wait before closing\n");
    bg_work_finished_signal_.Wait();
  }
  mutex_.Unlock();
  std::cout << versions_->DebugString();

  delete versions_;
  if (mem_ != nullptr) mem_->Unref();
  if (imm_ != nullptr) imm_->Unref();
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
  channel_factory_ =
      new SZD::SZDChannelFactory(zns_device_->GetDeviceManager(), 0x100);
  channel_factory_->Ref();
  return Status::OK();
}

Status DBImplZNS::ResetZNSDevice() {
  Status s;
  SZD::SZDChannel* channel;
  channel_factory_->Ref();
  SZD::DeviceInfo info;
  s = FromStatus(zns_device_->GetInfo(&info));
  if (!s.ok()) {
    return s;
  }
  channel_factory_->register_channel(&channel, ZnsConfig::min_zone,
                                     ZnsConfig::max_zone);
  s = FromStatus(channel->ResetAllZones());
  channel_factory_->unregister_channel(channel);
  channel_factory_->Unref();
  return s.ok() ? Status::OK() : Status::IOError("Error resetting device");
}

Status DBImplZNS::InitDB(const DBOptions& options) {
  assert(zns_device_ != nullptr);
  SZD::DeviceInfo device_info;
  zns_device_->GetInfo(&device_info);
  uint64_t zone_head = ZnsConfig::min_zone;
  uint64_t zone_step = 0;

  zone_step = ZnsConfig::manifest_zones;
  manifest_ = new ZnsManifest(channel_factory_, device_info, zone_head,
                              zone_head + zone_step);
  manifest_->Ref();
  zone_head += zone_step;

  zone_step = ZnsConfig::wal_count * ZnsConfig::zones_foreach_wal;
  wal_man_ = new ZnsWALManager(channel_factory_, device_info, zone_head,
                               zone_step + zone_head, ZnsConfig::wal_count);
  wal_man_->Ref();
  zone_head += zone_step;

  std::pair<uint64_t, uint64_t>* ranges =
      new std::pair<uint64_t, uint64_t>[ZnsConfig::level_count];
  {
    uint64_t nzones = device_info.lba_cap / device_info.zone_size;
    uint64_t distr = std::accumulate(
        ZnsConfig::ss_distribution,
        ZnsConfig::ss_distribution + ZnsConfig::level_count, 0U);
    for (size_t i = 0; i < ZnsConfig::level_count - 1; i++) {
      zone_step = (nzones / distr) * ZnsConfig::ss_distribution[i];
      zone_step = zone_step < ZnsConfig::min_ss_zone_count
                      ? ZnsConfig::min_ss_zone_count
                      : zone_step;
      ranges[i] = std::make_pair(zone_head, zone_head + zone_step);
      zone_head += zone_step;
    }
    zone_step = nzones - zone_head;
    ranges[ZnsConfig::level_count - 1] =
        std::make_pair(zone_head, zone_head + zone_step);
  }
  ss_manager_ = new ZNSSSTableManager(channel_factory_, device_info, ranges);
  delete[] ranges;
  ss_manager_->Ref();

  mem_ = new ZNSMemTable(options, this->internal_comparator_);
  mem_->Ref();

  Options opts(options, ColumnFamilyOptions());
  table_cache_ = new ZnsTableCache(opts, internal_comparator_, 1024 * 1024 * 4,
                                   ss_manager_);

  versions_ = new ZnsVersionSet(internal_comparator_, ss_manager_, manifest_,
                                device_info.lba_size, table_cache_);

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

Status DBImplZNS::Close() { return Status::OK(); }

Status DBImplZNS::Recover() {
  Status s;
  // Recover index structure
  s = versions_->Recover();
  // If there is no version to be recovered, we assume there is no valid DB.
  if (!s.ok()) {
    return options_.create_if_missing ? ResetZNSDevice() : s;
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
  // printf("Reset device\n");
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

Status DBImplZNS::Write(const WriteOptions& options, WriteBatch* updates) {
  Status s;
  MutexLock l(&mutex_);
  // TODO: syncing

  Slice log_entry;
  if (updates != nullptr) {
    log_entry = WriteBatchInternal::Contents(updates);
    s = MakeRoomForWrite(log_entry);
  }
  uint64_t last_sequence = versions_->LastSequence();

  // TODO make threadsafe for multiple writes and add writebatch
  // optimisations...

  // Write to what is needed
  if (s.ok() && updates != nullptr) {
    WriteBatchInternal::SetSequence(updates, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(updates);
    {
      wal_->Ref();
      mutex_.Unlock();
      s = wal_->Append(log_entry);
      // write to memtable
      assert(this->mem_ != nullptr);
      if (s.ok()) {
        s = mem_->Write(options, updates);
      }
      mutex_.Lock();
      wal_->Unref();
    }
    versions_->SetLastSequence(last_sequence);
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
