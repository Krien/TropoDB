// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/zns_impl/tropodb_impl.h"

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
#include "db/zns_impl/tropodb_config.h"
#include "db/zns_impl/index/tropodb_compaction.h"
#include "db/zns_impl/index/tropodb_version.h"
#include "db/zns_impl/index/tropodb_version_set.h"
#include "db/zns_impl/io/szd_port.h"
#include "db/zns_impl/persistence/zns_manifest.h"
#include "db/zns_impl/persistence/zns_wal.h"
#include "db/zns_impl/persistence/zns_wal_manager.h"
#include "db/zns_impl/table/zns_sstable_manager.h"
#include "db/zns_impl/table/zns_table_cache.h"
#include "db/zns_impl/utils/tropodb_logger.h"
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

// TODO: we do not use this? Remove?
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
      versions_(nullptr),
      // Thread count (1 HIGH for each flush thread, 1~3 HIGH for each L0 thread
      // and 1~3 LOW for each LN thread)
      low_level_threads_((1 + ZnsConfig::compaction_allow_prefetching +
                          ZnsConfig::compaction_allow_deferring_writes)),
      high_level_threads_(ZnsConfig::lower_concurrency +
                          ZnsConfig::lower_concurrency *
                              (1 + ZnsConfig::compaction_allow_prefetching +
                               ZnsConfig::compaction_allow_deferring_writes)),
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
      clock_(SystemClock::Default().get()) {
  SetTropoDBLogLevel(ZnsConfig::default_log_level);
  // The following variables are set to safeguards
  for (size_t i = 0; i < ZnsConfig::lower_concurrency; i++) {
    wal_man_[i] = nullptr;
    wal_[i] = nullptr;
    mem_[i] = nullptr;
    imm_[i] = nullptr;
    bg_flush_scheduled_[i] = false;
    tmp_batch_[i] = new WriteBatch;
    wal_reserved_[i] = 0;
  }
  std::fill(compactions_.begin(), compactions_.end(), 0);
  // Setup background threads (RocksDB env will not let us make threads
  // otherwise)
  TROPODB_INFO("INFO: TropoDB will use a maximum of %u background threads\n",
               low_level_threads_ + high_level_threads_);
  env_->SetBackgroundThreads(high_level_threads_,
                             ROCKSDB_NAMESPACE::Env::Priority::HIGH);
  env_->SetBackgroundThreads(low_level_threads_,
                             ROCKSDB_NAMESPACE::Env::Priority::LOW);
}

DBImplZNS::~DBImplZNS() {
  TROPODB_INFO("INFO: Shutting down TropoDB\n");
  // Close all tasks
  {
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
  }
  TROPODB_INFO("INFO: All jobs done - ready to exit\n");
  PrintStats();

  // Reaping what we have sown with ref counters
  {
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
  }
  TROPODB_INFO("INFO: Exiting\n");
}

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
    TROPODB_ERROR("ERROR: SPDK will not init. Are you root?\n");
    return Status::IOError("Error opening SPDK");
  }
  s = FromStatus(
      zns_device_->Open(this->name_, ZnsConfig::min_zone, ZnsConfig::max_zone));
  if (!s.ok()) {
    TROPODB_ERROR("ERROR: SZD device does not open. Is it ZNS?\n");
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
  } else {
    TROPODB_ERROR("ERROR: Can not create I/O QPair for clearing device\n");
  }
  if (s.ok()) {
    s = FromStatus(channel_factory_->unregister_channel(channel));
  } else {
    TROPODB_ERROR("ERROR: Can not clear device \n");
  }
  channel_factory_->Unref();
  return s.ok() ? Status::OK() : Status::IOError("Error resetting device");
}

Status DBImplZNS::InitDB(const DBOptions& options,
                         const size_t max_write_buffer_size) {
  assert(zns_device_ != nullptr);
  max_write_buffer_size_ = max_write_buffer_size;

  // Setup info string
  std::ostringstream info_str;
  info_str << "==== Zone division ====\n";
  info_str << std::setfill('-') << std::setw(76) << "\n" << std::setfill(' ');
  info_str << std::left << std::setw(15) << "Structure" << std::right
           << std::setw(25) << "Begin (zone nr)" << std::setw(25)
           << "End (zone nr)"
           << "\n";
  info_str << std::setfill('-') << std::setw(76) << "\n" << std::setfill(' ');

  // Get device info
  SZD::DeviceInfo device_info;
  zns_device_->GetInfo(&device_info);
  uint64_t zone_head = device_info.min_lba / device_info.zone_size;
  uint64_t zone_step = 0;

  // Init manifest
  {
    zone_step = ZnsConfig::manifest_zones;
    manifest_ = new ZnsManifest(channel_factory_, device_info, zone_head,
                                zone_head + zone_step);
    manifest_->Ref();
    info_str << std::left << std::setw(15) << "Manifest" << std::right
             << std::setw(25) << zone_head << std::setw(25)
             << zone_head + zone_step << "\n";
    zone_head += zone_step;
  }

  // Init WALs
  {
    zone_step = ZnsConfig::wal_count * ZnsConfig::zones_foreach_wal;
    zone_step /= ZnsConfig::lower_concurrency;
    for (size_t i = 0; i < ZnsConfig::lower_concurrency; i++) {
      wal_man_[i] = new ZnsWALManager<ZnsConfig::wal_manager_zone_count>(
          channel_factory_, device_info, zone_head, zone_step + zone_head);
      wal_man_[i]->Ref();
      info_str << std::left << std::setw(15) << ("WALMAN-" + std::to_string(i))
               << std::right << std::setw(25) << zone_head << std::setw(25)
               << (zone_head + zone_step) << "\n";
      zone_head += zone_step;
    }
  }

  // Init SSTable manager
  {
    zone_step = device_info.max_lba / device_info.zone_size - zone_head - 1;
    // If only we had access to C++23.
    ss_manager_ =
        ZNSSSTableManager::NewZNSSTableManager(channel_factory_, device_info,
                                               zone_head, zone_head + zone_step)
            .value_or(nullptr);
    if (ss_manager_ == nullptr) {
      TROPODB_ERROR("ERROR: Could not initialise SSTable manager\n");
      return Status::Corruption();
    }
    ss_manager_->Ref();
    info_str << ss_manager_->LayoutDivisionString();
    zone_head = device_info.max_lba / device_info.zone_size;
  }

  // Init Memtables, table ache and version structure
  {
    for (size_t i = 0; i < ZnsConfig::lower_concurrency; i++) {
      mem_[i] = new ZNSMemTable(options, internal_comparator_,
                                max_write_buffer_size_);
      mem_[i]->Ref();
    }

    Options opts(options, ColumnFamilyOptions());
    table_cache_ = new ZnsTableCache(opts, internal_comparator_,
                                     1024 * 1024 * 4, ss_manager_);

    versions_ = new ZnsVersionSet(internal_comparator_, ss_manager_, manifest_,
                                  device_info.lba_size, device_info.zone_cap,
                                  table_cache_, this->env_);
  }

  // Print info string (if enabled)
  info_str << std::setfill('-') << std::setw(76) << "\n" << std::setfill(' ');
  layout_string_ = info_str.str();
  TROPODB_INFO("%s", layout_string_.data());

  return Status::OK();
}

void DBImplZNS::RecoverBackgroundFlow() {
  // Make WALs lively again
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
  // Reinstigate background jobs
  for (size_t i = 0; i < ZnsConfig::lower_concurrency; i++) {
    MaybeScheduleFlush(i);
  }
  MaybeScheduleCompactionL0();
  MaybeScheduleCompaction(false);
}

Status DBImplZNS::Recover() {
  TROPODB_INFO("INFO: recovering TropoDB\n");
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

  // Recover WAL and MVCC
  {
    SequenceNumber old_seq;
    for (size_t i = 0; i < ZnsConfig::lower_concurrency; i++) {
      s = wal_man_[i]->Recover(mem_[i], &old_seq);
    }
    if (!s.ok()) {
      return s;
    }
    versions_->SetLastSequence(old_seq);
  }

  // Recover flow
  RecoverBackgroundFlow();
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
    TROPODB_ERROR("ERROR: Invalid options for TropoDB\n");
    return s;
  }
  // We do not support column families, so we just clear them
  handles->clear();

  // Set write buffer size to an acceptable level
  size_t max_write_buffer_size = 0;
  for (auto cf : column_families) {
    max_write_buffer_size =
        std::max(max_write_buffer_size, cf.options.write_buffer_size);
  }

  // Open a SZD connection
  DBImplZNS* impl = new DBImplZNS(db_options, name);
  s = impl->OpenZNSDevice("TropoDB");
  if (!s.ok()) {
    return s;
  }

  // Divide device and recover/create components
  s = impl->InitDB(db_options, max_write_buffer_size);
  if (!s.ok()) {
    return s;
  }
  // Lock to ensure we are master of TropoDB, not bg threads
  impl->mutex_.Lock();
  s = impl->Recover();
  impl->mutex_.Unlock();

  // Return DB or null based on state
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
  // Wait till all jobs are done
  TROPODB_INFO("INFO: Closing: waiting for background jobs to finish\n");
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
  TROPODB_ERROR("ERROR: Closing: Not implemented\n");
  return Status::OK();
}

Status DBImplZNS::DestroyDB(const std::string& dbname, const Options& options) {
  // Destroy "all files" from the DB. Since we do not use multitenancy, we
  // might as well reset the device.
  Status s;
  DBImplZNS* impl = new DBImplZNS(options, dbname);
  TROPODB_INFO("INFO: Attemtping to reset entire ZNS device\n");
  s = impl->OpenZNSDevice("TropoDB");
  if (!s.ok()) {
    return s;
  }
  s = impl->InitDB(options, options.write_buffer_size);
  if (!s.ok()) {
    return s;
  }
  s = impl->ResetZNSDevice();
  if (!s.ok()) {
    return s;
  }
  TROPODB_INFO("INFO: Entire ZNS device has been reset\n");
  delete impl;
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
