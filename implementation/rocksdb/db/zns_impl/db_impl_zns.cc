// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/zns_impl/db_impl_zns.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <iostream>
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
#include "db/zns_impl/io/device_wrapper.h"
#include "db/zns_impl/persistence/zns_manifest.h"
#include "db/zns_impl/persistence/zns_wal.h"
#include "db/zns_impl/persistence/zns_wal_manager.h"
#include "db/zns_impl/table/zns_sstable_manager.h"
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
      env_(options.env),
      internal_comparator_(BytewiseComparator()),
      mem_(nullptr),
      imm_(nullptr),
      versions_(nullptr),
      bg_work_finished_signal_(&mutex_),
      bg_compaction_scheduled_(false) {}

Status DBImplZNS::NewDB() { return Status::OK(); }

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DBImplZNS::Put(const WriteOptions& options, const Slice& key,
                      const Slice& value) {
  // create writebatch of size key + value + 24 bytes.8 bytes are taken by
  // header(sequence number), 4 bytes for count, 1 byte for type + 11 for size.
  // then write the batch. the batch does some stuff with cf, that we do not
  // care about.  See top write_batch.cc for some awesome insights. Some flags
  // are stored as well, protected information and save points and commits can
  // be made (friend classes..) Then it moves into WriteImpl. writes have a prio
  // which can be assigned (ignore for now I guess...) Get counts from
  // WriteBatch back. Then write to WAL
  //    Write to WAL: separate thread (WAL thread?) something with batch
  //    groups..
  // then one write to the memtable, which we can copy I guess.
  // multiple variants, unordered writes, pipelined writes etc.
  // NOTE, no flushing or compaction at all! probably a different threads
  WriteBatch batch;
  batch.Put(key, value);
  return Write(options, &batch);
}

Status DBImplZNS::Put(const WriteOptions& options,
                      ColumnFamilyHandle* column_family, const Slice& key,
                      const Slice& value) {
  return Status::NotSupported("Column families not supported");
}
Status DBImplZNS::Put(const WriteOptions& options,
                      ColumnFamilyHandle* column_family, const Slice& key,
                      const Slice& ts, const Slice& value) {
  return Status::NotSupported("Column families not supported");
}

Status DBImplZNS::Get(const ReadOptions& options, const Slice& key,
                      std::string* value) {
  MutexLock l(&mutex_);
  Status s;
  /* GetImpl
  1. tracing, we dont care.
  2. sequence number? not relevant as of now. probably problematic for
  flushes...
  3. memtable lookup, copy? requires the superversion...
  4. if IsMergeInProgress!, look in immutable as well.
  5. can also return merge operands??? then get does nothing on normal table
  (except return) and gets operands for imm when exists. SLOW!
  6. get version logic. think for sstable locations? differentiates on action.
  **/

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

Status DBImplZNS::Get(const ReadOptions& options,
                      ColumnFamilyHandle* column_family, const Slice& key,
                      PinnableSlice* value) {
  return Status::NotSupported();
}

Status DBImplZNS::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

Status DBImplZNS::Delete(const WriteOptions& options,
                         ColumnFamilyHandle* column_family, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(options, &batch);
}
Status DBImplZNS::Delete(const WriteOptions& options,
                         ColumnFamilyHandle* column_family, const Slice& key,
                         const Slice& ts) {
  return Status::NotSupported();
}

Status DBImplZNS::Write(const WriteOptions& options, WriteBatch* updates) {
  Status s;
  MutexLock l(&mutex_);
  // TODO: syncing

  s = MakeRoomForWrite();
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
      Slice log_entry = WriteBatchInternal::Contents(updates);
      wal_->Append(log_entry);
      // write to memtable
      assert(this->mem_ != nullptr);
      this->mem_->Write(options, updates);
      mutex_.Lock();
      wal_->Unref();
    }
    versions_->SetLastSequence(last_sequence);
  }
  return s;
}

Status DBImplZNS::MakeRoomForWrite() {
  mutex_.AssertHeld();
  Status s;
  bool allow_delay = true;
  Slice stub;
  while (true) {
    if (allow_delay && versions_->NeedsFlushing()) {
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);
      allow_delay = false;
      mutex_.Lock();
    } else if (!mem_->ShouldScheduleFlush() && wal_->SpaceLeft(stub)) {
      // space left in memory table
      break;
    } else if (imm_ != nullptr) {
      // flush is scheduled, wait...
      // printf("is it done????\n");
      bg_work_finished_signal_.Wait();
    } else if (versions_->NeedsFlushing()) {
      // printf("waiting for compaction\n");
      bg_work_finished_signal_.Wait();
    } else if (!wal_man_->WALAvailable()) {
      // printf("waiting for WAL clearing\n");
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

void DBImplZNS::MaybeScheduleCompaction(bool force) {
  // printf("Scheduling?\n");
  mutex_.AssertHeld();
  if (bg_compaction_scheduled_) {
    return;
  }
  if (!force && imm_ == nullptr && !versions_->NeedsCompaction() &&
      wal_man_->WALAvailable()) {
    return;
  }
  forced_schedule_ = force;
  bg_compaction_scheduled_ = true;
  env_->Schedule(&DBImplZNS::BGWork, this, rocksdb::Env::HIGH);
}

void DBImplZNS::BGWork(void* db) {
  reinterpret_cast<DBImplZNS*>(db)->BackgroundCall();
}

void DBImplZNS::BackgroundCall() {
  MutexLock l(&mutex_);
  assert(bg_compaction_scheduled_);
  {
    // printf("starting background work\n");
    BackgroundCompaction();
  }
  bg_compaction_scheduled_ = false;
  forced_schedule_ = false;
  // cascading.
  MaybeScheduleCompaction(false);
  bg_work_finished_signal_.SignalAll();
}

void DBImplZNS::BackgroundCompaction() {
  mutex_.AssertHeld();
  Status s;
  if (imm_ != nullptr) {
    // printf("  Compact memtable...\n");
    s = CompactMemtable();
    return;
  }
  if (!wal_man_->WALAvailable()) {
    // printf(" Trying to free WALS...\n");
    s = wal_man_->ResetOldWALs(&mutex_);
    return;
  }
  // Compaction itself does not require a lock. only once the changes become
  // visible.
  mutex_.Unlock();
  ZnsVersionEdit edit;
  {
    // printf("  Compact LN...\n");
    ZnsCompaction compaction(versions_);
    versions_->Compact(&compaction);
    compaction.MarkStaleTargetsReusable(&edit);
    if (compaction.IsTrivialMove()) {
      s = compaction.DoTrivialMove(&edit);
    } else {
      s = compaction.DoCompaction(&edit);
    }
  }
  mutex_.Lock();
  if (!s.ok()) {
    printf("ERROR during compaction!!!\n");
    return;
  }
  s = s.ok() ? versions_->LogAndApply(&edit) : s;
  s = s.ok() ? RemoveObsoleteZones() : s;
  // printf("Compacted!!\n");
}

Status DBImplZNS::CompactMemtable() {
  mutex_.AssertHeld();
  assert(imm_ != nullptr);
  assert(bg_compaction_scheduled_);
  Status s;
  // Flush and set new version
  {
    ZnsVersionEdit edit;
    SSZoneMetaData meta;
    meta.number = versions_->NewSSNumber();
    s = FlushL0SSTables(&meta);
    int level = 0;
    if (s.ok() && meta.lba_count > 0) {
      edit.AddSSDefinition(level, meta.number, meta.lba, meta.lba_count,
                           meta.numbers, meta.smallest, meta.largest);
      s = versions_->LogAndApply(&edit);
    } else {
      printf("Fatal error \n");
    }
    imm_->Unref();
    imm_ = nullptr;
    // wal
    s = wal_man_->ResetOldWALs(&mutex_);
    if (!s.ok()) return s;
    s = RemoveObsoleteZones();
    // printf("Flushed!!\n");
  }
  return s;
}

Status DBImplZNS::FlushL0SSTables(SSZoneMetaData* meta) {
  Status s;
  ss_manager_->Ref();
  s = ss_manager_->FlushMemTable(imm_, meta);
  ss_manager_->Unref();
  return s;
}

Status DBImplZNS::RemoveObsoleteZones() {
  mutex_.AssertHeld();
  Status s = Status::OK();
  // // table files
  std::vector<std::pair<uint64_t, uint64_t>> ranges;
  for (size_t i = 0; i < ZnsConfig::level_count; i++) {
    versions_->GetLiveZoneRanges(i, &ranges);
    s = ss_manager_->InvalidateUpTo(i, ranges.back().first);
    if (!s.ok()) return s;
  }
  return s;
}

Status DBImplZNS::Merge(const WriteOptions& options,
                        ColumnFamilyHandle* column_family, const Slice& key,
                        const Slice& value) {
  return Status::OK();
}

bool DBImplZNS::SetPreserveDeletesSequenceNumber(SequenceNumber seqnum) {
  return false;
}

Status DBImplZNS::OpenDevice() {
  device_manager_ = new SZD::DeviceManager*;
  int rc = 0;
  SZD::DeviceOptions device_options = {.name = "ZNSLSM", !SZD::device_set};
  if (!SZD::device_set) {
    rc = SZD::z_init(device_manager_, &device_options);
    SZD::device_set = true;
  } else {
    rc = SZD::z_init(device_manager_, &device_options);
  }
  if (rc != 0) {
    return Status::IOError("Error opening SPDK");
  }
  rc = SZD::z_open(*device_manager_, this->name_.c_str());
  if (rc != 0) {
    return Status::IOError("Error opening ZNS device");
  }
  qpair_factory_ = new QPairFactory(*device_manager_);
  qpair_factory_->Ref();
  return Status::OK();
}

Status DBImplZNS::InitDB(const DBOptions& options) {
  assert(device_manager_ != nullptr);
  SZD::DeviceInfo device_info = (*device_manager_)->info;
  manifest_ =
      new ZnsManifest(qpair_factory_, device_info, 0,
                      device_info.zone_size * ZnsConfig::manifest_zones);
  manifest_->Ref();

  wal_man_ =
      new ZnsWALManager(qpair_factory_, device_info,
                        device_info.zone_size * ZnsConfig::manifest_zones,
                        device_info.zone_size * ZnsConfig::manifest_zones +
                            device_info.zone_size * ZnsConfig::wal_count *
                                ZnsConfig::zones_foreach_wal,
                        ZnsConfig::wal_count);
  wal_man_->Ref();

  std::pair<uint64_t, uint64_t>* ranges =
      new std::pair<uint64_t, uint64_t>[ZnsConfig::level_count];
  {
    uint64_t nzones = device_info.lba_cap / device_info.zone_size;
    uint64_t distr = 0;
    for (size_t i = 0; i < ZnsConfig::level_count; i++) {
      distr += ZnsConfig::ss_distribution[i];
    }
    uint64_t distr_walker =
        (ZnsConfig::manifest_zones +
         ZnsConfig::wal_count * ZnsConfig::zones_foreach_wal) *
        device_info.zone_size;
    uint64_t step = 0;
    for (size_t i = 0; i < ZnsConfig::level_count - 1; i++) {
      step = (nzones / distr) * ZnsConfig::ss_distribution[i] *
             device_info.zone_size;
      step = (step / device_info.zone_size) < ZnsConfig::min_ss_zone_count
                 ? ZnsConfig::min_ss_zone_count * device_info.zone_size
                 : step;
      ranges[i] = std::make_pair(distr_walker, distr_walker + step);
      distr_walker += step;
      printf("SS range  %lu %lu \n", ranges[i].first / device_info.zone_size,
             ranges[i].second / device_info.zone_size);
    }
    step = (nzones * device_info.zone_size) - distr_walker;
    ranges[ZnsConfig::level_count - 1] =
        std::make_pair(distr_walker, distr_walker + step);
    printf("SS range  %lu %lu \n",
           ranges[ZnsConfig::level_count - 1].first / device_info.zone_size,
           ranges[ZnsConfig::level_count - 1].second / device_info.zone_size);
  }
  ss_manager_ = new ZNSSSTableManager(qpair_factory_, device_info, ranges);
  ss_manager_->Ref();

  mem_ = new ZNSMemTable(options, this->internal_comparator_);
  mem_->Ref();

  versions_ = new ZnsVersionSet(internal_comparator_, ss_manager_, manifest_,
                                device_info.lba_size);
  return Status::OK();
}

Status DBImplZNS::ResetDevice() {
  qpair_factory_->Ref();
  SZD::QPair** qpair = (SZD::QPair**)calloc(1, sizeof(SZD::QPair*));
  qpair_factory_->register_qpair(qpair);
  int rc = SZD::z_reset(*qpair, 0, true);
  qpair_factory_->unregister_qpair(*qpair);
  qpair_factory_->Unref();
  delete qpair;
  return rc == 0 ? Status::OK() : Status::IOError("Error resetting device");
}

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
  if (qpair_factory_ != nullptr) qpair_factory_->Unref();
  if (device_manager_ != nullptr) {
    SZD::z_destroy(*device_manager_);
    free(device_manager_);
  }
  // printf("exiting \n");
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
  s = impl->OpenDevice();
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
    impl->RemoveObsoleteZones();
    impl->MaybeScheduleCompaction(false);
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    *dbptr = reinterpret_cast<DB*>(impl);
  } else {
    delete impl;
  }
  return s;
  // recover?
  //  !readonly: set directories, lockfile and check if current manifest exists
  //  create_if_missing (NewDB). verify options and system compability readonly
  //  find or error
  // recover version
  // setid
  // recover from WAL
  // mutex
  // s = impl->Recover(column_families, false, false, false, &recovered_seq);

  // if (s.ok()) {
  // do something
  // new wall with higher version? max_write_buffer_size
  // increment superversion
  //}
  // write options file
  // if (s.ok()) {
  // persist_options_status = impl->WriteOptionsFile(
  //     false /*need_mutex_lock*/, false /*need_enter_write_thread*/);
  // // delete obsolete files, maybe schedule or flush
  //}

  // get live files metadata

  // reserve disk bufferspace
}

Status DBImplZNS::Recover() {
  Status s;
  // Recover index structure
  s = versions_->Recover();
  // If there is no version to be recovered, we assume there is no valid DB.
  if (!s.ok()) {
    return options_.create_if_missing ? ResetDevice() : s;
  }
  // TODO: currently this still writes a new version... if not an identical one.
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

Status DBImplZNS::InitWAL() {
  mutex_.AssertHeld();
  Status s;
  // We must force flush all WALs if there is not enough space.
  if (!wal_man_->WALAvailable() || !wal_man_->SafeToDiscard()) {
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
  s = wal_man_->NewWAL(&mutex_, &wal_);
  wal_->Ref();
  return s;
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

Status DBImplZNS::Close() { return Status::OK(); }

const Snapshot* DBImplZNS::GetSnapshot() { return nullptr; }

void DBImplZNS::ReleaseSnapshot(const Snapshot* snapshot) {}

Status DBImplZNS::GetMergeOperands(
    const ReadOptions& options, ColumnFamilyHandle* column_family,
    const Slice& key, PinnableSlice* merge_operands,
    GetMergeOperandsOptions* get_merge_operands_options,
    int* number_of_operands) {
  return Status::NotSupported();
}

std::vector<Status> DBImplZNS::MultiGet(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_family,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  std::vector<Status> test;
  return test;
}
std::vector<Status> DBImplZNS::MultiGet(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_family,
    const std::vector<Slice>& keys, std::vector<std::string>* values,
    std::vector<std::string>* timestamps) {
  std::vector<Status> test;
  return test;
}

Status DBImplZNS::SingleDelete(const WriteOptions& options,
                               ColumnFamilyHandle* column_family,
                               const Slice& key, const Slice& ts) {
  return Status::NotSupported();
}

Status DBImplZNS::SingleDelete(const WriteOptions& options,
                               ColumnFamilyHandle* column_family,
                               const Slice& key) {
  return Status::NotSupported();
}

Iterator* DBImplZNS::NewIterator(const ReadOptions& options,
                                 ColumnFamilyHandle* column_family) {
  return NULL;
}
Status DBImplZNS::NewIterators(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_families,
    std::vector<Iterator*>* iterators) {
  return Status::NotSupported();
}
bool DBImplZNS::GetProperty(ColumnFamilyHandle* column_family,
                            const Slice& property, std::string* value) {
  return false;
}
bool DBImplZNS::GetMapProperty(ColumnFamilyHandle* column_family,
                               const Slice& property,
                               std::map<std::string, std::string>* value) {
  return false;
}
bool DBImplZNS::GetIntProperty(ColumnFamilyHandle* column_family,
                               const Slice& property, uint64_t* value) {
  return false;
}
bool DBImplZNS::GetAggregatedIntProperty(const Slice& property,
                                         uint64_t* aggregated_value) {
  return false;
};
Status DBImplZNS::GetApproximateSizes(const SizeApproximationOptions& options,
                                      ColumnFamilyHandle* column_family,
                                      const Range* range, int n,
                                      uint64_t* sizes) {
  return Status::NotSupported();
};
void DBImplZNS::GetApproximateMemTableStats(ColumnFamilyHandle* column_family,
                                            const Range& range,
                                            uint64_t* const count,
                                            uint64_t* const size){};
Status DBImplZNS::CompactRange(const CompactRangeOptions& options,
                               ColumnFamilyHandle* column_family,
                               const Slice* begin, const Slice* end) {
  return Status::NotSupported();
};
Status DBImplZNS::SetDBOptions(
    const std::unordered_map<std::string, std::string>& options_map) {
  return Status::NotSupported();
}
Status DBImplZNS::CompactFiles(
    const CompactionOptions& compact_options, ColumnFamilyHandle* column_family,
    const std::vector<std::string>& input_file_names, const int output_level,
    const int output_path_id, std::vector<std::string>* const output_file_names,
    CompactionJobInfo* compaction_job_info) {
  return Status::NotSupported();
}
Status DBImplZNS::PauseBackgroundWork() { return Status::NotSupported(); }
Status DBImplZNS::ContinueBackgroundWork() { return Status::NotSupported(); }
Status DBImplZNS::EnableAutoCompaction(
    const std::vector<ColumnFamilyHandle*>& column_family_handles) {
  return Status::NotSupported();
}
void DBImplZNS::EnableManualCompaction() {}
void DBImplZNS::DisableManualCompaction() {}
int DBImplZNS::NumberLevels(ColumnFamilyHandle* column_family) { return 0; }
int DBImplZNS::MaxMemCompactionLevel(ColumnFamilyHandle* column_family) {
  return 0;
}
int DBImplZNS::Level0StopWriteTrigger(ColumnFamilyHandle* column_family) {
  return 0;
}
const std::string& DBImplZNS::GetName() const { return name_; }
Env* DBImplZNS::GetEnv() const { return env_; }
Options DBImplZNS::GetOptions(ColumnFamilyHandle* column_family) const {
  Options options(options_, ColumnFamilyOptions());
  return options;
}
DBOptions DBImplZNS::GetDBOptions() const {
  Options options(options_, ColumnFamilyOptions());
  return options;
};
Status DBImplZNS::Flush(const FlushOptions& options,
                        ColumnFamilyHandle* column_family) {
  return Status::NotSupported();
}
Status DBImplZNS::Flush(
    const FlushOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_families) {
  return Status::NotSupported();
}

Status DBImplZNS::SyncWAL() { return Status::NotSupported(); }

SequenceNumber DBImplZNS::GetLatestSequenceNumber() const {
  SequenceNumber n = 0;
  return n;
}

Status DBImplZNS::DisableFileDeletions() { return Status::NotSupported(); }

Status DBImplZNS::IncreaseFullHistoryTsLow(ColumnFamilyHandle* column_family,
                                           std::string ts_low) {
  return Status::NotSupported();
}

Status DBImplZNS::GetFullHistoryTsLow(ColumnFamilyHandle* column_family,
                                      std::string* ts_low) {
  return Status::NotSupported();
}

Status DBImplZNS::EnableFileDeletions(bool force) {
  return Status::NotSupported();
}

Status DBImplZNS::GetLiveFiles(std::vector<std::string>&,
                               uint64_t* manifest_file_size,
                               bool flush_memtable) {
  return Status::NotSupported();
}
Status DBImplZNS::GetSortedWalFiles(VectorLogPtr& files) {
  return Status::NotSupported();
}
Status DBImplZNS::GetCurrentWalFile(
    std::unique_ptr<LogFile>* current_log_file) {
  return Status::NotSupported();
}
Status DBImplZNS::GetCreationTimeOfOldestFile(uint64_t* creation_time) {
  return Status::NotSupported();
}

Status DBImplZNS::GetUpdatesSince(
    SequenceNumber seq_number, std::unique_ptr<TransactionLogIterator>* iter,
    const TransactionLogIterator::ReadOptions& read_options) {
  return Status::NotSupported();
}

Status DBImplZNS::DeleteFile(std::string name) {
  return Status::NotSupported();
}

Status DBImplZNS::GetLiveFilesChecksumInfo(FileChecksumList* checksum_list) {
  return Status::NotSupported();
}

Status DBImplZNS::GetLiveFilesStorageInfo(
    const LiveFilesStorageInfoOptions& opts,
    std::vector<LiveFileStorageInfo>* files) {
  return Status::NotSupported();
}

Status DBImplZNS::IngestExternalFile(
    ColumnFamilyHandle* column_family,
    const std::vector<std::string>& external_files,
    const IngestExternalFileOptions& ingestion_options) {
  return Status::NotSupported();
}

Status DBImplZNS::IngestExternalFiles(
    const std::vector<IngestExternalFileArg>& args) {
  return Status::NotSupported();
}

Status DBImplZNS::CreateColumnFamilyWithImport(
    const ColumnFamilyOptions& options, const std::string& column_family_name,
    const ImportColumnFamilyOptions& import_options,
    const ExportImportFilesMetaData& metadata, ColumnFamilyHandle** handle) {
  return Status::NotSupported();
}

Status DBImplZNS::VerifyChecksum(const ReadOptions& /*read_options*/) {
  return Status::NotSupported();
}

Status DBImplZNS::GetDbIdentity(std::string& identity) const {
  return Status::NotSupported();
}

Status DBImplZNS::GetDbSessionId(std::string& session_id) const {
  return Status::NotSupported();
}

ColumnFamilyHandle* DBImplZNS::DefaultColumnFamily() const { return NULL; }

Status DBImplZNS::GetPropertiesOfAllTables(ColumnFamilyHandle* column_family,
                                           TablePropertiesCollection* props) {
  return Status::NotSupported();
}
Status DBImplZNS::GetPropertiesOfTablesInRange(
    ColumnFamilyHandle* column_family, const Range* range, std::size_t n,
    TablePropertiesCollection* props) {
  return Status::NotSupported();
}

Status DBImplZNS::DestroyDB(const std::string& dbname, const Options& options) {
  // Destroy "all" files from the DB. Since we do not use multitenancy, we might
  // as well reset the device.
  Status s;
  DBImplZNS* impl = new DBImplZNS(options, dbname);
  s = impl->OpenDevice();
  if (!s.ok()) return s;
  s = impl->InitDB(options);
  if (!s.ok()) return s;
  s = impl->ResetDevice();
  if (!s.ok()) return s;
  // printf("Reset device\n");
  delete impl;
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
