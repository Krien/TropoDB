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

#include "rocksdb/db.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/listener.h"
#include "rocksdb/metadata.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/transaction_log.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

const int kNumNonTableCacheFiles = 10;

DBImplZNS::DBImplZNS(const DBOptions& options, const std::string& dbname,
                     const bool seq_per_batch, const bool batch_per_txn,
                     bool read_only)
    : name_(dbname) {}

Status DBImplZNS::NewDB() { return Status::OK(); }

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DBImplZNS::Put(const WriteOptions& options, const Slice& key,
                      const Slice& value) {
  uint64_t lba_size = (*this->qpair_)->man->info.lba_size;
  char* payload =
      (char*)ZnsDevice::z_calloc(*this->qpair_, lba_size, sizeof(char));
  strncpy(payload, "SStable", 8);
  char* payload_tmp = EncodeVarint32(payload + 7, key.size());
  payload_tmp = EncodeVarint32(payload_tmp, value.size());
  int w = 0;
  memcpy(payload_tmp, key.data(), key.size());
  w += key.size();
  memcpy(payload_tmp + w, value.data(), value.size());
  w += value.size();
  int rc = ZnsDevice::z_append(*this->qpair_, 0, payload, lba_size);
  return rc == 0 ? Status::OK() : Status::IOError("Error appending to zone");
  return Status::OK();
}

Status DBImplZNS::Put(const WriteOptions& options,
                      ColumnFamilyHandle* column_family, const Slice& key,
                      const Slice& value) {
  return Status::NotSupported();
}
Status DBImplZNS::Put(const WriteOptions& options,
                      ColumnFamilyHandle* column_family, const Slice& key,
                      const Slice& ts, const Slice& value) {
  return Status::NotSupported();
}

Status DBImplZNS::Get(const ReadOptions& options, const Slice& key,
                      std::string* value) {
  uint64_t lba_size = (*this->qpair_)->man->info.lba_size;
  char* val = (char*)ZnsDevice::z_calloc(*this->qpair_, lba_size, sizeof(char));
  int rc = ZnsDevice::z_read(*this->qpair_, 0, val, lba_size);
  if (strncmp(val, "SStable", strlen("sstable")) != 0) {
    return Status::NotFound("Invalid block");
  }
  const char* val_tmp = val + strlen("SStable");
  // get next 4 bytes for kv count
  uint32_t key_size, val_size;
  val_tmp = GetVarint32Ptr(val_tmp, val_tmp + 5, &key_size);
  val_tmp = GetVarint32Ptr(val_tmp, val_tmp + 5, &val_size);

  if (key_size + val_size + (uint32_t)(val_tmp - val) > lba_size) {
    return Status::NotFound("Invalid block, wrong offset");
  }
  if (strncmp(val_tmp, key.data(), key_size) == 0) {
    (*value).append((const char*)(val_tmp + key_size), (size_t)val_size);
    return Status::OK();
  }
  return Status::NotFound("Key not found");
}

Status DBImplZNS::Get(const ReadOptions& options,
                      ColumnFamilyHandle* column_family, const Slice& key,
                      PinnableSlice* value) {
  return Status::NotSupported();
}

Status DBImplZNS::Delete(const WriteOptions& opt, const Slice& key) {
  return Status::OK();
}

Status DBImplZNS::Delete(const WriteOptions& options,
                         ColumnFamilyHandle* column_family, const Slice& key) {
  return Status::NotSupported();
}
Status DBImplZNS::Delete(const WriteOptions& options,
                         ColumnFamilyHandle* column_family, const Slice& key,
                         const Slice& ts) {
  return Status::NotSupported();
}

Status DBImplZNS::Write(const WriteOptions& options, WriteBatch* updates) {
  return Status::NotSupported();
}

Status DBImplZNS::Merge(const WriteOptions& options,
                        ColumnFamilyHandle* column_family, const Slice& key,
                        const Slice& value) {
  return Status::OK();
};

bool DBImplZNS::SetPreserveDeletesSequenceNumber(SequenceNumber seqnum) {
  return false;
}

Status DBImplZNS::InitDB() {
  ZnsDevice::DeviceManager** device_manager =
      (ZnsDevice::DeviceManager**)calloc(1, sizeof(ZnsDevice::DeviceManager));
  int rc = ZnsDevice::z_init(device_manager);
  this->device_manager_ = device_manager;
  if (rc != 0) {
    return Status::IOError("Error opening SPDK");
  }
  rc = ZnsDevice::z_open(*device_manager, this->name_.c_str());
  if (rc != 0) {
    return Status::IOError("Error opening ZNS device");
  }
  this->qpair_ = (ZnsDevice::QPair**)calloc(1, sizeof(ZnsDevice::QPair*));
  rc = ZnsDevice::z_create_qpair(*device_manager, this->qpair_);
  if (rc != 0) {
    return Status::IOError("Error creating QPair");
  }
  rc = ZnsDevice::z_reset(*this->qpair_, 0, true);
  return rc == 0 ? Status::OK() : Status::IOError("Error resetting device");
}

DBImplZNS::~DBImplZNS() = default;

Status DBImplZNS::Open(
    const DBOptions& db_options, const std::string& name,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, DB** dbptr,
    const bool seq_per_batch, const bool batch_per_txn) {
  DBImplZNS* impl = new DBImplZNS(db_options, name);
  *dbptr = (DB*)impl;
  return impl->InitDB();
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
Env* DBImplZNS::GetEnv() const { return NULL; }
Options DBImplZNS::GetOptions(ColumnFamilyHandle* column_family) const {
  Options options_;
  return options_;
}
DBOptions DBImplZNS::GetDBOptions() const {
  DBOptions options_;
  return options_;
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
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
