// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/zns_impl/db_impl_zns.h"

namespace ROCKSDB_NAMESPACE {

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

Status DBImplZNS::Delete(const WriteOptions& options,
                         ColumnFamilyHandle* column_family, const Slice& key) {
  return Status::NotSupported("Column families not supported");
}

Status DBImplZNS::Delete(const WriteOptions& options,
                         ColumnFamilyHandle* column_family, const Slice& key,
                         const Slice& ts) {
  return Status::NotSupported("Column families not supported");
}

Status DBImplZNS::Get(const ReadOptions& options,
                      ColumnFamilyHandle* column_family, const Slice& key,
                      PinnableSlice* value) {
  return Status::NotSupported("Column families not supported");
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

}  // namespace ROCKSDB_NAMESPACE