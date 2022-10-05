// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/zns_impl/db_impl_zns.h"
#include "db/zns_impl/utils/tropodb_logger.h"

namespace ROCKSDB_NAMESPACE {

Status DBImplZNS::Merge(const WriteOptions& options,
                        ColumnFamilyHandle* column_family, const Slice& key,
                        const Slice& value) {
  TROPODB_ERROR("Not implemented\n");
  return Status::OK();
}

int DBImplZNS::MaxMemCompactionLevel(ColumnFamilyHandle* column_family) {
  TROPODB_INFO("Not implemented\n");
  return 0;
}

int DBImplZNS::Level0StopWriteTrigger(ColumnFamilyHandle* column_family) {
  TROPODB_INFO("Not implemented\n");
  return 0;
}

const Snapshot* DBImplZNS::GetSnapshot() { return nullptr; }

void DBImplZNS::ReleaseSnapshot(const Snapshot* snapshot) {}

Status DBImplZNS::GetMergeOperands(
    const ReadOptions& options, ColumnFamilyHandle* column_family,
    const Slice& key, PinnableSlice* merge_operands,
    GetMergeOperandsOptions* get_merge_operands_options,
    int* number_of_operands) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

std::vector<Status> DBImplZNS::MultiGet(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_family,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  std::vector<Status> test;
  TROPODB_ERROR("Not implemented\n");
  return test;
}
std::vector<Status> DBImplZNS::MultiGet(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_family,
    const std::vector<Slice>& keys, std::vector<std::string>* values,
    std::vector<std::string>* timestamps) {
  std::vector<Status> test;
  TROPODB_ERROR("Not implemented\n");
  return test;
}

Status DBImplZNS::SingleDelete(const WriteOptions& options,
                               ColumnFamilyHandle* column_family,
                               const Slice& key, const Slice& ts) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status DBImplZNS::SingleDelete(const WriteOptions& options,
                               ColumnFamilyHandle* column_family,
                               const Slice& key) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status DBImplZNS::Put(const WriteOptions& options,
                      ColumnFamilyHandle* column_family, const Slice& key,
                      const Slice& value) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported("Column families not supported");
}

Status DBImplZNS::Put(const WriteOptions& options,
                      ColumnFamilyHandle* column_family, const Slice& key,
                      const Slice& ts, const Slice& value) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported("Column families not supported");
}

Status DBImplZNS::Delete(const WriteOptions& options,
                         ColumnFamilyHandle* column_family, const Slice& key) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported("Column families not supported");
}

Status DBImplZNS::Delete(const WriteOptions& options,
                         ColumnFamilyHandle* column_family, const Slice& key,
                         const Slice& ts) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported("Column families not supported");
}

Status DBImplZNS::Get(const ReadOptions& options,
                      ColumnFamilyHandle* column_family, const Slice& key,
                      PinnableSlice* value) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported("Column families not supported");
}

Status DBImplZNS::NewIterators(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_families,
    std::vector<Iterator*>* iterators) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}
bool DBImplZNS::GetProperty(ColumnFamilyHandle* column_family,
                            const Slice& property, std::string* value) {
  TROPODB_ERROR("Not implemented\n");
  return false;
}
bool DBImplZNS::GetMapProperty(ColumnFamilyHandle* column_family,
                               const Slice& property,
                               std::map<std::string, std::string>* value) {
  TROPODB_ERROR("Not implemented\n");
  return false;
}
bool DBImplZNS::GetIntProperty(ColumnFamilyHandle* column_family,
                               const Slice& property, uint64_t* value) {
  TROPODB_ERROR("Not implemented\n");
  return false;
}
bool DBImplZNS::GetAggregatedIntProperty(const Slice& property,
                                         uint64_t* aggregated_value) {
  TROPODB_ERROR("Not implemented\n");
  return false;
};
Status DBImplZNS::GetApproximateSizes(const SizeApproximationOptions& options,
                                      ColumnFamilyHandle* column_family,
                                      const Range* range, int n,
                                      uint64_t* sizes) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
};
void DBImplZNS::GetApproximateMemTableStats(ColumnFamilyHandle* column_family,
                                            const Range& range,
                                            uint64_t* const count,
                                            uint64_t* const size){};
Status DBImplZNS::CompactRange(const CompactRangeOptions& options,
                               ColumnFamilyHandle* column_family,
                               const Slice* begin, const Slice* end) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
};
Status DBImplZNS::SetDBOptions(
    const std::unordered_map<std::string, std::string>& options_map) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}
Status DBImplZNS::CompactFiles(
    const CompactionOptions& compact_options, ColumnFamilyHandle* column_family,
    const std::vector<std::string>& input_file_names, const int output_level,
    const int output_path_id, std::vector<std::string>* const output_file_names,
    CompactionJobInfo* compaction_job_info) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}
Status DBImplZNS::PauseBackgroundWork() { 
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported(); 
}
Status DBImplZNS::ContinueBackgroundWork() { 
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported(); 
}
Status DBImplZNS::EnableAutoCompaction(
    const std::vector<ColumnFamilyHandle*>& column_family_handles) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}
void DBImplZNS::EnableManualCompaction() {
    TROPODB_ERROR("Not implemented\n");
}
void DBImplZNS::DisableManualCompaction() {
    TROPODB_ERROR("Not implemented\n");
}

Status DBImplZNS::Flush(const FlushOptions& options,
                        ColumnFamilyHandle* column_family) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}
Status DBImplZNS::Flush(
    const FlushOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_families) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status DBImplZNS::SyncWAL() { 
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported(); 
}

Status DBImplZNS::DisableFileDeletions() { 
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported(); 
}

Status DBImplZNS::IncreaseFullHistoryTsLow(ColumnFamilyHandle* column_family,
                                           std::string ts_low) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status DBImplZNS::GetFullHistoryTsLow(ColumnFamilyHandle* column_family,
                                      std::string* ts_low) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status DBImplZNS::EnableFileDeletions(bool force) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status DBImplZNS::GetLiveFiles(std::vector<std::string>&,
                               uint64_t* manifest_file_size,
                               bool flush_memtable) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}
Status DBImplZNS::GetSortedWalFiles(VectorLogPtr& files) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}
Status DBImplZNS::GetCurrentWalFile(
    std::unique_ptr<LogFile>* current_log_file) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}
Status DBImplZNS::GetCreationTimeOfOldestFile(uint64_t* creation_time) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status DBImplZNS::GetUpdatesSince(
    SequenceNumber seq_number, std::unique_ptr<TransactionLogIterator>* iter,
    const TransactionLogIterator::ReadOptions& read_options) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status DBImplZNS::DeleteFile(std::string name) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status DBImplZNS::GetLiveFilesChecksumInfo(FileChecksumList* checksum_list) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status DBImplZNS::GetLiveFilesStorageInfo(
    const LiveFilesStorageInfoOptions& opts,
    std::vector<LiveFileStorageInfo>* files) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status DBImplZNS::IngestExternalFile(
    ColumnFamilyHandle* column_family,
    const std::vector<std::string>& external_files,
    const IngestExternalFileOptions& ingestion_options) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status DBImplZNS::IngestExternalFiles(
    const std::vector<IngestExternalFileArg>& args) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status DBImplZNS::CreateColumnFamilyWithImport(
    const ColumnFamilyOptions& options, const std::string& column_family_name,
    const ImportColumnFamilyOptions& import_options,
    const ExportImportFilesMetaData& metadata, ColumnFamilyHandle** handle) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status DBImplZNS::VerifyChecksum(const ReadOptions& /*read_options*/) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status DBImplZNS::GetDbIdentity(std::string& identity) const {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status DBImplZNS::GetDbSessionId(std::string& session_id) const {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

ColumnFamilyHandle* DBImplZNS::DefaultColumnFamily() const { 
  TROPODB_ERROR("Not implemented\n");
  return NULL; 
}

Status DBImplZNS::GetPropertiesOfAllTables(ColumnFamilyHandle* column_family,
                                           TablePropertiesCollection* props) {       
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}
Status DBImplZNS::GetPropertiesOfTablesInRange(
    ColumnFamilyHandle* column_family, const Range* range, std::size_t n,
    TablePropertiesCollection* props) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

}  // namespace ROCKSDB_NAMESPACE