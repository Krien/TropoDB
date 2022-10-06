// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/tropodb/tropodb_impl.h"
#include "db/tropodb/utils/tropodb_logger.h"

namespace ROCKSDB_NAMESPACE {

Iterator* TropoDBImpl::NewIterator(const ReadOptions& options,
                                 ColumnFamilyHandle* column_family) {
  TROPODB_ERROR("Not implemented\n");
  return NULL;
}

Status TropoDBImpl::Merge(const WriteOptions& options,
                        ColumnFamilyHandle* column_family, const Slice& key,
                        const Slice& value) {
  TROPODB_ERROR("Not implemented\n");
  return Status::OK();
}

int TropoDBImpl::MaxMemCompactionLevel(ColumnFamilyHandle* column_family) {
  TROPODB_INFO("Not implemented\n");
  return 0;
}

int TropoDBImpl::Level0StopWriteTrigger(ColumnFamilyHandle* column_family) {
  TROPODB_INFO("Not implemented\n");
  return 0;
}

const Snapshot* TropoDBImpl::GetSnapshot() { return nullptr; }

void TropoDBImpl::ReleaseSnapshot(const Snapshot* snapshot) {}

Status TropoDBImpl::GetMergeOperands(
    const ReadOptions& options, ColumnFamilyHandle* column_family,
    const Slice& key, PinnableSlice* merge_operands,
    GetMergeOperandsOptions* get_merge_operands_options,
    int* number_of_operands) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

std::vector<Status> TropoDBImpl::MultiGet(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_family,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  std::vector<Status> test;
  TROPODB_ERROR("Not implemented\n");
  return test;
}
std::vector<Status> TropoDBImpl::MultiGet(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_family,
    const std::vector<Slice>& keys, std::vector<std::string>* values,
    std::vector<std::string>* timestamps) {
  std::vector<Status> test;
  TROPODB_ERROR("Not implemented\n");
  return test;
}

Status TropoDBImpl::SingleDelete(const WriteOptions& options,
                               ColumnFamilyHandle* column_family,
                               const Slice& key, const Slice& ts) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status TropoDBImpl::SingleDelete(const WriteOptions& options,
                               ColumnFamilyHandle* column_family,
                               const Slice& key) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status TropoDBImpl::Put(const WriteOptions& options,
                      ColumnFamilyHandle* column_family, const Slice& key,
                      const Slice& value) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported("Column families not supported");
}

Status TropoDBImpl::Put(const WriteOptions& options,
                      ColumnFamilyHandle* column_family, const Slice& key,
                      const Slice& ts, const Slice& value) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported("Column families not supported");
}

Status TropoDBImpl::Delete(const WriteOptions& options,
                         ColumnFamilyHandle* column_family, const Slice& key) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported("Column families not supported");
}

Status TropoDBImpl::Delete(const WriteOptions& options,
                         ColumnFamilyHandle* column_family, const Slice& key,
                         const Slice& ts) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported("Column families not supported");
}

Status TropoDBImpl::Get(const ReadOptions& options,
                      ColumnFamilyHandle* column_family, const Slice& key,
                      PinnableSlice* value) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported("Column families not supported");
}

Status TropoDBImpl::NewIterators(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_families,
    std::vector<Iterator*>* iterators) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}
bool TropoDBImpl::GetProperty(ColumnFamilyHandle* column_family,
                            const Slice& property, std::string* value) {
  TROPODB_ERROR("Not implemented\n");
  return false;
}
bool TropoDBImpl::GetMapProperty(ColumnFamilyHandle* column_family,
                               const Slice& property,
                               std::map<std::string, std::string>* value) {
  TROPODB_ERROR("Not implemented\n");
  return false;
}
bool TropoDBImpl::GetIntProperty(ColumnFamilyHandle* column_family,
                               const Slice& property, uint64_t* value) {
  TROPODB_ERROR("Not implemented\n");
  return false;
}
bool TropoDBImpl::GetAggregatedIntProperty(const Slice& property,
                                         uint64_t* aggregated_value) {
  TROPODB_ERROR("Not implemented\n");
  return false;
};
Status TropoDBImpl::GetApproximateSizes(const SizeApproximationOptions& options,
                                      ColumnFamilyHandle* column_family,
                                      const Range* range, int n,
                                      uint64_t* sizes) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
};
void TropoDBImpl::GetApproximateMemTableStats(ColumnFamilyHandle* column_family,
                                            const Range& range,
                                            uint64_t* const count,
                                            uint64_t* const size){};
Status TropoDBImpl::CompactRange(const CompactRangeOptions& options,
                               ColumnFamilyHandle* column_family,
                               const Slice* begin, const Slice* end) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
};
Status TropoDBImpl::SetDBOptions(
    const std::unordered_map<std::string, std::string>& options_map) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}
Status TropoDBImpl::CompactFiles(
    const CompactionOptions& compact_options, ColumnFamilyHandle* column_family,
    const std::vector<std::string>& input_file_names, const int output_level,
    const int output_path_id, std::vector<std::string>* const output_file_names,
    CompactionJobInfo* compaction_job_info) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}
Status TropoDBImpl::PauseBackgroundWork() {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}
Status TropoDBImpl::ContinueBackgroundWork() {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}
Status TropoDBImpl::EnableAutoCompaction(
    const std::vector<ColumnFamilyHandle*>& column_family_handles) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}
void TropoDBImpl::EnableManualCompaction() { TROPODB_ERROR("Not implemented\n"); }
void TropoDBImpl::DisableManualCompaction() {
  TROPODB_ERROR("Not implemented\n");
}

Status TropoDBImpl::Flush(const FlushOptions& options,
                        ColumnFamilyHandle* column_family) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}
Status TropoDBImpl::Flush(
    const FlushOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_families) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status TropoDBImpl::SyncWAL() {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status TropoDBImpl::DisableFileDeletions() {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status TropoDBImpl::IncreaseFullHistoryTsLow(ColumnFamilyHandle* column_family,
                                           std::string ts_low) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status TropoDBImpl::GetFullHistoryTsLow(ColumnFamilyHandle* column_family,
                                      std::string* ts_low) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status TropoDBImpl::EnableFileDeletions(bool force) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status TropoDBImpl::GetLiveFiles(std::vector<std::string>&,
                               uint64_t* manifest_file_size,
                               bool flush_memtable) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}
Status TropoDBImpl::GetSortedWalFiles(VectorLogPtr& files) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}
Status TropoDBImpl::GetCurrentWalFile(
    std::unique_ptr<LogFile>* current_log_file) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}
Status TropoDBImpl::GetCreationTimeOfOldestFile(uint64_t* creation_time) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status TropoDBImpl::GetUpdatesSince(
    SequenceNumber seq_number, std::unique_ptr<TransactionLogIterator>* iter,
    const TransactionLogIterator::ReadOptions& read_options) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status TropoDBImpl::DeleteFile(std::string name) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status TropoDBImpl::GetLiveFilesChecksumInfo(FileChecksumList* checksum_list) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status TropoDBImpl::GetLiveFilesStorageInfo(
    const LiveFilesStorageInfoOptions& opts,
    std::vector<LiveFileStorageInfo>* files) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status TropoDBImpl::IngestExternalFile(
    ColumnFamilyHandle* column_family,
    const std::vector<std::string>& external_files,
    const IngestExternalFileOptions& ingestion_options) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status TropoDBImpl::IngestExternalFiles(
    const std::vector<IngestExternalFileArg>& args) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status TropoDBImpl::CreateColumnFamilyWithImport(
    const ColumnFamilyOptions& options, const std::string& column_family_name,
    const ImportColumnFamilyOptions& import_options,
    const ExportImportFilesMetaData& metadata, ColumnFamilyHandle** handle) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status TropoDBImpl::VerifyChecksum(const ReadOptions& /*read_options*/) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status TropoDBImpl::GetDbIdentity(std::string& identity) const {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

Status TropoDBImpl::GetDbSessionId(std::string& session_id) const {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

ColumnFamilyHandle* TropoDBImpl::DefaultColumnFamily() const {
  TROPODB_ERROR("Not implemented\n");
  return NULL;
}

Status TropoDBImpl::GetPropertiesOfAllTables(ColumnFamilyHandle* column_family,
                                           TablePropertiesCollection* props) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}
Status TropoDBImpl::GetPropertiesOfTablesInRange(
    ColumnFamilyHandle* column_family, const Range* range, std::size_t n,
    TablePropertiesCollection* props) {
  TROPODB_ERROR("Not implemented\n");
  return Status::NotSupported();
}

}  // namespace ROCKSDB_NAMESPACE