//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef DB_IMPL_ZNS_H
#define DB_IMPL_ZNS_H

//#define WALPerfTest

#include <atomic>
#include <deque>
#include <functional>
#include <limits>
#include <list>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "db/zns_impl/config.h"
#include "db/zns_impl/index/zns_version.h"
#include "db/zns_impl/index/zns_version_set.h"
#include "db/zns_impl/io/szd_port.h"
#include "db/zns_impl/memtable/zns_memtable.h"
#include "db/zns_impl/persistence/zns_manifest.h"
#include "db/zns_impl/persistence/zns_wal.h"
#include "db/zns_impl/persistence/zns_wal_manager.h"
#include "db/zns_impl/table/zns_sstable_manager.h"
#include "db/zns_impl/table/zns_zonemetadata.h"
#include "options/cf_options.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/listener.h"
#include "rocksdb/metadata.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/transaction_log.h"

namespace ROCKSDB_NAMESPACE {

class Arena;
class ArenaWrappedDBIter;
class InMemoryStatsHistoryIterator;
class MemTable;
class PersistentStatsHistoryIterator;
class PeriodicWorkScheduler;
#ifndef NDEBUG
class PeriodicWorkTestScheduler;
#endif  // !NDEBUG
class TableCache;
class TaskLimiterToken;
class WriteCallback;
struct JobContext;
struct ExternalSstFileInfo;
struct MemTableInfo;
class ColumnFamilyMemTables;
class WriteBufferManager;
class ZnsTableCache;

class DBImplZNS : public DB {
 public:
  DBImplZNS(const DBOptions& options, const std::string& dbname,
            const bool seq_per_batch = false, const bool batch_per_txn = true,
            bool read_only = false);

  DBImplZNS(const DBImplZNS&) = delete;
  DBImplZNS& operator=(const DBImplZNS&) = delete;

  void IODiagnostics();
  ~DBImplZNS() override;

  static Status ValidateOptions(const DBOptions& db_options);

  static Status Open(const DBOptions& db_options, const std::string& name,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     std::vector<ColumnFamilyHandle*>* handles, DB** dbptr,
                     const bool seq_per_batch, const bool batch_per_txn);

  static Status DestroyDB(const std::string& dbname, const Options& options);

  Status OpenZNSDevice(const std::string dbname);
  Status ResetZNSDevice();

  Status InitDB(const DBOptions& options, const size_t max_write_buffer_size);
  Status InitWAL();

  virtual Status Close() override;

  // Implementations of the DB interface
  using DB::Put;
  Status Put(const WriteOptions& options, const Slice& key,
             const Slice& value) override;
  Status Put(const WriteOptions& options, ColumnFamilyHandle* column_family,
             const Slice& key, const Slice& value) override;
  Status Put(const WriteOptions& options, ColumnFamilyHandle* column_family,
             const Slice& key, const Slice& ts, const Slice& value) override;

  using DB::Delete;
  Status Delete(const WriteOptions&, const Slice& key) override;
  Status Delete(const WriteOptions& options, ColumnFamilyHandle* column_family,
                const Slice& key) override;
  Status Delete(const WriteOptions& options, ColumnFamilyHandle* column_family,
                const Slice& key, const Slice& ts) override;

  using DB::SingleDelete;
  Status SingleDelete(const WriteOptions& options,
                      ColumnFamilyHandle* column_family,
                      const Slice& key) override;
  Status SingleDelete(const WriteOptions& options,
                      ColumnFamilyHandle* column_family, const Slice& key,
                      const Slice& ts) override;
  using DB::Merge;
  Status Merge(const WriteOptions& options, ColumnFamilyHandle* column_family,
               const Slice& key, const Slice& value) override;

  using DB::Write;
  Status Write(const WriteOptions& options, WriteBatch* updates) override;

  using DB::Get;
  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value) override;
  Status Get(const ReadOptions& options, ColumnFamilyHandle* column_family,
             const Slice& key, PinnableSlice* value) override;
  Status Get(const ReadOptions& options, ColumnFamilyHandle* column_family,
             const Slice& key, PinnableSlice* value,
             std::string* timestamp) override;

  Status GetMergeOperands(const ReadOptions& options,
                          ColumnFamilyHandle* column_family, const Slice& key,
                          PinnableSlice* merge_operands,
                          GetMergeOperandsOptions* get_merge_operands_options,
                          int* number_of_operands) override;

  using DB::MultiGet;
  virtual std::vector<Status> MultiGet(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_family,
      const std::vector<Slice>& keys,
      std::vector<std::string>* values) override;
  virtual std::vector<Status> MultiGet(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_family,
      const std::vector<Slice>& keys, std::vector<std::string>* values,
      std::vector<std::string>* timestamps) override;

  using DB::NewIterator;
  virtual Iterator* NewIterator(const ReadOptions& options,
                                ColumnFamilyHandle* column_family) override;
  virtual Status NewIterators(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_families,
      std::vector<Iterator*>* iterators) override;

  using DB::GetProperty;
  virtual bool GetProperty(ColumnFamilyHandle* column_family,
                           const Slice& property, std::string* value) override;

  using DB::GetMapProperty;
  virtual bool GetMapProperty(
      ColumnFamilyHandle* column_family, const Slice& property,
      std::map<std::string, std::string>* value) override;

  using DB::GetIntProperty;
  virtual bool GetIntProperty(ColumnFamilyHandle* column_family,
                              const Slice& property, uint64_t* value) override;

  using DB::GetAggregatedIntProperty;
  virtual bool GetAggregatedIntProperty(const Slice& property,
                                        uint64_t* aggregated_value) override;

  using DB::GetApproximateSizes;
  virtual Status GetApproximateSizes(const SizeApproximationOptions& options,
                                     ColumnFamilyHandle* column_family,
                                     const Range* range, int n,
                                     uint64_t* sizes) override;

  using DB::GetApproximateMemTableStats;
  virtual void GetApproximateMemTableStats(ColumnFamilyHandle* column_family,
                                           const Range& range,
                                           uint64_t* const count,
                                           uint64_t* const size) override;
  using DB::CompactRange;
  virtual Status CompactRange(const CompactRangeOptions& options,
                              ColumnFamilyHandle* column_family,
                              const Slice* begin, const Slice* end) override;
  virtual Status SetDBOptions(
      const std::unordered_map<std::string, std::string>& options_map) override;

  using DB::CompactFiles;
  virtual Status CompactFiles(
      const CompactionOptions& compact_options,
      ColumnFamilyHandle* column_family,
      const std::vector<std::string>& input_file_names, const int output_level,
      const int output_path_id = -1,
      std::vector<std::string>* const output_file_names = nullptr,
      CompactionJobInfo* compaction_job_info = nullptr) override;

  Status MakeRoomForWrite(size_t size);
  void MaybeScheduleFlush();
  void MaybeScheduleCompaction(bool force);
  static void BGFlushWork(void* db);
  static void BGCompactionWork(void* db);
  void BackgroundFlushCall();
  void BackgroundFlush();
  void BackgroundCompactionCall();
  void BackgroundCompaction();
  Status CompactMemtable();
  Status FlushL0SSTables(SSZoneMetaData* meta);

  virtual Status PauseBackgroundWork() override;
  virtual Status ContinueBackgroundWork() override;
  virtual Status EnableAutoCompaction(
      const std::vector<ColumnFamilyHandle*>& column_family_handles) override;
  virtual void EnableManualCompaction() override;
  virtual void DisableManualCompaction() override;

  using DB::NumberLevels;
  virtual int NumberLevels(ColumnFamilyHandle* column_family) override;

  using DB::MaxMemCompactionLevel;
  virtual int MaxMemCompactionLevel(ColumnFamilyHandle* column_family) override;

  using DB::Level0StopWriteTrigger;
  virtual int Level0StopWriteTrigger(
      ColumnFamilyHandle* column_family) override;
  virtual const std::string& GetName() const override;
  virtual Env* GetEnv() const override;

  using DB::GetOptions;
  virtual Options GetOptions(ColumnFamilyHandle* column_family) const override;

  using DB::GetDBOptions;
  virtual DBOptions GetDBOptions() const override;

  using DB::Flush;
  virtual Status Flush(const FlushOptions& options,
                       ColumnFamilyHandle* column_family) override;
  virtual Status Flush(
      const FlushOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_families) override;

  virtual Status SyncWAL() override;

  virtual SequenceNumber GetLatestSequenceNumber() const override;

  virtual Status DisableFileDeletions() override;

  Status IncreaseFullHistoryTsLow(ColumnFamilyHandle* column_family,
                                  std::string ts_low) override;

  Status GetFullHistoryTsLow(ColumnFamilyHandle* column_family,
                             std::string* ts_low) override;

  virtual Status EnableFileDeletions(bool force) override;

  virtual Status GetLiveFiles(std::vector<std::string>&,
                              uint64_t* manifest_file_size,
                              bool flush_memtable = true) override;
  virtual Status GetSortedWalFiles(VectorLogPtr& files) override;
  virtual Status GetCurrentWalFile(
      std::unique_ptr<LogFile>* current_log_file) override;
  virtual Status GetCreationTimeOfOldestFile(uint64_t* creation_time) override;

  virtual Status GetUpdatesSince(
      SequenceNumber seq_number, std::unique_ptr<TransactionLogIterator>* iter,
      const TransactionLogIterator::ReadOptions& read_options =
          TransactionLogIterator::ReadOptions()) override;

  virtual Status DeleteFile(std::string name) override;

  virtual Status GetLiveFilesChecksumInfo(
      FileChecksumList* checksum_list) override;

  virtual Status GetLiveFilesStorageInfo(
      const LiveFilesStorageInfoOptions& opts,
      std::vector<LiveFileStorageInfo>* files) override;

  using DB::IngestExternalFile;
  virtual Status IngestExternalFile(
      ColumnFamilyHandle* column_family,
      const std::vector<std::string>& external_files,
      const IngestExternalFileOptions& ingestion_options) override;

  using DB::IngestExternalFiles;
  virtual Status IngestExternalFiles(
      const std::vector<IngestExternalFileArg>& args) override;

  using DB::CreateColumnFamilyWithImport;
  virtual Status CreateColumnFamilyWithImport(
      const ColumnFamilyOptions& options, const std::string& column_family_name,
      const ImportColumnFamilyOptions& import_options,
      const ExportImportFilesMetaData& metadata,
      ColumnFamilyHandle** handle) override;

  using DB::VerifyChecksum;
  virtual Status VerifyChecksum(const ReadOptions& /*read_options*/) override;

  virtual Status GetDbIdentity(std::string& identity) const override;

  virtual Status GetDbSessionId(std::string& session_id) const override;

  ColumnFamilyHandle* DefaultColumnFamily() const override;

  using DB::GetPropertiesOfAllTables;
  virtual Status GetPropertiesOfAllTables(
      ColumnFamilyHandle* column_family,
      TablePropertiesCollection* props) override;
  virtual Status GetPropertiesOfTablesInRange(
      ColumnFamilyHandle* column_family, const Range* range, std::size_t n,
      TablePropertiesCollection* props) override;

  const Snapshot* GetSnapshot() override;
  void ReleaseSnapshot(const Snapshot* snapshot) override;

 private:
  struct Writer;
  Status Recover();
  Status RemoveObsoleteZones();
  WriteBatch* BuildBatchGroup(Writer** last_writer);

  // Should remain constant after construction
  const DBOptions options_;
  const std::string name_;
  const InternalKeyComparator internal_comparator_;
  Env* const env_;

  // Should be "constant" after SPDK is initialised.
  SZD::SZDDevice* zns_device_;
  SZD::SZDChannelFactory* channel_factory_;
  ZNSSSTableManager* ss_manager_;
  ZnsManifest* manifest_;
  ZnsTableCache* table_cache_;
  ZnsWALManager<ZnsConfig::wal_count>* wal_man_;
  ZnsVersionSet* versions_;
  size_t max_write_buffer_size_;

  // Dynamic data objects, protected by mutex
  ZNSWAL* wal_;
  ZNSMemTable* mem_;
  ZNSMemTable* imm_;
  std::deque<Writer*> writers_;
  WriteBatch* tmp_batch_;

  // Threading variables
  port::Mutex mutex_;
  port::CondVar bg_work_finished_signal_;
  port::CondVar bg_flush_work_finished_signal_;
  bool bg_compaction_scheduled_;
  bool bg_flush_scheduled_;
  bool shutdown_;
  Status bg_error_;
  bool forced_schedule_;

  // diagnostics
  uint64_t flushes_;
  std::array<uint64_t, ZnsConfig::level_count - 1> compactions_;
};
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
