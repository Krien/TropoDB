#include "db/db_impl/db_impl.h"
#ifdef TROPODB_PLUGIN_ENABLED
#include "db/zns_impl/tropodb_impl.h"
#endif
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

Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  if (db_options.persist_stats_to_disk) {
    column_families.push_back(
        ColumnFamilyDescriptor(kPersistentStatsColumnFamilyName, cf_options));
  }
  std::vector<ColumnFamilyHandle*> handles;
  Status s = DB::Open(db_options, dbname, column_families, &handles, dbptr);
#ifdef TROPODB_PLUGIN_ENABLED
  if (db_options.use_zns_impl) {
    return s;
  }
#endif
  if (s.ok()) {
    if (db_options.persist_stats_to_disk) {
      assert(handles.size() == 2);
    } else {
      assert(handles.size() == 1);
    }
    // i can delete the handle since DBImpl is always holding a reference to
    // default column family
    if (db_options.persist_stats_to_disk && handles[1] != nullptr) {
      delete handles[1];
    }
    delete handles[0];
  }
  return s;
}

Status DB::Open(const DBOptions& db_options, const std::string& dbname,
                const std::vector<ColumnFamilyDescriptor>& column_families,
                std::vector<ColumnFamilyHandle*>* handles, DB** dbptr) {
  const bool kSeqPerBatch = true;
  const bool kBatchPerTxn = true;
  // if this is the way to fix the antipattern than so be it.
#ifdef TROPODB_PLUGIN_ENABLED
  if (db_options.use_zns_impl) {
    return DBImplZNS::Open(db_options, dbname, column_families, handles, dbptr,
                           !kSeqPerBatch, kBatchPerTxn);
  } else {
#endif
    return DBImpl::Open(db_options, dbname, column_families, handles, dbptr,
                        !kSeqPerBatch, kBatchPerTxn);
  }
}

FileSystem* DB::GetFileSystem() const {
  const auto& fs = GetEnv()->GetFileSystem();
  return fs.get();
}

Status DestroyDB(const std::string& dbname, const Options& options,
                 const std::vector<ColumnFamilyDescriptor>& column_families) {
#ifdef TROPODB_PLUGIN_ENABLED
  if (options.use_zns_impl) {
    return DBImplZNS::DestroyDB(dbname, options);
  }
#endif
  ImmutableDBOptions soptions(SanitizeOptions(dbname, options));
  Env* env = soptions.env;
  std::vector<std::string> filenames;
  bool wal_in_db_path = soptions.IsWalDirSameAsDBPath();

  // Reset the logger because it holds a handle to the
  // log file and prevents cleanup and directory removal
  soptions.info_log.reset();
  // Ignore error in case directory does not exist
  env->GetChildren(dbname, &filenames).PermitUncheckedError();

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  Status result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    InfoLogPrefix info_log_prefix(!soptions.db_log_dir.empty(), dbname);
    for (const auto& fname : filenames) {
      if (ParseFileName(fname, &number, info_log_prefix.prefix, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del;
        std::string path_to_delete = dbname + "/" + fname;
        if (type == kMetaDatabase) {
          del = DestroyDB(path_to_delete, options);
        } else if (type == kTableFile || type == kWalFile ||
                   type == kBlobFile) {
          del = DeleteDBFile(
              &soptions, path_to_delete, dbname,
              /*force_bg=*/false,
              /*force_fg=*/(type == kWalFile) ? !wal_in_db_path : false);
        } else {
          del = env->DeleteFile(path_to_delete);
        }
        if (!del.ok() && result.ok()) {
          result = del;
        }
      }
    }

    std::set<std::string> paths;
    for (const DbPath& db_path : options.db_paths) {
      paths.insert(db_path.path);
    }
    for (const ColumnFamilyDescriptor& cf : column_families) {
      for (const DbPath& cf_path : cf.options.cf_paths) {
        paths.insert(cf_path.path);
      }
    }
    for (const auto& path : paths) {
      if (env->GetChildren(path, &filenames).ok()) {
        for (const auto& fname : filenames) {
          if (ParseFileName(fname, &number, &type) &&
              (type == kTableFile ||
               type == kBlobFile)) {  // Lock file will be deleted at end
            std::string file_path = path + "/" + fname;
            Status del = DeleteDBFile(&soptions, file_path, dbname,
                                      /*force_bg=*/false, /*force_fg=*/false);
            if (!del.ok() && result.ok()) {
              result = del;
            }
          }
        }
        // TODO: Should we return an error if we cannot delete the directory?
        env->DeleteDir(path).PermitUncheckedError();
      }
    }

    std::vector<std::string> walDirFiles;
    std::string archivedir = ArchivalDirectory(dbname);
    bool wal_dir_exists = false;
    if (!soptions.IsWalDirSameAsDBPath(dbname)) {
      wal_dir_exists = env->GetChildren(soptions.wal_dir, &walDirFiles).ok();
      archivedir = ArchivalDirectory(soptions.wal_dir);
    }

    // Archive dir may be inside wal dir or dbname and should be
    // processed and removed before those otherwise we have issues
    // removing them
    std::vector<std::string> archiveFiles;
    if (env->GetChildren(archivedir, &archiveFiles).ok()) {
      // Delete archival files.
      for (const auto& file : archiveFiles) {
        if (ParseFileName(file, &number, &type) && type == kWalFile) {
          Status del =
              DeleteDBFile(&soptions, archivedir + "/" + file, archivedir,
                           /*force_bg=*/false, /*force_fg=*/!wal_in_db_path);
          if (!del.ok() && result.ok()) {
            result = del;
          }
        }
      }
      // Ignore error in case dir contains other files
      env->DeleteDir(archivedir).PermitUncheckedError();
    }

    // Delete log files in the WAL dir
    if (wal_dir_exists) {
      for (const auto& file : walDirFiles) {
        if (ParseFileName(file, &number, &type) && type == kWalFile) {
          Status del =
              DeleteDBFile(&soptions, LogFileName(soptions.wal_dir, number),
                           soptions.wal_dir, /*force_bg=*/false,
                           /*force_fg=*/!wal_in_db_path);
          if (!del.ok() && result.ok()) {
            result = del;
          }
        }
      }
      // Ignore error in case dir contains other files
      env->DeleteDir(soptions.wal_dir).PermitUncheckedError();
    }

    // Ignore error since state is already gone
    env->UnlockFile(lock).PermitUncheckedError();
    env->DeleteFile(lockname).PermitUncheckedError();

    // sst_file_manager holds a ref to the logger. Make sure the logger is
    // gone before trying to remove the directory.
    soptions.sst_file_manager.reset();

    // Ignore error in case dir contains other files
    env->DeleteDir(dbname).PermitUncheckedError();
    ;
  }
  return result;
}

Status DestroyDB(const std::string& dbname, const Options& options) {
  const std::vector<ColumnFamilyDescriptor> column_families;
#ifdef TROPODB_PLUGIN_ENABLED
  if (options.use_zns_impl) {
    return DBImplZNS::DestroyDB(dbname, options);
  }
#endif
  return DestroyDB(dbname, options, column_families);
}

}  // namespace ROCKSDB_NAMESPACE