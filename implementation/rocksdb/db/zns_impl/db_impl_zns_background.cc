// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

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
#include "db/zns_impl/db_impl_zns.h"
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

void DBImplZNS::MaybeScheduleFlush() {
  // printf("Scheduling flush?\n");
  mutex_.AssertHeld();
  if (bg_flush_scheduled_) {
    return;
  } else if (imm_ == nullptr && wal_man_->WALAvailable()) {
    return;
  }
  bg_flush_scheduled_ = true;
  // printf("Scheduled flush\n");
  env_->Schedule(&DBImplZNS::BGFlushWork, this, rocksdb::Env::HIGH);
}

void DBImplZNS::BGFlushWork(void* db) {
  reinterpret_cast<DBImplZNS*>(db)->BackgroundFlushCall();
}

void DBImplZNS::BackgroundFlushCall() {
  // printf("bg\n");
  MutexLock l(&mutex_);
  assert(bg_flush_scheduled_);
  if (!bg_error_.ok()) {
  } else {
    // printf("starting background work\n");
    BackgroundFlush();
  }
  bg_flush_scheduled_ = false;
  forced_schedule_ = false;
  // cascading, but shutdown if ordered
  if (!shutdown_) {
    MaybeScheduleFlush();
    MaybeScheduleCompaction(false);
  }
  bg_flush_work_finished_signal_.SignalAll();
  // printf("bg done\n");
}

void DBImplZNS::BackgroundFlush() {
  mutex_.AssertHeld();
  Status s;

  if (imm_ != nullptr) {
    // printf("  Compact memtable...\n");
    s = CompactMemtable();
    if (!s.ok()) {
      printf("error during flushing\n");
    }
    return;
  }
  if (!wal_man_->WALAvailable()) {
    // printf(" Trying to free WALS...\n");
    s = wal_man_->ResetOldWALs(&mutex_);
    return;
  }
}

Status DBImplZNS::CompactMemtable() {
  mutex_.AssertHeld();
  // We can not do a flush...
  if (imm_->GetInternalSize() * 1.2 > ss_manager_->SpaceRemainingInBytes(0)) {
    MaybeScheduleCompaction(true);
    // printf("WAITING, can not flush %f %f \n",
    //        (float)imm_->GetInternalSize() / 1024. / 1024.,
    //        (float)ss_manager_->SpaceRemaining(0) / 1024. / 1024.);
    bg_work_finished_signal_.Wait();
  }
  assert(imm_ != nullptr);
  assert(bg_flush_scheduled_);
  Status s;
  // Flush and set new version
  {
    ZnsVersionEdit edit;
    SSZoneMetaData meta;

    ZnsVersion* current = versions_->current();
    // current->Ref();

    meta.number = versions_->NewSSNumber();
    mutex_.Unlock();
    s = FlushL0SSTables(&meta);
    mutex_.Lock();
    // current->Unref();
    int level = 0;
    if (s.ok() && meta.lba_count > 0) {
      edit.AddSSDefinition(level, meta);
      s = versions_->LogAndApply(&edit);
    } else {
      printf("Fatal error \n");
    }
    imm_->Unref();
    imm_ = nullptr;
    // wal
    s = wal_man_->ResetOldWALs(&mutex_);
    if (!s.ok()) return s;
    // printf("Flushed!!\n");
  }
  return s;
}

Status DBImplZNS::FlushL0SSTables(SSZoneMetaData* meta) {
  Status s;
  s = ss_manager_->FlushMemTable(imm_, meta);
  flushes_++;
  return s;
}

void DBImplZNS::MaybeScheduleCompaction(bool force) {
  // printf("Scheduling compaction?\n");
  mutex_.AssertHeld();
  if (!bg_error_.ok()) {
    return;
  } else if (bg_compaction_scheduled_) {
    return;
  } else if (!force && !versions_->NeedsCompaction()) {
    return;
  }
  forced_schedule_ = force;
  bg_compaction_scheduled_ = true;
  // printf("Scheduled compaction\n");
  env_->Schedule(&DBImplZNS::BGCompactionWork, this, rocksdb::Env::LOW);
}

void DBImplZNS::BGCompactionWork(void* db) {
  reinterpret_cast<DBImplZNS*>(db)->BackgroundCompactionCall();
}

void DBImplZNS::BackgroundCompactionCall() {
  // printf("bg\n");
  MutexLock l(&mutex_);
  assert(bg_compaction_scheduled_);
  if (!bg_error_.ok()) {
  } else {
    // printf("starting background work\n");
    BackgroundCompaction();
  }
  bg_compaction_scheduled_ = false;
  forced_schedule_ = false;
  // cascading, but shutdown if ordered
  if (!shutdown_) {
    MaybeScheduleCompaction(false);
  }
  bg_work_finished_signal_.SignalAll();
  // printf("bg done\n");
}

void DBImplZNS::BackgroundCompaction() {
  mutex_.AssertHeld();
  Status s;

  ZnsVersion* current = versions_->current();
  // current->Ref();

  // Compaction itself does not require a lock. only once the changes become
  // visible.
  if (current->CompactionLevel() >= ZnsConfig::level_count) {
    mutex_.Unlock();
    return;
  }
  ZnsVersionEdit edit;
  // This happens when a lot of readers interfere
  if (versions_->OnlyNeedDeletes()) {
    // TODO: we should probably wait a while instead of spamming reset requests.
    // Then probably all clients are done with their reads on old versions.
    s = RemoveObsoleteZones();
    versions_->RecalculateScore();
    if (!s.ok()) {
      printf("ERROR during reclaiming!!!\n");
    }
    // printf("Only reclaimed\n");
    return;
  } else {
    ZnsCompaction* c = versions_->PickCompaction();
    current->Ref();
    mutex_.Unlock();
    // printf("  Compact LN...\n");
    // printf("Picked compact\n");
    c->MarkStaleTargetsReusable(&edit);
    // printf("marked reusable\n");
    if (c->IsTrivialMove()) {
      // printf("starting trivial move\n");
      s = c->DoTrivialMove(&edit);
      // printf("\t\ttrivial move\n");
    } else {
      // printf("starting compaction\n");
      s = c->DoCompaction(&edit);
      // printf("\t\tnormal compaction\n");
    }
    current->Unref();
    // Note if this delete is not reached, a stale version will remain in memory
    // for the rest of this session.
    delete c;
    mutex_.Lock();
  }
  if (!s.ok()) {
    printf("ERROR during compaction A!!!\n");
    return;
  }
  // Diag
  compactions_[current->CompactionLevel()]++;
  // current->Unref();
  // printf("Removing cache \n");
  s = s.ok() ? versions_->LogAndApply(&edit) : s;
  // printf("Applied change \n");
  mutex_.Unlock();
  mutex_.Lock();
  s = s.ok() ? RemoveObsoleteZones() : s;
  // printf("Removed obsolete zones \n");
  mutex_.Unlock();
  mutex_.Lock();
  versions_->RecalculateScore();
  if (!s.ok()) {
    printf("ERROR during compaction!!!\n");
  }
  // printf("Compacted!!\n");
}

Status DBImplZNS::RemoveObsoleteZones() {
  mutex_.AssertHeld();
  Status s = Status::OK();
  s = versions_->ReclaimStaleSSTables();
  if (!s.ok()) {
    printf("error reclaiming \n");
    return s;
  }
  return s;
}

Status DBImplZNS::Merge(const WriteOptions& options,
                        ColumnFamilyHandle* column_family, const Slice& key,
                        const Slice& value) {
  return Status::OK();
}

int DBImplZNS::MaxMemCompactionLevel(ColumnFamilyHandle* column_family) {
  return 0;
}

int DBImplZNS::Level0StopWriteTrigger(ColumnFamilyHandle* column_family) {
  return 0;
}

}  // namespace ROCKSDB_NAMESPACE
