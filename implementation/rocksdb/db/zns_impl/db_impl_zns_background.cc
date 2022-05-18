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
void DBImplZNS::MaybeScheduleCompaction(bool force) {
  // printf("Scheduling?\n");
  mutex_.AssertHeld();
  if (!bg_error_.ok()) {
    return;
  } else if (bg_compaction_scheduled_) {
    return;
  } else if (!force && imm_ == nullptr && !versions_->NeedsCompaction() &&
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
  // cascading.
  MaybeScheduleCompaction(false);
  bg_work_finished_signal_.SignalAll();
  // printf("bg done\n");
}

void DBImplZNS::BackgroundCompaction() {
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
    printf(" Trying to free WALS...\n");
    s = wal_man_->ResetOldWALs(&mutex_);
    return;
  }
  // Compaction itself does not require a lock. only once the changes become
  // visible.
  mutex_.Unlock();
  ZnsVersionEdit edit;
  {
    // printf("  Compact LN...\n");
    ZnsCompaction* c = versions_->PickCompaction();
    printf("Picked compact\n");
    c->MarkStaleTargetsReusable(&edit);
    printf("marked reusable\n");
    if (c->IsTrivialMove()) {
      printf("starting trivial move\n");
      s = c->DoTrivialMove(&edit);
      printf("\t\ttrivial move\n");
    } else {
      printf("starting compaction\n");
      s = c->DoCompaction(&edit);
      printf("\t\tnormal compaction\n");
    }
  }
  mutex_.Lock();
  if (!s.ok()) {
    printf("ERROR during compaction A!!!\n");
    return;
  }
  s = s.ok() ? versions_->RemoveObsoleteZones(&edit) : s;
  s = s.ok() ? versions_->LogAndApply(&edit) : s;
  s = s.ok() ? RemoveObsoleteZones() : s;
  versions_->RecalculateScore();
  if (!s.ok()) {
    printf("ERROR during compaction!!!\n");
  }
  printf("Compacted!!\n");
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
      s = s.ok() ? versions_->RemoveObsoleteZones(&edit) : s;
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
