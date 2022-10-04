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

void DBImplZNS::MaybeScheduleFlush(uint8_t parallel_number) {
  // printf("Scheduling flush?\n");
  mutex_.AssertHeld();
  if (bg_flush_scheduled_[parallel_number]) {
    return;
  } else if ((imm_[parallel_number] == nullptr &&
              wal_man_[parallel_number]->WALAvailable())) {
    return;
  }
  bg_flush_scheduled_[parallel_number] = true;
  // printf("Scheduled flush\n");
  FlushData* dat = new FlushData;
  dat->db = this;
  dat->parallel_number = parallel_number;
  env_->Schedule(&DBImplZNS::BGFlushWork, dat, rocksdb::Env::HIGH);
}

void DBImplZNS::BGFlushWork(void* data) {
  FlushData* flush_data = reinterpret_cast<FlushData*>(data);
  DBImplZNS* db = flush_data->db;
  uint8_t parallel_number = flush_data->parallel_number;
  db->BackgroundFlushCall(parallel_number);
  delete flush_data;
}

void DBImplZNS::BackgroundFlushCall(uint8_t parallel_number) {
  // printf("bg\n");
  MutexLock l(&mutex_);
  assert(bg_flush_scheduled_);
#ifdef WALPerfTest
  wal_man_[parallel_number]->ResetOldWALs(&mutex_);
  bg_flush_work_finished_signal_.SignalAll();
  return;
#endif
  if (!bg_error_.ok()) {
  } else {
    // printf("starting background work\n");
    uint64_t before = clock_->NowMicros();
    BackgroundFlush(parallel_number);
    flush_total_counter_.AddTiming(clock_->NowMicros() - before);
  }
  bg_flush_scheduled_[parallel_number] = false;
  forced_schedule_ = false;
  // cascading, but shutdown if ordered
  if (!shutdown_) {
    MaybeScheduleFlush(parallel_number);
    MaybeScheduleCompactionL0();
    MaybeScheduleCompaction(false);
  }
  bg_flush_work_finished_signal_.SignalAll();
  // printf("bg done\n");
}

void DBImplZNS::BackgroundFlush(uint8_t parallel_number) {
  mutex_.AssertHeld();
  Status s;

  if (imm_[parallel_number] != nullptr) {
    printf("  Compact memtable...\n");
    s = CompactMemtable(parallel_number);
    if (!s.ok()) {
      printf("error during flushing\n");
    }
    return;
  }
  if (!wal_man_[parallel_number]->WALAvailable()) {
    // printf(" Trying to free WALS...\n");
    uint64_t before = clock_->NowMicros();
    s = wal_man_[parallel_number]->ResetOldWALs(&mutex_);
    flush_reset_wal_counter_.AddTiming(clock_->NowMicros() - before);
    return;
  }
}

Status DBImplZNS::CompactMemtable(uint8_t parallel_number) {
  mutex_.AssertHeld();
  // We can not do a flush...
  while (imm_[parallel_number]->GetInternalSize() * 1.2 >
         ss_manager_->SpaceRemainingInBytesL0(parallel_number)) {
    MaybeScheduleCompactionL0();
    printf(
        "WAITING, can not flush %f %f \n",
        (float)imm_[parallel_number]->GetInternalSize() / 1024. / 1024.,
        (float)ss_manager_->SpaceRemainingL0(parallel_number) / 1024. / 1024.);
    bg_work_l0_finished_signal_.Wait();
  }
  assert(imm_[parallel_number] != nullptr);
  assert(bg_flush_scheduled_[parallel_number]);
  Status s;
  // Flush and set new version
  {
    ZnsVersionEdit edit;
    std::vector<SSZoneMetaData> metas;
    // ZnsVersion* current = versions_->current();
    // current->Ref();
    uint64_t before = clock_->NowMicros();
    mutex_.Unlock();
    s = FlushL0SSTables(metas, parallel_number);
    mutex_.Lock();
    flush_flush_memtable_counter_.AddTiming(clock_->NowMicros() - before);
    // current->Unref();
    before = clock_->NowMicros();
    uint64_t l0_number = versions_->NewSSNumberL0();
    int level = 0;
    if (s.ok() && metas.size() > 0) {
      for (auto& meta : metas) {
        if (meta.lba_count > 0) {
          meta.number = versions_->NewSSNumber();
          meta.L0.number = l0_number;
          edit.AddSSDefinition(level, meta);
        }
      }
      s = versions_->LogAndApply(&edit);
    } else {
      printf("Fatal error \n");
    }
    imm_[parallel_number]->Unref();
    imm_[parallel_number] = nullptr;
    flush_update_version_counter_.AddTiming(clock_->NowMicros() - before);
    // wal
    before = clock_->NowMicros();
    s = wal_man_[parallel_number]->ResetOldWALs(&mutex_);
    flush_reset_wal_counter_.AddTiming(clock_->NowMicros() - before);
    if (!s.ok()) return s;
    printf("Flushed memtable!!\n");
  }
  return s;
}

Status DBImplZNS::FlushL0SSTables(std::vector<SSZoneMetaData>& metas,
                                  uint8_t parallel_number) {
  Status s;
  s = ss_manager_->FlushMemTable(imm_[parallel_number], metas, parallel_number);
  return s;
}

void DBImplZNS::MaybeScheduleCompactionL0() {
  // printf("Scheduling L0 compaction?\n");
  mutex_.AssertHeld();
  if (!bg_error_.ok()) {
    return;
  } else if (bg_compaction_l0_scheduled_) {
    return;
  } else if (!versions_->NeedsL0Compaction()) {
    return;
  }
  bg_compaction_l0_scheduled_ = true;
  // printf("Scheduled compaction\n");
  env_->Schedule(&DBImplZNS::BGCompactionL0Work, this, rocksdb::Env::HIGH);
}

void DBImplZNS::BGCompactionL0Work(void* db) {
  reinterpret_cast<DBImplZNS*>(db)->BackgroundCompactionL0Call();
}

void DBImplZNS::BackgroundCompactionL0Call() {
  // printf("bg\n");
  MutexLock l(&mutex_);
  assert(bg_compaction_l0_scheduled_);
  if (!bg_error_.ok()) {
  } else {
    // printf("starting background work\n");
    uint64_t before = clock_->NowMicros();
    BackgroundCompactionL0();
    compaction_compaction_L0_total_.AddTiming(clock_->NowMicros() - before);
  }
  bg_compaction_l0_scheduled_ = false;
  // cascading, but shutdown if ordered
  if (!shutdown_) {
    MaybeScheduleCompactionL0();
    MaybeScheduleCompaction(false);
  }
  bg_work_l0_finished_signal_.SignalAll();
  bg_work_finished_signal_.SignalAll();
  bg_flush_work_finished_signal_.SignalAll();
  // printf("bg done\n");
}

void DBImplZNS::BackgroundCompactionL0() {
  mutex_.AssertHeld();
  Status s;

  ZnsVersion* current = versions_->current();

  // Compaction itself does not require a lock. only once the changes become
  // visible.
  if (!versions_->NeedsL0Compaction()) {
    mutex_.Unlock();
    return;
  }
  ZnsVersionEdit edit;
  uint64_t before;
  // This happens when a lot of readers interfere
  if (versions_->OnlyNeedDeletes(0)) {
    current->Ref();
    // TODO: we should probably wait a while instead of spamming reset requests.
    // Then probably all clients are done with their reads on old versions.
    before = clock_->NowMicros();
    s = RemoveObsoleteZonesL0();
    compaction_reset_L0_counter_.AddTiming(clock_->NowMicros() - before);
    if (!s.ok()) {
      printf("ERROR during reclaiming!!!\n");
    }
    // printf("Only reclaimed\n");
    current->Unref();
    return;
  } else {
    before = clock_->NowMicros();
    ZnsCompaction* c = versions_->PickCompaction(0, reserved_comp_[1]);
    // Can not do this compaction
    while (reserve_claimed_ == 1 ||
           c->HasOverlapWithOtherCompaction(reserved_comp_[1])) {
      printf("\tOverlap with LN write %u %u\n", reserve_claimed_ == 1,
             c->HasOverlapWithOtherCompaction(reserved_comp_[1]));
      delete c;
      reserve_claimed_ = reserve_claimed_ == -1 ? 0 : reserve_claimed_;
      bg_work_finished_signal_.Wait();
      current = versions_->current();
      c = versions_->PickCompaction(0, reserved_comp_[1]);
    }
    reserve_claimed_ = reserve_claimed_ == 0 ? -1 : reserve_claimed_;
    c->GetCompactionTargets(&reserved_comp_[0]);
    compaction_pick_compaction_.AddTiming(clock_->NowMicros() - before);
    current->Ref();
    before = clock_->NowMicros();
    bool trivial = c->IsTrivialMove();
    mutex_.Unlock();
    printf("  Compact L0...\n");
    // printf("Picked compact\n");
    c->MarkStaleTargetsReusable(&edit);
    // printf("marked reusable\n");
    if (trivial) {
      printf("starting trivial move L0\n");
      s = c->DoTrivialMove(&edit);
      printf("\t\ttrivial move L0\n");
    } else {
      printf("starting compaction L0\n");
      s = c->DoCompaction(&edit);
      printf("\t\tnormal compaction L0\n");
    }
    // Note if this delete is not reached, a stale version will remain in memory
    // for the rest of this session.
    delete c;
    mutex_.Lock();
    if (trivial) {
      compaction_compaction_trivial_.AddTiming(clock_->NowMicros() - before);
    } else {
      compaction_compaction_.AddTiming(clock_->NowMicros() - before);
    }
    current->Unref();
  }
  if (!s.ok()) {
    printf("ERROR during compaction A!!!\n");
    return;
  }
  // Diag
  compactions_[0]++;
  // current->Unref();
  // printf("Removing cache \n");
  before = clock_->NowMicros();
  s = s.ok() ? versions_->LogAndApply(&edit) : s;
  compaction_version_edit_.AddTiming(clock_->NowMicros() - before);
  reserved_comp_[0].clear();
  // printf("Applied change \n");
  mutex_.Unlock();
  mutex_.Lock();
  before = clock_->NowMicros();
  s = s.ok() ? RemoveObsoleteZonesL0() : s;
  compaction_reset_L0_counter_.AddTiming(clock_->NowMicros() - before);
  // printf("Removed obsolete zones \n");
  mutex_.Unlock();
  mutex_.Lock();
  if (!s.ok()) {
    printf("ERROR during compaction!!!\n");
  }
  printf("Compacted L0!!\n");
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
    uint64_t before = clock_->NowMicros();
    BackgroundCompaction();
    compaction_compaction_LN_total_.AddTiming(clock_->NowMicros() - before);
  }
  bg_compaction_scheduled_ = false;
  forced_schedule_ = false;
  // cascading, but shutdown if ordered
  if (!shutdown_) {
    MaybeScheduleCompaction(false);
  }
  bg_work_finished_signal_.SignalAll();
  bg_flush_work_finished_signal_.SignalAll();
  bg_work_l0_finished_signal_.SignalAll();
  // printf("bg done\n");
}

void DBImplZNS::BackgroundCompaction() {
  mutex_.AssertHeld();
  Status s;

  ZnsVersion* current = versions_->current();

  // Compaction itself does not require a lock. only once the changes become
  // visible.
  if (current->CompactionLevel() >= ZnsConfig::level_count) {
    mutex_.Unlock();
    return;
  }
  ZnsVersionEdit edit;
  uint64_t before;
  // This happens when a lot of readers interfere
  if (versions_->OnlyNeedDeletes(current->CompactionLevel())) {
    // TODO: we should probably wait a while instead of spamming reset requests.
    // Then probably all clients are done with their reads on old versions.
    before = clock_->NowMicros();
    s = RemoveObsoleteZonesLN();
    compaction_reset_LN_counter_.AddTiming(clock_->NowMicros() - before);
    versions_->RecalculateScore();
    if (!s.ok()) {
      printf("ERROR during reclaiming!!!\n");
    }
    // printf("Only reclaimed\n");
    return;
  } else {
    before = clock_->NowMicros();
    ZnsCompaction* c = versions_->PickCompaction(current->CompactionLevel(),
                                                 reserved_comp_[0]);
    while (reserve_claimed_ == 0 ||
           c->HasOverlapWithOtherCompaction(reserved_comp_[0]) || c->IsBusy()) {
      printf("\tOverlap with L0 write... %u %u %u\n", reserve_claimed_ == 0,
             c->HasOverlapWithOtherCompaction(reserved_comp_[0]), c->IsBusy());
      delete c;
      reserve_claimed_ = reserve_claimed_ == -1 ? 1 : reserve_claimed_;
      bg_work_l0_finished_signal_.Wait();
      current = versions_->current();
      c = versions_->PickCompaction(current->CompactionLevel(),
                                    reserved_comp_[0]);
    }
    reserve_claimed_ = reserve_claimed_ == 1 ? -1 : reserve_claimed_;
    c->GetCompactionTargets(&reserved_comp_[1]);
    compaction_pick_compaction_LN_.AddTiming(clock_->NowMicros() - before);
    current->Ref();
    bool istrivial = c->IsTrivialMove();
    before = clock_->NowMicros();
    mutex_.Unlock();
    printf("  Compact LN...\n");
    // printf("Picked compact\n");
    c->MarkStaleTargetsReusable(&edit);
    if (istrivial) {
      // printf("starting trivial move\n");
      s = c->DoTrivialMove(&edit);
      // printf("\t\ttrivial move\n");
    } else {
      printf("starting compaction\n");
      s = c->DoCompaction(&edit);
      printf("\t\tnormal compaction\n");
    }
    // Note if this delete is not reached, a stale version will remain in memory
    // for the rest of this session.
    delete c;
    mutex_.Lock();
    if (istrivial) {
      compaction_compaction_trivial_LN_.AddTiming(clock_->NowMicros() - before);
    } else {
      compaction_compaction_LN_.AddTiming(clock_->NowMicros() - before);
    }
    current->Unref();
  }
  if (!s.ok()) {
    printf("ERROR during compaction A!!!\n");
    return;
  }
  // Diag
  compactions_[current->CompactionLevel()]++;
  // current->Unref();
  // printf("Removing cache \n");
  before = clock_->NowMicros();
  s = s.ok() ? versions_->LogAndApply(&edit) : s;
  compaction_version_edit_LN_.AddTiming(clock_->NowMicros() - before);
  reserved_comp_[1].clear();
  // printf("Applied change \n");
  mutex_.Unlock();
  mutex_.Lock();
  before = clock_->NowMicros();
  s = s.ok() ? RemoveObsoleteZonesLN() : s;
  compaction_reset_LN_counter_.AddTiming(clock_->NowMicros() - before);
  // printf("Removed obsolete zones \n");
  mutex_.Unlock();
  mutex_.Lock();
  versions_->RecalculateScore();
  if (!s.ok()) {
    printf("ERROR during compaction!!!\n");
  }
  printf("Compacted!!\n");
}

Status DBImplZNS::RemoveObsoleteZonesL0() {
  mutex_.AssertHeld();
  Status s = Status::OK();
  s = versions_->ReclaimStaleSSTablesL0(&mutex_, &bg_work_l0_finished_signal_);
  if (!s.ok()) {
    printf("error reclaiming L0 \n");
    return s;
  }
  return s;
}

Status DBImplZNS::RemoveObsoleteZonesLN() {
  mutex_.AssertHeld();
  Status s = Status::OK();
  s = versions_->ReclaimStaleSSTablesLN(&mutex_, &bg_work_finished_signal_);
  if (!s.ok()) {
    printf("error reclaiming LN \n");
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
