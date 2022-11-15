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
#include "db/tropodb/index/tropodb_compaction.h"
#include "db/tropodb/index/tropodb_version.h"
#include "db/tropodb/index/tropodb_version_set.h"
#include "db/tropodb/io/szd_port.h"
#include "db/tropodb/persistence/tropodb_manifest.h"
#include "db/tropodb/persistence/tropodb_wal.h"
#include "db/tropodb/persistence/tropodb_wal_manager.h"
#include "db/tropodb/table/tropodb_sstable_manager.h"
#include "db/tropodb/table/tropodb_table_cache.h"
#include "db/tropodb/tropodb_config.h"
#include "db/tropodb/tropodb_impl.h"
#include "db/tropodb/utils/tropodb_logger.h"
#include "db/write_batch_internal.h"
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

Status TropoDBImpl::FlushL0SSTables(std::vector<SSZoneMetaData>& metas,
                                    uint8_t parallel_number) {
  return ss_manager_->FlushMemTable(imm_[parallel_number], metas,
                                    parallel_number, env_);
}

Status TropoDBImpl::CompactMemtable(uint8_t parallel_number) {
  mutex_.AssertHeld();
  // Wait till there is space to flush (* 1.2 leaves buffer space for
  // serialisation)
  while (imm_[parallel_number]->GetInternalSize() * 1.2 >
         ss_manager_->SpaceRemainingInBytesL0(parallel_number)) {
    // Maybe there is no peer yet?, try to create one.
    MaybeScheduleCompactionL0();
    TROPO_LOG_DEBUG(
        "Flush: waiting for peer thread, can not flush: memtable %f Mb "
        "approaches available %f Mb \n",
        (float)imm_[parallel_number]->GetInternalSize() / 1024. / 1024.,
        (float)ss_manager_->SpaceRemainingL0(parallel_number) / 1024. / 1024.);
    bg_work_l0_finished_signal_.Wait();
  }
  assert(imm_[parallel_number] != nullptr);
  assert(bg_flush_scheduled_[parallel_number]);

  uint64_t before;
  Status s = Status::OK();

  // Flush and set new version
  {
    // Unlike LN/L0 compactions we need no refs to current index (we simply do
    // not care)
    before = clock_->NowMicros();
    mutex_.Unlock();
    // Flush memtable and generate "N" new SSTables (metadata still needs to be
    // transformed)
    std::vector<SSZoneMetaData> metas;
    s = FlushL0SSTables(metas, parallel_number);
    mutex_.Lock();
    flush_flush_memtable_counter_.AddTiming(clock_->NowMicros() - before);
    if (!s.ok()) {
      bg_error_ = s;
      TROPO_LOG_ERROR("ERROR: Flush: Can not flush memtable to storage\n");
    }

    if (s.ok() && metas.size() > 0) {
      before = clock_->NowMicros();
      TropoVersionEdit edit;
      uint64_t l0_number = versions_->NewSSNumberL0();
      // Auto-correct all SSTables to get unique monotic numbers
      for (auto& meta : metas) {
        if (meta.lba_count > 0) {
          meta.number = versions_->NewSSNumber();
          meta.L0.number = l0_number;
          edit.AddSSDefinition(0 /*level*/, meta);
        }
      }
      s = versions_->LogAndApply(&edit);
      flush_update_version_counter_.AddTiming(clock_->NowMicros() - before);
      if (!s.ok()) {
        bg_error_ = s;
        TROPO_LOG_ERROR("ERROR: Flush: Can not alter version structure\n");
      }
    }
    imm_[parallel_number]->Unref();
    imm_[parallel_number] = nullptr;
  }

  // Reset WALs
  {
    before = clock_->NowMicros();
    s = wal_man_[parallel_number]->ResetOldWALs(&mutex_);
    flush_reset_wal_counter_.AddTiming(clock_->NowMicros() - before);
    if (!s.ok()) {
      bg_error_ = s;
      TROPO_LOG_ERROR("ERROR: Flush: WALs could not be reset\n");
    }
  }
  return s;
}

void TropoDBImpl::BackgroundFlush(uint8_t parallel_number) {
  mutex_.AssertHeld();
  Status s;

  // It is possible that a flush is scheduled because there are no WALs left,
  // but there is no immutable table. Only reset WAL in this case.
  if (imm_[parallel_number] != nullptr) {
    s = CompactMemtable(parallel_number);
    if (!s.ok()) {
      TROPO_LOG_ERROR("ERROR: Flush: Can not flush memtable\n");
    }
  } else if (!wal_man_[parallel_number]->WALAvailable()) {
    TROPO_LOG_INFO("BG operation: Resetting WALs to make space\n");
    uint64_t before = clock_->NowMicros();
    s = wal_man_[parallel_number]->ResetOldWALs(&mutex_);
    flush_reset_wal_counter_.AddTiming(clock_->NowMicros() - before);
    TROPO_LOG_INFO("BG operation: Reset WALs to make space\n");
  } else {
    TROPO_LOG_ERROR(
        "ERROR: Flush: No immutable table to flush or WAL to reset\n");
  }
}

void TropoDBImpl::BackgroundFlushCall(uint8_t parallel_number) {
  MutexLock l(&mutex_);
  assert(bg_flush_scheduled_);
  // Hack to ensure that during a WAL test we do NOT test bg operations.
#ifdef DISABLE_BACKGROUND_OPS
  wal_man_[parallel_number]->ResetOldWALs(&mutex_);
#ifndef DISABLE_BACKGROUND_OPS_AND_RESETS
  bg_flush_work_finished_signal_.SignalAll();
#endif
  return;
#endif
  // TODO: how to deal with bg_error?
  if (!bg_error_.ok()) {
  } else {
    TROPO_LOG_INFO("BG operation: Flush started\n");
    uint64_t before = clock_->NowMicros();
    BackgroundFlush(parallel_number);
    flush_total_counter_.AddTiming(clock_->NowMicros() - before);
    TROPO_LOG_INFO("BG operation: Flush completed\n");
  }
  bg_flush_scheduled_[parallel_number] = false;
  forced_schedule_ = false;
  // Bg operations can cascade, but shutdown if ordered
  if (!shutdown_) {
    MaybeScheduleFlush(parallel_number);
    MaybeScheduleCompactionL0();
    MaybeScheduleCompaction(false);
  }
  // Only signal all threads that can wait for a flush.
  bg_flush_work_finished_signal_.SignalAll();
}

void TropoDBImpl::BGFlushWork(void* data) {
  FlushData* flush_data = reinterpret_cast<FlushData*>(data);
  flush_data->db_->BackgroundFlushCall(flush_data->parallel_number_);
  // Flush data is generated by schedule, but not managed.
  delete flush_data;
}

void TropoDBImpl::MaybeScheduleFlush(uint8_t parallel_number) {
  mutex_.AssertHeld();
  // No duplicate or unnecessary flushes
  if (bg_flush_scheduled_[parallel_number]) {
    return;
  } else if ((imm_[parallel_number] == nullptr &&
              wal_man_[parallel_number]->WALAvailable())) {
    return;
  }
  bg_flush_scheduled_[parallel_number] = true;
  // ! cleanup in callee !
  FlushData* dat = new FlushData(this, parallel_number);
  env_->Schedule(&TropoDBImpl::BGFlushWork, dat, rocksdb::Env::HIGH);
}

Status TropoDBImpl::RemoveObsoleteZonesL0() {
  mutex_.AssertHeld();
  Status s =
      versions_->ReclaimStaleSSTablesL0(&mutex_, &bg_work_l0_finished_signal_);
  if (!s.ok()) {
    TROPO_LOG_ERROR("ERROR: Reclaiming L0 zones \n");
  }
  return s;
}

void TropoDBImpl::BackgroundCompactionL0Call() {
  MutexLock l(&mutex_);
  assert(bg_compaction_l0_scheduled_);
  // TODO: how to deal with bg_error_?
  if (!bg_error_.ok()) {
  } else {
    TROPO_LOG_INFO("BG operation: L0 compaction started\n");
    uint64_t before = clock_->NowMicros();
    BackgroundCompactionL0();
    compaction_compaction_L0_total_.AddTiming(clock_->NowMicros() - before);
    TROPO_LOG_INFO("BG operation: L0 compaction finished\n");
  }
  bg_compaction_l0_scheduled_ = false;
  // Background orders can cascade, but shutdown if ordered
  if (!shutdown_) {
    MaybeScheduleCompactionL0();
    MaybeScheduleCompaction(false);
  }
  // TODO: investigate if all three signals are needed.
  bg_work_l0_finished_signal_.SignalAll();
  bg_work_finished_signal_.SignalAll();
  bg_flush_work_finished_signal_.SignalAll();
}

void TropoDBImpl::BackgroundCompactionL0() {
  mutex_.AssertHeld();
  // We do not need a compaction in L0, return.
  if (!versions_->NeedsL0Compaction()) {
    return;
  }

  Status s;
  uint64_t before;

  // It is possible that we only need deletes (and not compaction)
  // This happens when a lot of readers or threads kept references to the MVCC
  // of TropoDB (hence we could not delete)
  if (versions_->OnlyNeedDeletes(0 /*level*/)) {
    // TODO: we should probably wait a while instead of spamming reset requests.
    // Then probably all clients are done with their reads on old versions.
    before = clock_->NowMicros();
    s = RemoveObsoleteZonesL0();
    compaction_reset_L0_counter_.AddTiming(clock_->NowMicros() - before);
    if (!s.ok()) {
      bg_error_ = s;
      TROPO_LOG_ERROR("ERROR: L0 compaction: Can not reclaim L0 zones\n");
    }
    TROPO_LOG_DEBUG("BG Operation: L0 only reclaimed\n");
    return;
  }

  // Compact L0 to storage and prepare version
  TropoVersion* current = versions_->current();
  TropoVersionEdit edit;
  {
    // Pick compaction
    before = clock_->NowMicros();
    TropoCompaction* c =
        versions_->PickCompaction(0 /*level*/, reserved_comp_[1] /*LN thread*/);
    // Can not do this compaction while the other BG uses the same tables
    while (reserve_claimed_ == 1 ||
           c->HasOverlapWithOtherCompaction(reserved_comp_[1])) {
      TROPO_LOG_DEBUG(
          "BG Operation: L0 compaction: Overlap with LN write %u %u\n",
          reserve_claimed_ == 1,
          c->HasOverlapWithOtherCompaction(reserved_comp_[1]));
      delete c;
      reserve_claimed_ = reserve_claimed_ == -1 ? 0 : reserve_claimed_;
      bg_work_finished_signal_.Wait();
      current = versions_->current();
      c = versions_->PickCompaction(0 /*level*/, reserved_comp_[1]);
    }
    reserve_claimed_ = reserve_claimed_ == 0 ? -1 : reserve_claimed_;
    c->GetCompactionTargets(&reserved_comp_[0]);
    compaction_pick_compaction_.AddTiming(clock_->NowMicros() - before);

    // Do compaction
    before = clock_->NowMicros();
    current->Ref();
    bool trivial = c->IsTrivialMove();
    mutex_.Unlock();
    c->MarkCompactedTablesAsDead(&edit);
    if (trivial) {
      TROPO_LOG_INFO("BG Operation: Starting L0 Trivial move\n");
      s = c->DoTrivialMove(&edit);
      TROPO_LOG_INFO("BG Operation: Finished L0 Trivial move\n");
    } else {
      TROPO_LOG_INFO("BG Operation: Starting L0 Non-trivial compaction\n");
      s = c->DoCompaction(&edit);
      // Compaction perf counters
      {
        compaction_setup_perf_counter_ += c->GetCompactionSetupPerfCounter();
        compaction_k_merge_perf_counter_ += c->GetCompactionKMergePerfCounter();
        compaction_flush_perf_counter_ += c->GetCompactionFlushPerfCounter();
        compaction_breakdown_perf_counter_ +=
            c->GetCompactionBreakdownPerfCounter();
      }
      TROPO_LOG_INFO("BG Operation: Finished L0 Non-trivial compaction\n");
    }
    // Note if this delete is somehow not reached, a stale version will remain
    // in memory for the rest of this session (memory leak).
    delete c;
    mutex_.Lock();
    if (trivial) {
      compaction_compaction_trivial_.AddTiming(clock_->NowMicros() - before);
    } else {
      compaction_compaction_.AddTiming(clock_->NowMicros() - before);
    }
    current->Unref();

    if (!s.ok()) {
      TROPO_LOG_ERROR("ERROR: Compaction L0: Could not compact\n");
      reserved_comp_[0].clear();
      bg_error_ = s;
      return;
    }
  }

  // Apply version
  {
    before = clock_->NowMicros();
    s = versions_->LogAndApply(&edit);
    reserved_comp_[0].clear();
    compaction_version_edit_.AddTiming(clock_->NowMicros() - before);
    if (!s.ok()) {
      TROPO_LOG_ERROR("ERROR: Compaction L0: Could not apply to version\n");
      bg_error_ = s;
      return;
    }
    // Diag
    compactions_[0 /*level*/]++;
  }

  // Reset L0 zones
  {
    before = clock_->NowMicros();
    s = RemoveObsoleteZonesL0();
    compaction_reset_L0_counter_.AddTiming(clock_->NowMicros() - before);
    if (!s.ok()) {
      TROPO_LOG_ERROR("ERROR: Compaction L0: Resetting L0 Zones\n");
      bg_error_ = s;
      return;
    }
  }

  if (!s.ok()) {
    TROPO_LOG_ERROR("ERROR: Compaction L0: error in flow control of %s:%s \n",
                    __FILE__, __func__);
    bg_error_ = s;
  }
}

void TropoDBImpl::MaybeScheduleCompactionL0() {
  mutex_.AssertHeld();
  // No duplicate scheduling or unnecessary scheduling
  if (!bg_error_.ok()) {
    return;
  } else if (bg_compaction_l0_scheduled_) {
    return;
  } else if (!versions_->NeedsL0Compaction()) {
    return;
  }
  bg_compaction_l0_scheduled_ = true;
  env_->Schedule(&TropoDBImpl::BGCompactionL0Work, this, rocksdb::Env::HIGH);
}

void TropoDBImpl::BGCompactionL0Work(void* db) {
  reinterpret_cast<TropoDBImpl*>(db)->BackgroundCompactionL0Call();
}

Status TropoDBImpl::RemoveObsoleteZonesLN() {
  mutex_.AssertHeld();
  Status s =
      versions_->ReclaimStaleSSTablesLN(&mutex_, &bg_work_finished_signal_);
  if (!s.ok()) {
    TROPO_LOG_ERROR("ERROR: Reclaiming LN zones\n");
  }
  return s;
}

void TropoDBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();

  Status s;
  TropoVersion* current = versions_->current();
  uint64_t before;

  // Not a valid compaction
  if (current->CompactionLevel() >= TropoDBConfig::level_count) {
    TROPO_LOG_ERROR("ERROR: LN Compaction: not a valid compaction level \n");
    return;
  }

  // It is possible that we only need deletes (and not compaction)
  // This happens when a lot of readers or threads kept references to the MVCC
  // of TropoDB (hence we could not delete)
  if (versions_->OnlyNeedDeletes(current->CompactionLevel())) {
    // TODO: we should probably wait a while instead of spamming reset requests.
    // Then probably all clients are done with their reads on old versions.
    before = clock_->NowMicros();
    s = RemoveObsoleteZonesLN();
    compaction_reset_LN_counter_.AddTiming(clock_->NowMicros() - before);
    versions_->RecalculateScore();
    if (!s.ok()) {
      bg_error_ = s;
      TROPO_LOG_ERROR("ERROR: LN compaction: Can not reclaim LN zones\n");
    }
    TROPO_LOG_DEBUG("BG Operation: LN only reclaimed\n");
    return;
  }

  // Compact LN to storage and prepare version
  TropoVersionEdit edit;
  {
    // Pick compaction
    before = clock_->NowMicros();
    TropoCompaction* c = versions_->PickCompaction(current->CompactionLevel(),
                                                   reserved_comp_[0]);
    while (reserve_claimed_ == 0 ||
           c->HasOverlapWithOtherCompaction(reserved_comp_[0]) || c->IsBusy()) {
      TROPO_LOG_DEBUG(
          "BG Operation: LN compaction: Overlap with L0 write %u %u %u\n",
          reserve_claimed_ == 0,
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

    // Do compaction
    before = clock_->NowMicros();
    current->Ref();
    bool istrivial = c->IsTrivialMove();
    mutex_.Unlock();
    c->MarkCompactedTablesAsDead(&edit);
    if (istrivial) {
      TROPO_LOG_INFO("BG Operation: Starting LN Trivial move\n");
      s = c->DoTrivialMove(&edit);
      TROPO_LOG_INFO("BG Operation: Finished LN Trivial move\n");
    } else {
      TROPO_LOG_INFO("BG Operation: Starting LN Non-trivial compaction\n");
      s = c->DoCompaction(&edit);
      // Compaction perf counters
      {
        compaction_setup_perf_counter_ += c->GetCompactionSetupPerfCounter();
        compaction_k_merge_perf_counter_ += c->GetCompactionKMergePerfCounter();
        compaction_flush_perf_counter_ += c->GetCompactionFlushPerfCounter();
        compaction_breakdown_perf_counter_ +=
            c->GetCompactionBreakdownPerfCounter();
      }
      TROPO_LOG_INFO("BG Operation: Finished LN Non-trivial compaction\n");
    }
    // Note if this delete is not reached, a stale version will remain in memory
    // for the rest of this session (memory leak).
    delete c;
    mutex_.Lock();
    if (istrivial) {
      compaction_compaction_trivial_LN_.AddTiming(clock_->NowMicros() - before);
    } else {
      compaction_compaction_LN_.AddTiming(clock_->NowMicros() - before);
    }
    current->Unref();

    if (!s.ok()) {
      TROPO_LOG_ERROR("ERROR: Compaction LN: Could not compact\n");
      bg_error_ = s;
      return;
    }
  }

  // Apply version
  {
    before = clock_->NowMicros();
    s = versions_->LogAndApply(&edit);
    reserved_comp_[1].clear();
    compaction_version_edit_LN_.AddTiming(clock_->NowMicros() - before);
    if (!s.ok()) {
      TROPO_LOG_ERROR("ERROR: Compaction LN: Could not apply to version\n");
      bg_error_ = s;
      return;
    }
    // Diag
    compactions_[current->CompactionLevel()]++;
  }

  // Reset LN zones
  {
    before = clock_->NowMicros();
    s = s.ok() ? RemoveObsoleteZonesLN() : s;
    compaction_reset_LN_counter_.AddTiming(clock_->NowMicros() - before);
    if (!s.ok()) {
      TROPO_LOG_ERROR("ERROR: Compaction LN: Resetting LN zones\n");
      bg_error_ = s;
    }
    // LN compaction uses bytes (dead and alive) to determine score
    versions_->RecalculateScore();
  }

  if (!s.ok()) {
    TROPO_LOG_ERROR("ERROR: Compaction LN: error in flow control of %s:%s \n",
                    __FILE__, __func__);
    bg_error_ = s;
  }
}

void TropoDBImpl::BackgroundCompactionCall() {
  MutexLock l(&mutex_);
  assert(bg_compaction_scheduled_);
  // TODO: How to deal with background errors?
  if (!bg_error_.ok()) {
  } else {
    TROPO_LOG_INFO("BG operation: LN compaction started\n");
    uint64_t before = clock_->NowMicros();
    BackgroundCompaction();
    compaction_compaction_LN_total_.AddTiming(clock_->NowMicros() - before);
    TROPO_LOG_INFO("BG operation: LN compaction finished\n");
  }
  bg_compaction_scheduled_ = false;
  forced_schedule_ = false;
  // Background operations can cascade, but shutdown if ordered
  if (!shutdown_) {
    MaybeScheduleCompaction(false);
  }
  // TODO: investigate if all signals are needed
  bg_work_finished_signal_.SignalAll();
  bg_flush_work_finished_signal_.SignalAll();
  bg_work_l0_finished_signal_.SignalAll();
}

void TropoDBImpl::BGCompactionWork(void* db) {
  reinterpret_cast<TropoDBImpl*>(db)->BackgroundCompactionCall();
}

void TropoDBImpl::MaybeScheduleCompaction(bool force) {
  mutex_.AssertHeld();
  // No duplicate or unnecessary compactions
  if (!bg_error_.ok()) {
    return;
  } else if (bg_compaction_scheduled_) {
    return;
  } else if (!force && !versions_->NeedsCompaction()) {
    return;
  }
  forced_schedule_ = force;
  bg_compaction_scheduled_ = true;
  env_->Schedule(&TropoDBImpl::BGCompactionWork, this, rocksdb::Env::LOW);
}

}  // namespace ROCKSDB_NAMESPACE
