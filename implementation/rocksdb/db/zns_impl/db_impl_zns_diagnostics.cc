// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <numeric>
#include <set>
#include <string>
#include <vector>

#include "db/zns_impl/config.h"
#include "db/zns_impl/db_impl_zns.h"
#include "db/zns_impl/utils/tropodb_diagnostics.h"

namespace ROCKSDB_NAMESPACE {

static void PrintPerfCounterHeader() {
  std::ostringstream out;
  std::string table_line(113, '-');
  out << table_line << "\n";
  out << std::left << std::setw(20) << "| name" << std::setw(15)
            << "| total (ops)" << std::setw(15) << "| sum (micros)"
            << std::setw(15) << "| min (micros)" << std::setw(15)
            << "| max (micros)" << std::setw(15) << "| avg (micros)"
            << std::setw(15) << "| StdDev (micros)"
            << "|\n";
  out << table_line << "\n";
  TROPODB_PERF("%s", out.str().data());
}

static void PrintPerfCounterRow(std::string name,
                                const TimingCounter& counter) {
  std::ostringstream out;
  name = "| " + name;
  out << std::left << std::setw(20) << name << std::fixed
            << std::setprecision(0) << std::left << "|" << std::right
            << std::setw(14) << counter.GetNum() << std::left << "|"
            << std::right << std::setw(14) << counter.GetSum() << std::left
            << "|" << std::right << std::setw(14) << counter.GetMin()
            << std::left << "|" << std::right << std::setw(14)
            << counter.GetMax() << std::fixed << std::setprecision(2)
            << std::left << "|" << std::right << std::setw(14)
            << counter.GetAvg() << std::left << "|" << std::right
            << std::setw(14) << counter.GetStandardDeviation() << "  |\n";
  TROPODB_PERF("%s", out.str().data());
}

static void PrintPerfCounterTail() {
  std::ostringstream out;
  std::string table_line(113, '-');
  out << table_line << "\n";
  TROPODB_PERF("%s", out.str().data());
}

static void PrintCounterTable(
    const std::vector<std::pair<std::string, const TimingCounter&>> counters) {
  PrintPerfCounterHeader();
  for (auto& counter : counters) {
    PrintPerfCounterRow(counter.first, counter.second);
  }
  PrintPerfCounterTail();
}

void DBImplZNS::PrintCompactionStats() {
  TROPODB_PERF("==== Background operation ====\n");
  std::ostringstream out;
  out << "Flush count:\n\t" << flush_total_counter_.GetNum() << "\n";
  out << "Compaction count:\n";
  for (uint8_t level = 0; level < ZnsConfig::level_count - 1; level++) {
    out << "\tCompaction to " << (level + 1) << ":" << compactions_[level]
              << "\n";
  }
  TROPODB_PERF("%s", out.str().data());

  TROPODB_PERF("Flushes latency breakdown:\n");
  PrintCounterTable({{"Total", flush_total_counter_},
                     {"Writing L0", flush_flush_memtable_counter_},
                     {"Updating Version", flush_update_version_counter_},
                     {"Resetting WALs", flush_reset_wal_counter_}});

  TROPODB_PERF("L0 compaction latency breakdown:\n");
  PrintCounterTable({{"Total", compaction_compaction_L0_total_},
                     {"Picking SSTables", compaction_pick_compaction_},
                     {"Writing LN", compaction_compaction_},
                     {"Copying L0", compaction_compaction_trivial_},
                     {"Updating version", compaction_version_edit_},
                     {"Resetting L0", compaction_reset_L0_counter_}});

  TROPODB_PERF("LN compaction latency breakdown:\n");
  PrintCounterTable({{"Total", compaction_compaction_LN_total_},
                     {"Picking SSTables", compaction_pick_compaction_LN_},
                     {"Writing LN", compaction_compaction_LN_},
                     {"Copying LN", compaction_compaction_trivial_LN_},
                     {"Updating version", compaction_version_edit_LN_},
                     {"Resetting LN", compaction_reset_LN_counter_}});
}

void DBImplZNS::PrintSSTableStats() {
  TROPODB_PERF("====  SSTable layout ====\n");
  TROPODB_PERF("%s", versions_->DebugString().data());
}

void DBImplZNS::PrintWALStats() {
  TROPODB_PERF("====  WAL stats ====\n");
  for (size_t i = 0; i < ZnsConfig::lower_concurrency; i++) {
    std::ostringstream out;
    out << "====  WAL manager " << i << " ====\n";
    TROPODB_PERF("%s", out.str().data());
    std::vector<std::pair<std::string, const TimingCounter>> wal_stats =
        wal_man_[i]->GetAdditionalWALStatistics();
    std::vector<std::pair<std::string, const TimingCounter&>> wal_stats_ref;
    for (auto& p : wal_stats) {
      wal_stats_ref.push_back({p.first, p.second});
    }
    PrintCounterTable(wal_stats_ref);
  }
}

static void PrintIOColumn(const ZNSDiagnostics& diag) {
  std::ostringstream out;
  out << std::left << std::setw(10) << diag.name_ << std::right
            << std::setw(15) << diag.append_operations_counter_ << std::setw(25)
            << diag.bytes_written_ << std::setw(15)
            << diag.read_operations_counter_ << std::setw(25)
            << diag.bytes_read_ << std::setw(16) << diag.zones_erased_counter_
            << "\n";
  TROPODB_PERF("%s", out.str().data());
}

static void AddToJSONHotZoneStream(const ZNSDiagnostics& diag,
                                   std::ostringstream& erased,
                                   std::ostringstream& append) {
  for (uint64_t r : diag.zones_erased_) {
    erased << r << ",";
  }
  for (uint64_t a : diag.append_operations_) {
    append << a << ",";
  }
}

void DBImplZNS::PrintIODistrStats() {
  TROPODB_PERF("==== raw IO metrics ==== \n");
  std::ostringstream out;
  out << std::left << std::setw(10) << "Metric " << std::right
            << std::setw(15) << "Append (ops)" << std::setw(25)
            << "Written (Bytes)" << std::setw(15) << "Read (ops)"
            << std::setw(25) << "Read (Bytes)" << std::setw(16)
            << "Reset (zones)"
            << "\n";
  out << std::setfill('-') << std::setw(107) << "\n" << std::setfill(' ');
  struct ZNSDiagnostics totaldiag = {.name_ = "Total",
                                     .bytes_written_ = 0,
                                     .append_operations_counter_ = 0,
                                     .bytes_read_ = 0,
                                     .read_operations_counter_ = 0,
                                     .zones_erased_counter_ = 0};
  TROPODB_PERF("%s", out.str().data());
  std::ostringstream hotzones_reset;
  std::ostringstream hotzones_append;
  hotzones_reset << "[";
  hotzones_append << "[";
  {
    ZNSDiagnostics diag = manifest_->IODiagnostics();
    PrintIOColumn(diag);
    totaldiag.bytes_written_ += diag.bytes_written_;
    totaldiag.append_operations_counter_ += diag.append_operations_counter_;
    totaldiag.bytes_read_ += diag.bytes_read_;
    totaldiag.read_operations_counter_ += diag.read_operations_counter_;
    totaldiag.zones_erased_counter_ += diag.zones_erased_counter_;
    AddToJSONHotZoneStream(diag, hotzones_reset, hotzones_append);
  }
  {
    for (size_t i = 0; i < ZnsConfig::lower_concurrency; i++) {
      std::vector<ZNSDiagnostics> diags = wal_man_[i]->IODiagnostics();
      for (auto& diag : diags) {
        diag.name_ += i;
        PrintIOColumn(diag);
        totaldiag.bytes_written_ += diag.bytes_written_;
        totaldiag.append_operations_counter_ += diag.append_operations_counter_;
        totaldiag.bytes_read_ += diag.bytes_read_;
        totaldiag.read_operations_counter_ += diag.read_operations_counter_;
        totaldiag.zones_erased_counter_ += diag.zones_erased_counter_;
        AddToJSONHotZoneStream(diag, hotzones_reset, hotzones_append);
      }
    }
  }
  {
    std::vector<ZNSDiagnostics> diags = ss_manager_->IODiagnostics();
    for (auto& diag : diags) {
      PrintIOColumn(diag);
      totaldiag.bytes_written_ += diag.bytes_written_;
      totaldiag.append_operations_counter_ += diag.append_operations_counter_;
      totaldiag.bytes_read_ += diag.bytes_read_;
      totaldiag.read_operations_counter_ += diag.read_operations_counter_;
      totaldiag.zones_erased_counter_ += diag.zones_erased_counter_;
      AddToJSONHotZoneStream(diag, hotzones_reset, hotzones_append);
    }
  }
  PrintIOColumn(totaldiag);
  out.clear();
  out << std::setfill('_') << std::setw(107) << "\n" << std::setfill(' ');
  if (print_io_heat_stats_) {
    out << "=== Hot zones as a raw list === \n";
    out << hotzones_reset.str() << "]\n";
    out << hotzones_append.str() << "]\n";
  }
  TROPODB_PERF("%s", out.str().data());
}

void DBImplZNS::PrintStats() {
  if (print_compaction_stats_) {
    PrintCompactionStats();
  }
  if (print_ss_stats_) {
    PrintSSTableStats();
  }
  if (print_wal_stats_) {
    PrintWALStats();
  }
  if (print_io_stats_) {
    PrintIODistrStats();
  }
}
}  // namespace ROCKSDB_NAMESPACE