#pragma once
#ifdef TROPODB_PLUGIN_ENABLED
#ifndef TROPODB_DIAGNOSTICS_H
#define TROPODB_DIAGNOSTICS_H

#include <vector>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

// NOT thread-safe
struct TimingCounter {
  uint64_t num_;
  uint64_t sum_;
  uint64_t sum_squared_;
  uint64_t min_;
  uint64_t max_;
  
  TimingCounter();
  ~TimingCounter();

  TimingCounter operator + (TimingCounter const &tc);
  void operator += (TimingCounter const &tc);
  void AddTiming(uint64_t time);

  uint64_t GetNum() const;
  double GetSum() const;
  double GetMin() const;
  double GetMax() const;
  double GetAvg() const;
  double GetStandardDeviation() const; 
};

// NOT thread-safe
struct TropoDiagnostics {
  std::string name_;
  uint64_t bytes_written_;
  uint64_t append_operations_counter_;
  uint64_t bytes_read_;
  uint64_t read_operations_counter_;
  uint64_t zones_erased_counter_;
  std::vector<uint64_t> zones_erased_;
  std::vector<uint64_t> append_operations_;
};
}  // namespace ROCKSDB_NAMESPACE

#endif
#endif
