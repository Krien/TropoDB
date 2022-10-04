#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_DIAGNOSTICS_H
#define ZNS_DIAGNOSTICS_H

#include <vector>
#include <cmath>
#include <limits>

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
  
  TimingCounter() : num_(0), sum_(0), sum_squared_(0), min_(std::numeric_limits<uint64_t>::max()), max_(0) {}

  void AddTiming(uint64_t time) {
    if (time > max_) {
      max_ = time;
    }
    if (time < min_) {
      min_ = time;
    }
    num_++;
    sum_ += time;
    sum_squared_ += time * time;
  }

  uint64_t GetNum() const {
    return num_;
  }

  uint64_t GetMin() const {
    return min_;
  }

  uint64_t GetMax() const {
    return max_;
  }

  double GetAvg() const {
    return num_ == 0 ? -1. : 
      static_cast<double>(sum_) / static_cast<double>(num_);
  }

  double GetStandardDeviation() const {
    // See util/histogram.cc from LevelDB. However, we use -1 for none, not 0.
    return num_ == 0 ? -1. : 
      std::sqrt(static_cast<double>(sum_squared_ * num_ - sum_ * sum_) /
                static_cast<double>(num_ * num_));
  }      
};

// NOT thread-safe
struct ZNSDiagnostics {
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
