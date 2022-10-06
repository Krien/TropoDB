#include "db/zns_impl/utils/tropodb_diagnostics.h"

#include <cmath>
#include <limits>

namespace ROCKSDB_NAMESPACE {
TimingCounter::TimingCounter()
    : num_(0),
      sum_(0),
      sum_squared_(0),
      min_(std::numeric_limits<uint64_t>::max()),
      max_(0) {}

TimingCounter::~TimingCounter() {}

TimingCounter TimingCounter::operator+(TimingCounter const &tc) {
  TimingCounter tc_new;
  tc_new.num_ = num_ + tc.num_;
  tc_new.sum_ = sum_ + tc.sum_;
  tc_new.sum_squared_ = sum_squared_ + tc.sum_squared_;
  if (num_ == 0) {
    tc_new.min_ = tc.min_;
    tc_new.max_ = tc.max_;
  } else if (tc.num_ == 0) {
    tc_new.min_ = min_;
    tc_new.max_ = max_;
  } else {
    tc_new.min_ = std::min(min_, tc.min_);
    tc_new.max_ = std::max(max_, tc.max_);
  }
  return tc_new;
}

void TimingCounter::operator+=(TimingCounter const &tc) {
  num_ += tc.num_;
  sum_ += tc.sum_;
  sum_squared_ += tc.sum_squared_;
  if (num_ == 0) {
    min_ = tc.min_;
    max_ = tc.max_;
  } else if (tc.num_ == 0) {
    // nothing to do
  } else {
    min_ = std::min(min_, tc.min_);
    max_ = std::max(max_, tc.max_);
  }
}

void TimingCounter::AddTiming(uint64_t time) {
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

uint64_t TimingCounter::GetNum() const { return num_; }

double TimingCounter::GetSum() const { return static_cast<double>(sum_); }

double TimingCounter::GetMin() const {
  return min_ == std::numeric_limits<uint64_t>::max()
             ? -1.
             : static_cast<double>(min_);
}

double TimingCounter::GetMax() const {
  return max_ == 0 ? -1. : static_cast<double>(max_);
}

double TimingCounter::GetAvg() const {
  return num_ == 0 ? -1.
                   : static_cast<double>(sum_) / static_cast<double>(num_);
}

double TimingCounter::GetStandardDeviation() const {
  // See util/histogram.cc from LevelDB. However, we use -1 for none, not 0.
  return num_ == 0 ? -1.
                   : std::sqrt(static_cast<double>(sum_squared_ * num_ -
                                                   sum_ * sum_) /
                               static_cast<double>(num_ * num_));
}
}  // namespace ROCKSDB_NAMESPACE
