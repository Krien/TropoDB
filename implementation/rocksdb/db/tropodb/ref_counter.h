#pragma once
#ifdef TROPODB_PLUGIN_ENABLED
#ifndef TROPODB_REF_COUNTER_H
#define TROPODB_REF_COUNTER_H

#include <assert.h>

#include <cstdio>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
class RefCounter {
 public:
  RefCounter() : refs_(0) {}
  virtual ~RefCounter() = default;
  inline void Ref() { ++refs_; }
  inline void Unref() {
    assert(refs_ >= 1);
    if (--refs_ <= 0) {
      delete this;
    }
  }

  inline size_t Getref() { return refs_; }

 protected:
  size_t refs_ = 0;
};
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
