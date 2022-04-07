#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef REF_COUNTER_H
#define REF_COUNTER_H

#include <cstdio>
#include <assert.h>
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
class RefCounter {
 public:
  RefCounter() : refs_(0) {}
  virtual ~RefCounter() = default;
  inline void Ref() { ++refs_; }
  inline void Unref() {
    assert(refs_ >= 1);
    --refs_;
    if (refs_ == 0) {
      delete this;
    }
  }

  inline int Getref() {
    return refs_;
  }

 protected:
  int refs_;
};
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
