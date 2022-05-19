#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_ZONEMETADATA_H
#define ZNS_ZONEMETADATA_H

#include "db/dbformat.h"

namespace ROCKSDB_NAMESPACE {
struct SSZoneMetaData {
  SSZoneMetaData()
      : refs(0), allowed_seeks(1 << 30), number(0), numbers(0), lba_count(0) {}
  virtual ~SSZoneMetaData() = default;
  int refs;
  int allowed_seeks;     // Seeks allowed until compaction
  uint64_t number;       // version identifier
  uint64_t numbers;      // number of kv pairs
  uint64_t lba_count;    // data size in lbas
  InternalKey smallest;  // Smallest internal key served by table
  InternalKey largest;   // Largest internal key served by table
  virtual bool DecodeFrom(Slice* input) = 0;
  virtual void Encode(std::string* dst) const = 0;
};

}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
