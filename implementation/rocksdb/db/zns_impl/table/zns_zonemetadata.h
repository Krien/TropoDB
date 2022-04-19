#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_ZONEMETADATA_H
#define ZNS_ZONEMETADATA_H

#include "db/dbformat.h"

namespace ROCKSDB_NAMESPACE {
struct SSZoneMetaData {
  SSZoneMetaData() : refs(0), allowed_seeks(1 << 30), lba_count(0) {}
  SSZoneMetaData(SSZoneMetaData* other)
      : refs(other->refs),
        allowed_seeks(other->allowed_seeks),
        number(other->number),
        lba(other->lba),
        numbers(other->numbers),
        lba_count(other->lba_count),
        smallest(other->smallest),
        largest(other->largest) {}
  int refs;
  int allowed_seeks;     // Seeks allowed until compaction
  uint64_t number;       // version identifuer
  uint64_t lba;          // start lba
  uint64_t numbers;      // number of kv pairs
  uint64_t lba_count;    // data size in lbas
  InternalKey smallest;  // Smallest internal key served by table
  InternalKey largest;   // Largest internal key served by table
};
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
