#pragma once
#ifdef TROPODB_PLUGIN_ENABLED
#ifndef TROPODB_ZONEMETADATA_H
#define TROPODB_ZONEMETADATA_H

#include "db/dbformat.h"

namespace ROCKSDB_NAMESPACE {
struct SSZoneMetaData {
  SSZoneMetaData()
      : refs(0), allowed_seeks(1 << 30), number(0), numbers(0), lba_count(0) {}
  static SSZoneMetaData copy(const SSZoneMetaData& m) {
    SSZoneMetaData mnew;
    mnew.refs = m.refs;
    mnew.allowed_seeks = m.allowed_seeks;
    mnew.number = m.number;
    mnew.L0 = {.lba = m.L0.lba, .log_number = m.L0.log_number};
    mnew.LN = {.lba_regions = m.LN.lba_regions};
    mnew.numbers = m.numbers;
    mnew.lba_count = m.lba_count;
    mnew.smallest = m.smallest;
    mnew.largest = m.largest;
    for (size_t i = 0; i < m.LN.lba_regions; i++) {
      mnew.LN.lbas[i] = m.LN.lbas[i];
      mnew.LN.lba_region_sizes[i] = m.LN.lba_region_sizes[i];
    }
    return mnew;
  }
  int refs;
  int allowed_seeks;  // Seeks allowed until compaction
  uint64_t number;    // version identifier
  struct {
    uint64_t lba{0};        // Lba when no regions are used
    uint8_t log_number{0};  // circular log number
    uint64_t number;        // Used for versioning with multiple L0 threads.
  } L0;
  struct {
    uint8_t lba_regions{0};        // Number of start lbas (legal from 1 to 8)
    uint64_t lbas[8];              // start lbas (can be multiple, up to 8)
    uint64_t lba_region_sizes[8];  // Size in zones of an lbas region
  } LN;
  uint64_t numbers;      // number of kv pairs
  uint64_t lba_count;    // data size in lbas
  InternalKey smallest;  // Smallest internal key served by table
  InternalKey largest;   // Largest internal key served by table
};

}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
