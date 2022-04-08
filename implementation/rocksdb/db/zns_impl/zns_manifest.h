#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_MANIFEST_H
#define ZNS_MANIFEST_H

#include "db/zns_impl/device_wrapper.h"
#include "db/zns_impl/qpair_factory.h"
#include "db/zns_impl/ref_counter.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
class ZnsManifest : public RefCounter {
 public:
  ZnsManifest(QPairFactory* qpair_factory, const ZnsDevice::DeviceInfo& info,
              const uint64_t min_zone_head, uint64_t max_zone_head);
  ~ZnsManifest();
  Status Scan();
  Status NewManifest(const Slice& record);
  Status GetCurrentWriteHead(uint64_t* current);
  Status SetCurrent(uint64_t current_lba);
  Status RemoveObsoleteZones();
  Status Reset();

 private:
  // ephemeral
  uint64_t current_lba_;
  // logic
  uint64_t zone_head_;
  uint64_t write_head_;
  uint64_t min_zone_head_;
  uint64_t max_zone_head_;
  uint64_t zone_size_;
  uint64_t lba_size_;
  uint64_t zone_byte_range;
  // references
  QPairFactory* qpair_factory_;
  ZnsDevice::QPair** qpair_;
};
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
