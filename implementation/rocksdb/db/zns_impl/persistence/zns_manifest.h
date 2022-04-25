#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_MANIFEST_H
#define ZNS_MANIFEST_H

#include "db/zns_impl/io/device_wrapper.h"
#include "db/zns_impl/io/qpair_factory.h"
#include "db/zns_impl/persistence/zns_committer.h"
#include "db/zns_impl/ref_counter.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
class ZnsManifest : public RefCounter {
 public:
  ZnsManifest(QPairFactory* qpair_factory, const SZD::DeviceInfo& info,
              const uint64_t min_zone_head, uint64_t max_zone_head);
  ~ZnsManifest();
  Status Scan();
  Status NewManifest(const Slice& record);
  Status ReadManifest(std::string* manifest);
  Status GetCurrentWriteHead(uint64_t* current);
  Status SetCurrent(uint64_t current_lba);
  Status Recover();
  Status RemoveObsoleteZones();
  Status Reset();

 private:
  Status RecoverLog();
  Status TryGetCurrent(uint64_t* start_manifest, uint64_t* end_manifest);
  Status TryParseCurrent(uint64_t slba, uint64_t* start_manifest);
  Status ValidateManifestPointers();

  // ephemeral
  uint64_t current_lba_;
  uint64_t manifest_start_;
  uint64_t manifest_end_;
  // logic
  uint64_t zone_head_;
  uint64_t write_head_;
  uint64_t zone_tail_;
  uint64_t min_zone_head_;
  uint64_t max_zone_head_;
  uint64_t zone_size_;
  uint64_t lba_size_;
  uint64_t zone_byte_range;
  // references
  QPairFactory* qpair_factory_;
  SZD::QPair** qpair_;
  ZnsCommitter* committer_;
};
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
