#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_MANIFEST_H
#define ZNS_MANIFEST_H

#include "db/zns_impl/io/szd_port.h"
#include "db/zns_impl/persistence/zns_committer.h"
#include "db/zns_impl/ref_counter.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
class ZnsManifest : public RefCounter {
 public:
  ZnsManifest(SZD::SZDChannelFactory* channel_factory,
              const SZD::DeviceInfo& info, const uint64_t min_zone_head,
              const uint64_t max_zone_head);
  ~ZnsManifest();
  Status NewManifest(const Slice& record);
  Status ReadManifest(std::string* manifest);
  Status GetCurrentWriteHead(uint64_t* current);
  Status SetCurrent(uint64_t current_lba);
  Status Recover();
  inline Status Reset() {
    Status s = FromStatus(log_.ResetAll());
    current_lba_ = min_zone_head_;
    return s;
  }

 private:
  inline Status RecoverLog() { return FromStatus(log_.RecoverPointers()); }
  Status TryGetCurrent(uint64_t* start_manifest, uint64_t* end_manifest);
  Status TryParseCurrent(uint64_t slba, uint64_t* start_manifest);
  Status ValidateManifestPointers() const;

  // State
  uint64_t current_lba_;
  uint64_t manifest_start_;
  uint64_t manifest_end_;
  // Log
  SZD::SZDCircularLog log_;
  ZnsCommitter committer_;
  // const after init
  const uint64_t min_zone_head_;
  const uint64_t max_zone_head_;
  const uint64_t zone_size_;
  const uint64_t lba_size_;
  // references
  SZD::SZDChannelFactory* channel_factory_;
};
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
