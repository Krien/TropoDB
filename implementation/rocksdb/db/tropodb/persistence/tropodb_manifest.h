#pragma once
#ifdef TROPODB_PLUGIN_ENABLED
#ifndef TROPODB_MANIFEST_H
#define TROPODB_MANIFEST_H

#include "db/tropodb/utils/tropodb_diagnostics.h"
#include "db/tropodb/io/szd_port.h"
#include "db/tropodb/persistence/tropodb_committer.h"
#include "db/tropodb/ref_counter.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
class TropoManifest : public RefCounter {
 public:
  TropoManifest(SZD::SZDChannelFactory* channel_factory,
              const SZD::DeviceInfo& info, const uint64_t min_zone_nr,
              const uint64_t max_zone_nr);
  ~TropoManifest();
  Status NewManifest(const Slice& record);
  Status ReadManifest(std::string* manifest);
  Status SetCurrent();
  Status Recover();
  Status Reset();

  inline TropoDiagnostics GetDiagnostics() const {
    struct TropoDiagnostics diag = {
        .name_ = "Manifest",
        .bytes_written_ = log_.GetBytesWritten(),
        .append_operations_counter_ = log_.GetAppendOperationsCounter(),
        .bytes_read_ = log_.GetBytesRead(),
        .read_operations_counter_ = log_.GetReadOperationsCounter(),
        .zones_erased_counter_ = log_.GetZonesResetCounter(),
        .zones_erased_ = log_.GetZonesReset(),
        .append_operations_ = log_.GetAppendOperations()};
    return diag;
  }
  inline TropoDiagnostics IODiagnostics() {
    struct TropoDiagnostics diag = GetDiagnostics();
    return diag;
  }

 private:
  inline Status RecoverLog() { return FromStatus(log_.RecoverPointers()); }
  Status TryGetCurrent(uint64_t* start_manifest, uint64_t* end_manifest,
                       uint64_t* start_manifest_delete,
                       uint64_t* end_manifest_delete);
  Status TryParseCurrent(uint64_t slba, uint64_t* start_manifest,
                         uint64_t* end_manifest,
                         uint64_t* start_manifest_delete,
                         uint64_t* end_manifest_delete,
                         TropoCommitReader& reader);
  Status ValidateManifestPointers() const;

  // State
  uint64_t manifest_start_;
  uint64_t manifest_blocks_;
  uint64_t manifest_start_new_;
  uint64_t manifest_blocks_new_;
  uint64_t deleted_range_begin_;
  uint64_t deleted_range_blocks_;
  // Log
  SZD::SZDCircularLog log_;
  TropoCommitter committer_;
  // const after init
  const uint64_t min_zone_head_;
  const uint64_t max_zone_head_;
  const uint64_t zone_cap_;
  const uint64_t lba_size_;
  // references
  SZD::SZDChannelFactory* channel_factory_;
};
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
