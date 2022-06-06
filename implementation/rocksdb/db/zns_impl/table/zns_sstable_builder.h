#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_SSTABLE_BUILDER_H
#define ZNS_SSTABLE_BUILDER_H

#include "db/zns_impl/table/zns_sstable.h"
#include "db/zns_impl/table/zns_zonemetadata.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
class ZnsSSTable;
class SSTableBuilder {
 public:
  SSTableBuilder(ZnsSSTable* table, SSZoneMetaData* meta, bool use_encoding);
  ~SSTableBuilder();
  uint64_t EstimateSizeImpact(const Slice& key, const Slice& value) const;
  Status Apply(const Slice& key, const Slice& value);
  Status Finalise();
  Status Flush();
  uint64_t GetSize() const { return (uint64_t)buffer_.size(); }

 private:
  // Used for generating the string
  bool started_;
  std::string buffer_;
  std::vector<uint32_t> kv_pair_offsets_;
  uint32_t kv_numbers_;
  uint32_t counter_;
  // Used when encoding is used
  bool use_encoding_;
  std::string last_key_;
  // References
  ZnsSSTable* table_;
  SSZoneMetaData* meta_;
};

}  // namespace ROCKSDB_NAMESPACE

#endif
#endif
