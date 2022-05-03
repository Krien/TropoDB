#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_SSTABLE_COMMON_H
#define ZNS_SSTABLE_COMMON_H

#include "db/zns_impl/table/zns_sstable.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
void EncodeKVPair(std::string* dst, const Slice& key, const Slice& value);
void EncodeSSTablePreamble(std::string* dst,
                           const std::vector<uint32_t>& kv_pair_offsets_);
}  // namespace ROCKSDB_NAMESPACE

#endif
#endif