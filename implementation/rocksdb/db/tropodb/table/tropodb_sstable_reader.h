#pragma once
#ifdef TROPODB_PLUGIN_ENABLED
#ifndef ZNS_SSTABLE_READER_H
#define ZNS_SSTABLE_READER_H

#include "db/tropodb/table/tropodb_sstable.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
namespace ZNSEncoding {
extern const char* DecodeEncodedEntry(const char* p, const char* limit,
                                      uint32_t* shared, uint32_t* non_shared,
                                      uint32_t* value_length);

extern void ParseNextNonEncoded(char** src, Slice* key, Slice* value);
}  // namespace ZNSEncoding
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
