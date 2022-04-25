#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_UTILS_H
#define ZNS_UTILS_H
#include "db/zns_impl/io/device_wrapper.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {
namespace ZnsUtils {
inline void allign_size(uint64_t* alligned_size, Slice slice,
                        uint64_t lba_size) {
  uint64_t slice_size = (uint64_t)slice.size();
  // allign
  *alligned_size = (slice_size / lba_size) * lba_size;
  *alligned_size += slice_size % lba_size != 0 ? lba_size : 0;
}

inline char* slice_to_spdkformat(uint64_t* alligned_size, Slice slice,
                                 SZD::QPair* qpair, uint64_t lba_size) {
  uint64_t slice_size = (uint64_t)slice.size();
  // allign
  uint64_t zcalloc_size = (slice_size / lba_size) * lba_size;
  zcalloc_size += slice_size % lba_size != 0 ? lba_size : 0;
  // copy to dma memory
  char* payload = (char*)SZD::z_calloc(qpair, zcalloc_size, sizeof(char));
  if (payload == nullptr) {
    return nullptr;
  }
  memcpy(payload, slice.data(), slice_size);
  *alligned_size = zcalloc_size;
  return payload;
}
}  // namespace ZnsUtils
}  // namespace ROCKSDB_NAMESPACE

#endif
#endif
