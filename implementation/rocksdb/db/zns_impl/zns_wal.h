#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_WAL_H
#define ZNS_WAL_H

#include "db/zns_impl/device_wrapper.h"
#include "db/zns_impl/qpair_factory.h"
#include "db/zns_impl/ref_counter.h"
#include "db/zns_impl/zns_memtable.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"


namespace ROCKSDB_NAMESPACE {
/**
 * @brief
 *
 */
class ZNSWAL : public RefCounter {
 public:
  ZNSWAL(QPairFactory* qpair_factory, const ZnsDevice::DeviceInfo& info,
         const uint64_t min_zone_head, uint64_t max_zone_head);
  // No copying or implicits
  ZNSWAL(const ZNSWAL&) = delete;
  ZNSWAL& operator=(const ZNSWAL&) = delete;
  ~ZNSWAL();
  void Append(Slice data);
  Status Reset();
  Status Recover();
  Status Replay(ZNSMemTable* mem, SequenceNumber* seq);

 private:
  // data
  uint64_t zone_head_;
  uint64_t write_head_;
  uint64_t min_zone_head_;
  uint64_t max_zone_head_;
  uint64_t zone_size_;
  uint64_t lba_size_;
  // references
  QPairFactory* qpair_factory_;
  ZnsDevice::QPair** qpair_;
};
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
