#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_WAL_H
#define ZNS_WAL_H

#include "db/zns_impl/io/device_wrapper.h"
#include "db/zns_impl/io/qpair_factory.h"
#include "db/zns_impl/memtable/zns_memtable.h"
#include "db/zns_impl/persistence/zns_committer.h"
#include "db/zns_impl/ref_counter.h"
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
  ZNSWAL(QPairFactory* qpair_factory, const SZD::DeviceInfo& info,
         const uint64_t min_zone_head, uint64_t max_zone_head);
  // No copying or implicits
  ZNSWAL(const ZNSWAL&) = delete;
  ZNSWAL& operator=(const ZNSWAL&) = delete;
  ~ZNSWAL();
  void Append(Slice data);
  Status Reset();
  Status Recover();
  Status Replay(ZNSMemTable* mem, SequenceNumber* seq);
  bool Empty();
  bool SpaceLeft(const Slice& data);

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
  SZD::QPair** qpair_;
  ZnsCommitter* committer_;
};
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
