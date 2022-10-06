#pragma once
#ifdef TROPODB_PLUGIN_ENABLED
#ifndef ZNS_SSTABLE_H
#define ZNS_SSTABLE_H

#include "db/zns_impl/utils/tropodb_diagnostics.h"
#include "db/zns_impl/io/szd_port.h"
#include "db/zns_impl/memtable/zns_memtable.h"
#include "db/zns_impl/ref_counter.h"
#include "db/zns_impl/table/zns_sstable_builder.h"
#include "db/zns_impl/table/zns_zonemetadata.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
enum class EntryStatus { found, deleted, notfound };

class ZNSSSTableManager;
class SSTableBuilder;

class ZnsSSTable {
 public:
  ZnsSSTable(SZD::SZDChannelFactory* channel_factory,
             const SZD::DeviceInfo& info, const uint64_t min_zone_nr,
             const uint64_t max_zone_nr)
      : min_zone_head_(min_zone_nr * info.zone_cap),
        max_zone_head_(max_zone_nr * info.zone_cap),
        zone_cap_(info.zone_cap),
        lba_size_(info.lba_size),
        mdts_(info.mdts),
        channel_factory_(channel_factory),
        buffer_(0, lba_size_) {
    assert(channel_factory_ != nullptr);
    channel_factory_->Ref();
  }
  virtual ~ZnsSSTable() {
    channel_factory_->Unref();
    channel_factory_ = nullptr;
  }
  virtual Status ReadSSTable(Slice* sstable, const SSZoneMetaData& meta) = 0;
  virtual Status Get(const InternalKeyComparator& icmp, const Slice& key,
                     std::string* value, const SSZoneMetaData& meta,
                     EntryStatus* entry) = 0;
  virtual bool EnoughSpaceAvailable(const Slice& slice) const = 0;
  virtual uint64_t SpaceAvailable() const = 0;
  virtual Status InvalidateSSZone(const SSZoneMetaData& meta) = 0;
  virtual SSTableBuilder* NewBuilder(SSZoneMetaData* meta) = 0;
  virtual Status WriteSSTable(const Slice& content, SSZoneMetaData* meta) = 0;
  virtual Iterator* NewIterator(const SSZoneMetaData& meta,
                                const Comparator* cmp) = 0;
  virtual Status Recover() = 0;
  virtual uint64_t GetTail() const = 0;
  virtual uint64_t GetHead() const = 0;

  virtual ZNSDiagnostics GetDiagnostics() const = 0;

 protected:
  // const after init
  const uint64_t min_zone_head_;
  const uint64_t max_zone_head_;
  const uint64_t zone_cap_;
  const uint64_t lba_size_;
  const uint64_t mdts_;
  // references
  SZD::SZDChannelFactory* channel_factory_;
  SZD::SZDBuffer buffer_;
};

}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
