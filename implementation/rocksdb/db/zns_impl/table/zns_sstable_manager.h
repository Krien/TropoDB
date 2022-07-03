#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_SSTABLE_MANAGER_H
#define ZNS_SSTABLE_MANAGER_H

#include "db/zns_impl/config.h"
#include "db/zns_impl/diagnostics.h"
#include "db/zns_impl/io/szd_port.h"
#include "db/zns_impl/memtable/zns_memtable.h"
#include "db/zns_impl/ref_counter.h"
#include "db/zns_impl/table/l0_zns_sstable.h"
#include "db/zns_impl/table/ln_zns_sstable.h"
#include "db/zns_impl/table/zns_sstable.h"
#include "db/zns_impl/table/zns_zonemetadata.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
class ZnsSSTableManagerInternal;
class ZNSSSTableManager : public RefCounter {
 public:
  static std::optional<ZNSSSTableManager*> NewZNSSTableManager(
      SZD::SZDChannelFactory* channel_factory, const SZD::DeviceInfo& info,
      const uint64_t min_zone, const uint64_t max_zone);

  ~ZNSSSTableManager();

  bool EnoughSpaceAvailable(const uint8_t level, const Slice& slice) const;
  Status FlushMemTable(ZNSMemTable* mem, SSZoneMetaData* meta) const;
  Status CopySSTable(const uint8_t level1, const uint8_t level2,
                     const SSZoneMetaData& meta,
                     SSZoneMetaData* new_meta) const;
  Status WriteSSTable(const uint8_t level, const Slice& content,
                      SSZoneMetaData* meta) const;
  Status ReadSSTable(const uint8_t level, Slice* sstable,
                     const SSZoneMetaData& meta) const;
  Status Get(const uint8_t level, const InternalKeyComparator& icmp,
             const Slice& key, std::string* value, const SSZoneMetaData& meta,
             EntryStatus* entry) const;
  Status InvalidateSSZone(const uint8_t level,
                          const SSZoneMetaData& meta) const;
  Status DeleteL0Table(const std::vector<SSZoneMetaData*>& metas,
                       std::vector<SSZoneMetaData*>& remaining_metas) const;
  Status SetValidRangeAndReclaim(uint64_t* live_tail, uint64_t* blocks,
                                 uint64_t blocks_to_delete) const;
  Status DeleteLNTable(const uint8_t level, const SSZoneMetaData& meta) const;
  L0ZnsSSTable* GetL0SSTableLog() const;
  Iterator* NewIterator(const uint8_t level, const SSZoneMetaData& meta,
                        const Comparator* cmp) const;
  SSTableBuilder* NewBuilder(const uint8_t level, SSZoneMetaData* meta) const;
  // Used for persistency
  Status Recover(const std::vector<std::pair<uint8_t, std::string>>& frag);
  std::string GetFragmentedLogData(const uint8_t level);
  // Used for compaction
  double GetFractionFilled(const uint8_t level) const;
  uint64_t SpaceRemaining(const uint8_t level) const;
  uint64_t SpaceRemainingInBytes(const uint8_t level) const;

  // Used for cleaning
  void GetDefaultRange(const uint8_t level,
                       std::pair<uint64_t, uint64_t>* range) const;
  void GetRange(const uint8_t level, const std::vector<SSZoneMetaData*>& metas,
                std::pair<uint64_t, uint64_t>* range) const;
  // Utils
  static size_t FindSSTableIndex(const Comparator* icmp,
                                 const std::vector<SSZoneMetaData*>& ss,
                                 const Slice& key);
  std::vector<ZNSDiagnostics> IODiagnostics();

 private:
  using RangeArray =
      std::array<std::pair<uint64_t, uint64_t>, ZnsConfig::level_count>;
  using SSTableArray = std::array<ZnsSSTable*, ZnsConfig::level_count>;

  ZNSSSTableManager(SZD::SZDChannelFactory* channel_factory,
                    const SZD::DeviceInfo& info, const RangeArray& ranges);
  const uint64_t zone_cap_;
  const uint64_t lba_size_;
  // sstables
  RangeArray ranges_;
  SSTableArray sstable_level_;
  // references
  SZD::SZDChannelFactory* channel_factory_;
};
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
