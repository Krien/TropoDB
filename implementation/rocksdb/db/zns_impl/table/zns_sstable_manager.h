#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_SSTABLE_MANAGER_H
#define ZNS_SSTABLE_MANAGER_H

#include <optional>

#include "db/zns_impl/config.h"
#include "db/zns_impl/utils/tropodb_diagnostics.h"
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
  Status FlushMemTable(ZNSMemTable* mem, std::vector<SSZoneMetaData>& metas,
                       uint8_t parallel_number) const;
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
  Status DeleteLNTable(const uint8_t level, const SSZoneMetaData& meta) const;
  L0ZnsSSTable* GetL0SSTableLog(uint8_t parallel_number) const;
  Iterator* NewIterator(const uint8_t level, const SSZoneMetaData& meta,
                        const Comparator* cmp) const;
  SSTableBuilder* NewBuilder(const uint8_t level, SSZoneMetaData* meta) const;
  // Used for persistency
  Status Recover();
  Status Recover(const std::string& frag);
  std::string GetFragmentedLogData();
  // Used for compaction
  double GetFractionFilledL0(const uint8_t parallel_number) const;
  double GetFractionFilled(const uint8_t level) const;
  uint64_t SpaceRemainingL0(uint8_t parallel_number) const;
  uint64_t SpaceRemainingInBytesL0(uint8_t parallel_number) const;
  uint64_t SpaceRemainingLN() const;
  uint64_t SpaceRemainingInBytesLN() const;

  // util
  uint64_t GetBytesInLevel(const std::vector<SSZoneMetaData*>& metas);
  static size_t FindSSTableIndex(const Comparator* cmp,
                                 const std::vector<SSZoneMetaData*>& ss,
                                 const Slice& key);
  std::vector<ZNSDiagnostics> IODiagnostics();

 private:
  using RangeArray = std::array<std::pair<uint64_t, uint64_t>,
                                1 + ZnsConfig::lower_concurrency>;
  using SSTableArray =
      std::array<ZnsSSTable*, 1 + ZnsConfig::lower_concurrency>;

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
