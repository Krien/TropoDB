#pragma once
#ifdef TROPODB_PLUGIN_ENABLED
#ifndef ZNS_SSTABLE_MANAGER_H
#define ZNS_SSTABLE_MANAGER_H

#include <optional>

#include "db/tropodb/tropodb_config.h"
#include "db/tropodb/utils/tropodb_diagnostics.h"
#include "db/tropodb/io/szd_port.h"
#include "db/tropodb/memtable/tropodb_memtable.h"
#include "db/tropodb/ref_counter.h"
#include "db/tropodb/table/tropodb_l0_sstable.h"
#include "db/tropodb/table/tropodb_ln_sstable.h"
#include "db/tropodb/table/tropodb_sstable.h"
#include "db/tropodb/table/tropodb_zonemetadata.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
class ZnsSSTableManagerInternal;
class ZNSSSTableManager : public RefCounter {
 public:
  static std::optional<ZNSSSTableManager*> NewZNSSTableManager(
      SZD::SZDChannelFactory* channel_factory, const SZD::DeviceInfo& info,
      const uint64_t min_zone, const uint64_t max_zone);
  static size_t FindSSTableIndex(const Comparator* cmp,
                                 const std::vector<SSZoneMetaData*>& ss,
                                 const Slice& key);
  ~ZNSSSTableManager();

  // Reading
  Status Get(const uint8_t level, const InternalKeyComparator& icmp,
             const Slice& key, std::string* value, const SSZoneMetaData& meta,
             EntryStatus* entry) const;
  Iterator* NewIterator(const uint8_t level, const SSZoneMetaData& meta,
                        const Comparator* cmp) const;
 
  // Used for persistency
  Status Recover(const std::string& recovery_data);
  std::string GetRecoveryData();
  
  // Compaction
  SSTableBuilder* NewSSTableBuilder(const uint8_t level, SSZoneMetaData* meta) const;
  Status CopySSTable(const uint8_t level1, const uint8_t level2,
                     const SSZoneMetaData& meta,
                     SSZoneMetaData* new_meta) const;
  double GetFractionFilled(const uint8_t level) const;
  bool EnoughSpaceAvailable(const uint8_t level, const Slice& slice) const;

  // L0 specific
  L0ZnsSSTable* GetL0SSTableLog(uint8_t parallel_number) const;
  Status FlushMemTable(ZNSMemTable* mem, std::vector<SSZoneMetaData>& metas,
                       uint8_t parallel_number) const;
  Status DeleteL0Table(const std::vector<SSZoneMetaData*>& metas_to_delete,
                       std::vector<SSZoneMetaData*>& remaining_metas) const;
  double GetFractionFilledL0(const uint8_t parallel_number) const;
  uint64_t SpaceRemainingL0(uint8_t parallel_number) const;
  uint64_t SpaceRemainingInBytesL0(uint8_t parallel_number) const;
  
  // LN specific
  Iterator* GetLNIterator(const Slice& file_value,
                                       const Comparator* cmp);
  Status DeleteLNTable(const uint8_t level, const SSZoneMetaData& meta) const;
  uint64_t SpaceRemainingLN() const;
  uint64_t SpaceRemainingInBytesLN() const;
  
  // util
  uint64_t GetBytesInLevel(const std::vector<SSZoneMetaData*>& metas);
  std::vector<ZNSDiagnostics> IODiagnostics();
  std::string LayoutDivisionString();

 private:
  using RangeArray = std::array<std::pair<uint64_t, uint64_t>,
                                1 + ZnsConfig::lower_concurrency>;
  using SSTableArray =
      std::array<ZnsSSTable*, 1 + ZnsConfig::lower_concurrency>;

  ZNSSSTableManager(SZD::SZDChannelFactory* channel_factory,
                    const SZD::DeviceInfo& info, const RangeArray& ranges);

   // Recovery
   Status RecoverL0();
   Status RecoverLN(const std::string& recovery_data);
   // Reads
   Status ReadSSTable(const uint8_t level, Slice* sstable,
                     const SSZoneMetaData& meta) const;

  // ZNS
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
