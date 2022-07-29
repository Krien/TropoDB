#include "db/zns_impl/table/zns_sstable_manager.h"

#include <iomanip>
#include <iostream>

#include "db/zns_impl/config.h"
#include "db/zns_impl/io/szd_port.h"
#include "db/zns_impl/memtable/zns_memtable.h"
#include "db/zns_impl/table/l0_zns_sstable.h"
#include "db/zns_impl/table/ln_zns_sstable.h"
#include "db/zns_impl/table/zns_sstable.h"
#include "db/zns_impl/table/zns_zonemetadata.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "table/internal_iterator.h"

namespace ROCKSDB_NAMESPACE {
ZNSSSTableManager::ZNSSSTableManager(SZD::SZDChannelFactory* channel_factory,
                                     const SZD::DeviceInfo& info,
                                     const RangeArray& ranges)
    : zone_cap_(info.zone_cap),
      lba_size_(info.lba_size),
      ranges_(ranges),
      channel_factory_(channel_factory) {
  assert(channel_factory_ != nullptr);
  channel_factory_->Ref();
  // Create tables
  sstable_level_[0] = new L0ZnsSSTable(channel_factory_, info, ranges[0].first,
                                       ranges[0].second);
  sstable_level_[1] = new LNZnsSSTable(channel_factory_, info, ranges[1].first,
                                       ranges[ranges.size() - 1].second);
  // Increase ranges
  ranges_[0] = std::make_pair(ranges_[0].first * info.zone_cap,
                              ranges_[0].second * info.zone_cap);
  ranges_[1] =
      std::make_pair(ranges_[1].first * info.zone_cap,
                     ranges_[ranges.size() - 1].second * info.zone_cap);
}

ZNSSSTableManager::~ZNSSSTableManager() {
  // printf("Deleting SSTable manager.\n");
  for (size_t i = 0; i < 2; i++) {
    if (sstable_level_[i] != nullptr) delete sstable_level_[i];
  }
  channel_factory_->Unref();
  channel_factory_ = nullptr;
}

Status ZNSSSTableManager::FlushMemTable(
    ZNSMemTable* mem, std::vector<SSZoneMetaData>& metas) const {
  return GetL0SSTableLog()->FlushMemTable(mem, metas);
}

Status ZNSSSTableManager::WriteSSTable(const uint8_t level,
                                       const Slice& content,
                                       SSZoneMetaData* meta) const {
  assert(level < ZnsConfig::level_count);
  return sstable_level_[level ? 1 : 0]->WriteSSTable(content, meta);
}

Status ZNSSSTableManager::CopySSTable(const uint8_t level1,
                                      const uint8_t level2,
                                      const SSZoneMetaData& meta,
                                      SSZoneMetaData* new_meta) const {
  Status s = Status::OK();
  // We do not have to rewrite, all tables are in LN!!!
  if (level1 != 0) {
    *new_meta = SSZoneMetaData(meta);
    return s;
  } else {
    // Read and copy to LN
    Slice original;
    s = ReadSSTable(level1 ? 1 : 0, &original, meta);
    if (!s.ok() || original.size() == 0) {
      return s;
    }
    *new_meta = SSZoneMetaData(meta);

    s = sstable_level_[level2 ? 1 : 0]->WriteSSTable(original, new_meta);
    delete[] original.data();
  }

  return s;
}

uint64_t ZNSSSTableManager::GetBytesInLevel(
    const std::vector<SSZoneMetaData*>& metas) {
  uint64_t total = 0;
  for (const auto& meta : metas) {
    total += meta->lba_count * lba_size_;
  }
  return total;
}

bool ZNSSSTableManager::EnoughSpaceAvailable(const uint8_t level,
                                             const Slice& slice) const {
  assert(level < ZnsConfig::level_count);
  return sstable_level_[level ? 1 : 0]->EnoughSpaceAvailable(slice);
}

Status ZNSSSTableManager::InvalidateSSZone(const uint8_t level,
                                           const SSZoneMetaData& meta) const {
  assert(level < ZnsConfig::level_count);
  return sstable_level_[level ? 1 : 0]->InvalidateSSZone(meta);
}

Status ZNSSSTableManager::DeleteL0Table(
    const std::vector<SSZoneMetaData*>& metas,
    std::vector<SSZoneMetaData*>& remaining_metas) const {
  // Error handling
  if (metas.size() == 0) {
    return Status::OK();
  }
  return static_cast<L0ZnsSSTable*>(sstable_level_[0])
      ->TryInvalidateSSZones(metas, remaining_metas);
}

Status ZNSSSTableManager::SetValidRangeAndReclaim(
    uint64_t* live_tail, uint64_t* blocks, uint64_t blocks_to_delete) const {
  // TODO: move to sstable
  SSZoneMetaData meta;
  uint64_t written_tail = sstable_level_[0]->GetTail();

  meta.L0.lba = *live_tail;
  uint64_t nexthead =
      ((meta.L0.lba + blocks_to_delete) / zone_cap_) * zone_cap_;
  meta.lba_count = nexthead - meta.L0.lba;

  Status s = Status::OK();
  if (meta.lba_count != 0) {
    // printf("test %lu %lu %lu %lu %lu\n", meta.L0.lba, meta.lba_count,
    //        written_tail, *blocks, blocks_to_delete);
    s = sstable_level_[0]->InvalidateSSZone(meta);
  }
  if (s.ok()) {
    *blocks -= meta.lba_count;
    *live_tail = sstable_level_[0]->GetTail();
    // printf("New tail %lu \n", *live_tail);
  } else {
    printf("Error reclaiming L0? %lu %lu, true %lu\n", meta.L0.lba,
           meta.lba_count, written_tail);
  }
  return s;
}

Status ZNSSSTableManager::DeleteLNTable(const uint8_t level,
                                        const SSZoneMetaData& meta) const {
  Status s = sstable_level_[level ? 1 : 0]->InvalidateSSZone(meta);
  return s;
}

Status ZNSSSTableManager::Get(const uint8_t level,
                              const InternalKeyComparator& icmp,
                              const Slice& key_ptr, std::string* value_ptr,
                              const SSZoneMetaData& meta,
                              EntryStatus* status) const {
  assert(level < ZnsConfig::level_count);
  return sstable_level_[level ? 1 : 0]->Get(icmp, key_ptr, value_ptr, meta,
                                            status);
}

Status ZNSSSTableManager::ReadSSTable(const uint8_t level, Slice* sstable,
                                      const SSZoneMetaData& meta) const {
  assert(level < ZnsConfig::level_count);
  return sstable_level_[level ? 1 : 0]->ReadSSTable(sstable, meta);
}

L0ZnsSSTable* ZNSSSTableManager::GetL0SSTableLog() const {
  return static_cast<L0ZnsSSTable*>(sstable_level_[0]);
}

Iterator* ZNSSSTableManager::NewIterator(const uint8_t level,
                                         const SSZoneMetaData& meta,
                                         const Comparator* cmp) const {
  assert(level < ZnsConfig::level_count);
  return sstable_level_[level ? 1 : 0]->NewIterator(meta, cmp);
}

SSTableBuilder* ZNSSSTableManager::NewBuilder(const uint8_t level,
                                              SSZoneMetaData* meta) const {
  assert(level < ZnsConfig::level_count);
  if (level > 1) {
    return static_cast<LNZnsSSTable*>(sstable_level_[level ? 1 : 0])
        ->NewLNBuilder(meta);
  } else {
    return sstable_level_[level ? 1 : 0]->NewBuilder(meta);
  }
}

Status ZNSSSTableManager::Recover() {
  Status s = Status::OK();
  // Recover L0
  s = static_cast<L0ZnsSSTable*>(sstable_level_[0])->Recover();
  if (!s.ok()) {
    printf("Error recovering L0\n");
  }
  return s;
}

Status ZNSSSTableManager::Recover(const std::string& frag) {
  Status s = Recover();
  if (!s.ok()) {
    return s;
  }
  // Recover LN
  s = static_cast<LNZnsSSTable*>(sstable_level_[1])->Recover(frag);
  if (!s.ok()) {
    printf("Error recovering > L%u \n", 1);
    return s;
  }

  return s;
}

std::string ZNSSSTableManager::GetFragmentedLogData() {
  assert(level > 0);  // L0 does not work!!
  LNZnsSSTable* table = static_cast<LNZnsSSTable*>(sstable_level_[1]);
  return table->Encode();
}

double ZNSSSTableManager::GetFractionFilled(const uint8_t level) const {
  assert(level < ZnsConfig::level_count);
  uint64_t space_available =
      sstable_level_[level ? 1 : 0]->SpaceAvailable() / lba_size_;
  uint64_t total_space =
      ranges_[level ? 1 : 0].second - ranges_[level ? 1 : 0].first;

  double fract = (double)(total_space - space_available) / (double)total_space;
  // printf("Space available %lu, Total space %lu, fraction %f\n",
  // space_available,
  //        total_space, fract);
  return fract;
}

uint64_t ZNSSSTableManager::SpaceRemainingInBytes(const uint8_t level) const {
  assert(level < ZnsConfig::level_count);
  return sstable_level_[level ? 1 : 0]->SpaceAvailable();
}

uint64_t ZNSSSTableManager::SpaceRemaining(const uint8_t level) const {
  assert(level < ZnsConfig::level_count);
  return SpaceRemainingInBytes(level ? 1 : 0) / lba_size_;
}

size_t ZNSSSTableManager::FindSSTableIndex(
    const Comparator* cmp, const std::vector<SSZoneMetaData*>& ss,
    const Slice& key) {
  size_t left = 0;
  size_t right = ss.size();
  // binary search I guess.
  while (left < right) {
    size_t mid = (left + right) / 2;
    const SSZoneMetaData* m = ss[mid];
    if (cmp->Compare(m->largest.user_key(), ExtractUserKey(key)) < 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  return right;
}

std::vector<ZNSDiagnostics> ZNSSSTableManager::IODiagnostics() {
  std::vector<ZNSDiagnostics> diags;
  for (size_t level = 0; level < 2; level++) {
    ZNSDiagnostics diag = sstable_level_[level ? 1 : 0]->GetDiagnostics();
    diag.name_ = "L" + std::to_string(level);
    diags.push_back(diag);
  }
  return diags;
}

std::optional<ZNSSSTableManager*> ZNSSSTableManager::NewZNSSTableManager(
    SZD::SZDChannelFactory* channel_factory, const SZD::DeviceInfo& info,
    const uint64_t min_zone, const uint64_t max_zone) {
  uint64_t zone_head = min_zone;
  uint64_t zone_step;
  uint64_t num_zones = max_zone - min_zone;
  RangeArray ranges;
  // Validate
  if (min_zone > max_zone || num_zones < ZnsConfig::level_count * 2 ||
      channel_factory == nullptr) {
    return {};
  }
  // Distribute
  zone_step = ZnsConfig::L0_zones;
  zone_step = zone_step < ZnsConfig::min_ss_zone_count
                  ? ZnsConfig::min_ss_zone_count
                  : zone_step;
  ranges[0] = std::make_pair(zone_head, zone_head + zone_step);
  zone_head += zone_step;
  // Last zone will also get the remainer.
  zone_step = max_zone - zone_head;
  ranges[1] = std::make_pair(zone_head, zone_head + zone_step);

  std::cout << std::left << "L0" << std::setw(13) << "" << std::right
            << std::setw(25) << ranges[0].first << std::setw(25)
            << ranges[0].second << "\n";
  std::cout << std::left << "LN" << std::setw(13) << "" << std::right
            << std::setw(25) << ranges[1].first << std::setw(25)
            << ranges[1].second << "\n";
  zone_head += zone_step;
  assert(zone_head == max_zone);
  // Create
  return new ZNSSSTableManager(channel_factory, info, ranges);
}  // namespace ROCKSDB_NAMESPACE

}  // namespace ROCKSDB_NAMESPACE
