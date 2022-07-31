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
  for (size_t i = 0; i < ZnsConfig::lower_concurrency; i++) {
    sstable_level_[i] = new L0ZnsSSTable(channel_factory_, info,
                                         ranges[i].first, ranges[i].second);
  }
  sstable_level_[ZnsConfig::lower_concurrency] = new LNZnsSSTable(
      channel_factory_, info, ranges[ZnsConfig::lower_concurrency].first,
      ranges[ZnsConfig::lower_concurrency].second);

  // Increase ranges
  for (size_t i = 0; i < ZnsConfig::lower_concurrency; i++) {
    ranges_[i] = std::make_pair(ranges_[i].first * info.zone_cap,
                                ranges_[i].second * info.zone_cap);
  }
  ranges_[ZnsConfig::lower_concurrency] = std::make_pair(
      ranges_[ZnsConfig::lower_concurrency].first * info.zone_cap,
      ranges_[ZnsConfig::lower_concurrency].second * info.zone_cap);
}

ZNSSSTableManager::~ZNSSSTableManager() {
  // printf("Deleting SSTable manager.\n");
  for (size_t i = 0; i < 1 + ZnsConfig::lower_concurrency; i++) {
    if (sstable_level_[i] != nullptr) delete sstable_level_[i];
  }
  channel_factory_->Unref();
  channel_factory_ = nullptr;
}

Status ZNSSSTableManager::FlushMemTable(ZNSMemTable* mem,
                                        std::vector<SSZoneMetaData>& metas,
                                        uint8_t parallel_number) const {
  Status s = GetL0SSTableLog(parallel_number)
                 ->FlushMemTable(mem, metas, parallel_number);
  return s;
}

Status ZNSSSTableManager::WriteSSTable(const uint8_t level,
                                       const Slice& content,
                                       SSZoneMetaData* meta) const {
  assert(level < ZnsConfig::level_count);
  if (level == 0) {
    return sstable_level_[meta->L0.log_number]->WriteSSTable(content, meta);
  } else {
    return sstable_level_[ZnsConfig::lower_concurrency]->WriteSSTable(content,
                                                                      meta);
  }
}

Status ZNSSSTableManager::CopySSTable(const uint8_t level1,
                                      const uint8_t level2,
                                      const SSZoneMetaData& meta,
                                      SSZoneMetaData* new_meta) const {
  Status s = Status::OK();
  // We do not have to rewrite, all tables are in LN!!!
  if (level1 != 0) {
    *new_meta = SSZoneMetaData::copy(meta);
    return s;
  } else {
    // Read and copy to LN
    Slice original;
    s = ReadSSTable(level1, &original, meta);
    if (!s.ok() || original.size() == 0) {
      return s;
    }
    *new_meta = SSZoneMetaData::copy(meta);

    s = sstable_level_[level2]->WriteSSTable(original, new_meta);
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

Status ZNSSSTableManager::InvalidateSSZone(const uint8_t level,
                                           const SSZoneMetaData& meta) const {
  if (level == 0) {
    return sstable_level_[meta.L0.log_number]->InvalidateSSZone(meta);
  } else {
    assert(level < ZnsConfig::level_count);
    return sstable_level_[ZnsConfig::lower_concurrency]->InvalidateSSZone(meta);
  }
}

Status ZNSSSTableManager::DeleteL0Table(
    const std::vector<SSZoneMetaData*>& metas,
    std::vector<SSZoneMetaData*>& remaining_metas) const {
  // Error handling
  Status s = Status::OK();
  for (size_t i = 0; i < ZnsConfig::lower_concurrency; i++) {
    std::vector<SSZoneMetaData*> metas_for_log;
    for (auto& m : metas) {
      if (m->L0.log_number == i) {
        metas_for_log.push_back(m);
      }
    }
    if (metas_for_log.size() == 0) {
      continue;
    }
    s = static_cast<L0ZnsSSTable*>(sstable_level_[i])
            ->TryInvalidateSSZones(metas_for_log, remaining_metas);
  }
  return s;
}

Status ZNSSSTableManager::DeleteLNTable(const uint8_t level,
                                        const SSZoneMetaData& meta) const {
  if (level == 0) {
    printf("Invalid level for LN delete\n");
    return Status::InvalidArgument();
  }
  Status s =
      sstable_level_[ZnsConfig::lower_concurrency]->InvalidateSSZone(meta);
  return s;
}

Status ZNSSSTableManager::Get(const uint8_t level,
                              const InternalKeyComparator& icmp,
                              const Slice& key_ptr, std::string* value_ptr,
                              const SSZoneMetaData& meta,
                              EntryStatus* status) const {
  assert(level < ZnsConfig::level_count);
  if (level == 0) {
    return sstable_level_[meta.L0.log_number]->Get(icmp, key_ptr, value_ptr,
                                                   meta, status);
  } else {
    return sstable_level_[ZnsConfig::lower_concurrency]->Get(
        icmp, key_ptr, value_ptr, meta, status);
  }
}

Status ZNSSSTableManager::ReadSSTable(const uint8_t level, Slice* sstable,
                                      const SSZoneMetaData& meta) const {
  assert(level < ZnsConfig::level_count);
  if (level == 0) {
    return sstable_level_[meta.L0.log_number]->ReadSSTable(sstable, meta);
  } else {
    return sstable_level_[ZnsConfig::lower_concurrency]->ReadSSTable(sstable,
                                                                     meta);
  }
}

L0ZnsSSTable* ZNSSSTableManager::GetL0SSTableLog(
    uint8_t parallel_number) const {
  return static_cast<L0ZnsSSTable*>(sstable_level_[parallel_number]);
}

Iterator* ZNSSSTableManager::NewIterator(const uint8_t level,
                                         const SSZoneMetaData& meta,
                                         const Comparator* cmp) const {
  assert(level < ZnsConfig::level_count);
  if (level == 0) {
    return sstable_level_[meta.L0.log_number]->NewIterator(meta, cmp);
  } else {
    return sstable_level_[ZnsConfig::lower_concurrency]->NewIterator(meta, cmp);
  }
}

SSTableBuilder* ZNSSSTableManager::NewBuilder(const uint8_t level,
                                              SSZoneMetaData* meta) const {
  assert(level < ZnsConfig::level_count);
  if (level == 0) {
    return sstable_level_[meta->L0.log_number]->NewBuilder(meta);
  } else {
    return level == 1
               ? sstable_level_[ZnsConfig::lower_concurrency]->NewBuilder(meta)
               : static_cast<LNZnsSSTable*>(
                     sstable_level_[ZnsConfig::lower_concurrency])
                     ->NewLNBuilder(meta);
  }
}

Status ZNSSSTableManager::Recover() {
  Status s = Status::OK();
  // Recover L0
  for (size_t i = 0; i < ZnsConfig::lower_concurrency; i++) {
    s = static_cast<L0ZnsSSTable*>(sstable_level_[i])->Recover();
    if (!s.ok()) {
      printf("Error recovering L0-%lu\n", i);
      return s;
    }
  }
  return s;
}

Status ZNSSSTableManager::Recover(const std::string& frag) {
  Status s = Recover();
  if (!s.ok()) {
    return s;
  }
  // Recover LN
  s = static_cast<LNZnsSSTable*>(sstable_level_[ZnsConfig::lower_concurrency])
          ->Recover(frag);
  if (!s.ok()) {
    printf("Error recovering LN\n");
    return s;
  }

  return s;
}

std::string ZNSSSTableManager::GetFragmentedLogData() {
  assert(level > 0);  // L0 does not work!!
  LNZnsSSTable* table =
      static_cast<LNZnsSSTable*>(sstable_level_[ZnsConfig::lower_concurrency]);
  return table->Encode();
}

double ZNSSSTableManager::GetFractionFilledL0(
    const uint8_t parallel_number) const {
  assert(level < ZnsConfig::level_count);
  uint64_t space_available = 0;
  uint64_t total_space = 0;

  space_available +=
      sstable_level_[parallel_number]->SpaceAvailable() / lba_size_;
  total_space +=
      ranges_[parallel_number].second - ranges_[parallel_number].first;

  double fract = (double)(total_space - space_available) / (double)total_space;
  // printf("Space available %lu, Total space %lu, fraction %f\n",
  // space_available,
  //        total_space, fract);
  return fract;
}

double ZNSSSTableManager::GetFractionFilled(const uint8_t level) const {
  assert(level < ZnsConfig::level_count);
  uint64_t space_available = 0;
  uint64_t total_space = 0;
  if (level == 0) {
    for (size_t i = 0; i < ZnsConfig::lower_concurrency; i++) {
      space_available += sstable_level_[i]->SpaceAvailable() / lba_size_;
      total_space += ranges_[i].second - ranges_[i].first;
    }
  } else {
    space_available =
        sstable_level_[ZnsConfig::lower_concurrency]->SpaceAvailable() /
        lba_size_;
    total_space = ranges_[ZnsConfig::lower_concurrency].second -
                  ranges_[ZnsConfig::lower_concurrency].first;
  }
  double fract = (double)(total_space - space_available) / (double)total_space;
  // printf("Space available %lu, Total space %lu, fraction %f\n",
  // space_available,
  //        total_space, fract);
  return fract;
}

bool ZNSSSTableManager::EnoughSpaceAvailable(const uint8_t level,
                                             const Slice& slice) const {
  assert(level < ZnsConfig::level_count);
  if (level == 0) {
    // TODO: Not used for L0 so not tested
    for (size_t i = 0; i < ZnsConfig::lower_concurrency; i++) {
      if (!sstable_level_[i]->EnoughSpaceAvailable(slice)) {
        return false;
      }
    }
    return true;
  } else {
    return sstable_level_[ZnsConfig::lower_concurrency]->EnoughSpaceAvailable(
        slice);
  }
}

uint64_t ZNSSSTableManager::SpaceRemainingInBytesL0(
    uint8_t parallel_number) const {
  assert(level < ZnsConfig::level_count);
  return sstable_level_[parallel_number]->SpaceAvailable();
}

// TODO: investigate
uint64_t ZNSSSTableManager::SpaceRemainingL0(uint8_t parallel_number) const {
  assert(level < ZnsConfig::level_count);
  return SpaceRemainingInBytesL0(parallel_number) / lba_size_;
}

uint64_t ZNSSSTableManager::SpaceRemainingInBytesLN() const {
  return sstable_level_[ZnsConfig::lower_concurrency]->SpaceAvailable();
}

uint64_t ZNSSSTableManager::SpaceRemainingLN() const {
  return SpaceRemainingInBytesLN() / lba_size_;
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
  for (size_t i = 0; i < ZnsConfig::lower_concurrency; i++) {
    ZNSDiagnostics diag = sstable_level_[i]->GetDiagnostics();
    diag.name_ = "L0-" + std::to_string(i);
    diags.push_back(diag);
  }
  ZNSDiagnostics diag =
      sstable_level_[ZnsConfig::lower_concurrency]->GetDiagnostics();
  diag.name_ = "LN";
  diags.push_back(diag);

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
  zone_step /= ZnsConfig::lower_concurrency;
  for (size_t i = 0; i < ZnsConfig::lower_concurrency; i++) {
    ranges[i] = std::make_pair(zone_head, zone_head + zone_step);
    zone_head += zone_step;
  }
  // Last zone will also get the remainer.
  zone_step = max_zone - zone_head;
  ranges[ZnsConfig::lower_concurrency] =
      std::make_pair(zone_head, zone_head + zone_step);

  for (size_t i = 0; i < ZnsConfig::lower_concurrency; i++) {
    std::cout << std::left << "L0-" << i << std::setw(13) << "" << std::right
              << std::setw(25) << ranges[i].first << std::setw(25)
              << ranges[i].second << "\n";
  }
  std::cout << std::left << "LN" << std::setw(13) << "" << std::right
            << std::setw(25) << ranges[1].first << std::setw(25)
            << ranges[1].second << "\n";
  zone_head += zone_step;
  assert(zone_head == max_zone);
  // Create
  return new ZNSSSTableManager(channel_factory, info, ranges);
}  // namespace ROCKSDB_NAMESPACE

}  // namespace ROCKSDB_NAMESPACE
