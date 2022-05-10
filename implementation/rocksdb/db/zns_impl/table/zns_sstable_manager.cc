#include "db/zns_impl/table/zns_sstable_manager.h"

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

ZNSSSTableManager* ZNSSSTableManager::NewZNSSTableManager(
    SZD::SZDChannelFactory* channel_factory, const SZD::DeviceInfo& info,
    const uint64_t min_zone, const uint64_t max_zone) {
  uint64_t zone_head = min_zone;
  uint64_t zone_step;
  uint64_t num_zones = max_zone - min_zone;
  std::array<std::pair<uint64_t, uint64_t>, ZnsConfig::level_count> ranges;
  // Validate
  if (min_zone > max_zone || num_zones < ZnsConfig::level_count * 2) {
    return nullptr;
  }
  // Distribute
  uint64_t distr =
      std::accumulate(ZnsConfig::ss_distribution,
                      ZnsConfig::ss_distribution + ZnsConfig::level_count, 0U);
  for (size_t i = 0; i < ZnsConfig::level_count - 1; i++) {
    zone_step = (num_zones / distr) * ZnsConfig::ss_distribution[i];
    zone_step = zone_step < ZnsConfig::min_ss_zone_count
                    ? ZnsConfig::min_ss_zone_count
                    : zone_step;
    ranges[i] = std::make_pair(zone_head, zone_head + zone_step);
    printf("ranges %lu %lu \n", ranges[i].first, ranges[i].second);
    zone_head += zone_step;
  }
  zone_step = info.max_lba / info.zone_size - zone_head;
  ranges[ZnsConfig::level_count - 1] =
      std::make_pair(zone_head, zone_head + zone_step);
  printf("ranges %lu %lu \n", ranges[ZnsConfig::level_count - 1].first,
         ranges[ZnsConfig::level_count - 1].second);
  // Create
  return new ZNSSSTableManager(channel_factory, info, ranges);
}

ZNSSSTableManager::ZNSSSTableManager(
    SZD::SZDChannelFactory* channel_factory, const SZD::DeviceInfo& info,
    const std::array<std::pair<uint64_t, uint64_t>, ZnsConfig::level_count>&
        ranges)
    : channel_factory_(channel_factory), ranges_(ranges) {
  assert(channel_factory_ != nullptr);
  channel_factory_->Ref();
  // Create tables
  sstable_wal_level_[0] = new L0ZnsSSTable(channel_factory_, info,
                                           ranges[0].first, ranges[0].second);
  for (size_t level = 1; level < ZnsConfig::level_count; level++) {
    sstable_wal_level_[level] = new LNZnsSSTable(
        channel_factory_, info, ranges[level].first, ranges[level].second);
  }
  // Increase ranges
  for (size_t level = 0; level < ZnsConfig::level_count; level++) {
    ranges_[level].first *= info.zone_size;
    ranges_[level].second *= info.zone_size;
  }
}

ZNSSSTableManager::~ZNSSSTableManager() {
  // printf("Deleting SSTable manager.\n");
  for (size_t i = 0; i < ZnsConfig::level_count; i++) {
    if (sstable_wal_level_[i] != nullptr) delete sstable_wal_level_[i];
  }
  channel_factory_->Unref();
  channel_factory_ = nullptr;
}

Status ZNSSSTableManager::FlushMemTable(ZNSMemTable* mem,
                                        SSZoneMetaData* meta) const {
  return GetL0SSTableLog()->FlushMemTable(mem, meta);
}

Status ZNSSSTableManager::WriteSSTable(const uint8_t level,
                                       const Slice& content,
                                       SSZoneMetaData* meta) const {
  assert(level < ZnsConfig::level_count);
  return sstable_wal_level_[level]->WriteSSTable(content, meta);
}

Status ZNSSSTableManager::CopySSTable(const uint8_t level1,
                                      const uint8_t level2,
                                      SSZoneMetaData* meta) const {
  Status s;
  Slice original;
  SSZoneMetaData tmp(*meta);
  s = ReadSSTable(level1, &original, &tmp);
  if (!s.ok()) {
    return s;
  }
  s = sstable_wal_level_[level2]->WriteSSTable(original, meta);
  delete[] original.data();
  return s;
}

bool ZNSSSTableManager::EnoughSpaceAvailable(const uint8_t level,
                                             const Slice& slice) const {
  assert(level < ZnsConfig::level_count);
  return sstable_wal_level_[level]->EnoughSpaceAvailable(slice);
}

Status ZNSSSTableManager::InvalidateSSZone(const uint8_t level,
                                           const SSZoneMetaData& meta) const {
  assert(level < ZnsConfig::level_count);
  return sstable_wal_level_[level]->InvalidateSSZone(meta);
}

Status ZNSSSTableManager::SetValidRangeAndReclaim(
    const uint8_t level, const uint64_t live_tail,
    const uint64_t live_head) const {
  assert(level < ZnsConfig::level_count);
  SSZoneMetaData meta;
  uint64_t written_tail = sstable_wal_level_[level]->GetTail();
  if (live_tail == written_tail) {
    return Status::OK();
  } else if (live_tail < written_tail) {
    meta.lba = written_tail;
    meta.lba_count = ranges_[level].second - written_tail;
    printf("Invalidating data from %u %lu %lu \n", level, meta.lba,
           meta.lba_count);
    Status s = sstable_wal_level_[level]->InvalidateSSZone(meta);
    if (!s.ok()) return s;
    meta.lba = ranges_[level].first;
    meta.lba_count = live_tail - meta.lba;
    if (meta.lba_count == 0) {
      return Status::OK();
    }
    s = sstable_wal_level_[level]->InvalidateSSZone(meta);
    printf("Invalidating data from %u %lu %lu %f\n", level, meta.lba,
           meta.lba_count, GetFractionFilled(level));
    return s;
  } else {
    meta.lba = written_tail;
    meta.lba_count = live_tail - written_tail;
    Status s = sstable_wal_level_[level]->InvalidateSSZone(meta);
    printf("Invalidating data from %u %lu %lu %f\n", level, meta.lba,
           meta.lba_count, GetFractionFilled(level));
    return s;
  }
}

Status ZNSSSTableManager::Get(const uint8_t level,
                              const InternalKeyComparator& icmp,
                              const Slice& key_ptr, std::string* value_ptr,
                              const SSZoneMetaData& meta,
                              EntryStatus* status) const {
  assert(level < ZnsConfig::level_count);
  return sstable_wal_level_[level]->Get(icmp, key_ptr, value_ptr, meta, status);
}

Status ZNSSSTableManager::ReadSSTable(const uint8_t level, Slice* sstable,
                                      const SSZoneMetaData& meta) const {
  assert(level < ZnsConfig::level_count);
  return sstable_wal_level_[level]->ReadSSTable(sstable, meta);
}

L0ZnsSSTable* ZNSSSTableManager::GetL0SSTableLog() const {
  return dynamic_cast<L0ZnsSSTable*>(sstable_wal_level_[0]);
}

Iterator* ZNSSSTableManager::NewIterator(
    const uint8_t level, const SSZoneMetaData& meta,
    const InternalKeyComparator& icmp) const {
  assert(level < ZnsConfig::level_count);
  return sstable_wal_level_[level]->NewIterator(meta, icmp);
}

SSTableBuilder* ZNSSSTableManager::NewBuilder(const uint8_t level,
                                              SSZoneMetaData* meta) const {
  assert(level < ZnsConfig::level_count);
  return sstable_wal_level_[level]->NewBuilder(meta);
}

Status ZNSSSTableManager::Recover() {
  Status s = Status::OK();
  for (size_t i = 0; i < ZnsConfig::level_count; i++) {
    if (!(s = sstable_wal_level_[i]->Recover()).ok()) {
      return s;
    }
  }
  return s;
}

double ZNSSSTableManager::GetFractionFilled(const uint8_t level) const {
  assert(level < ZnsConfig::level_count);
  uint64_t head = sstable_wal_level_[level]->GetHead();
  uint64_t tail = sstable_wal_level_[level]->GetTail();
  uint64_t sum =
      head >= tail
          ? (head - tail)
          : (ranges_[level].second - tail + head - ranges_[level].first);
  double fract = (double)sum /
                 ((double)ranges_[level].second - (double)ranges_[level].first);
  return fract;
}

void ZNSSSTableManager::GetDefaultRange(
    const uint8_t level, std::pair<uint64_t, uint64_t>* range) const {
  *range = std::make_pair(ranges_[level].first, ranges_[level].first);
}

void ZNSSSTableManager::GetRange(const uint8_t level,
                                 const std::vector<SSZoneMetaData*>& metas,
                                 std::pair<uint64_t, uint64_t>* range) const {
  // if (metas.size() > 1) {
  //   *range = std::make_pair(metas[metas.size()-1]->lba,
  //   metas[0]->lba+metas[0]->lba_count);
  // }
  uint64_t lowest = 0, lowest_res = ranges_[level].first;
  uint64_t highest = 0, highest_res = ranges_[level].second;
  bool first = false;

  for (auto n = metas.begin(); n != metas.end(); n++) {
    if (!first) {
      lowest = (*n)->number;
      lowest_res = (*n)->lba;
      highest = (*n)->number;
      highest_res = (*n)->lba + (*n)->lba_count;
      first = true;
    }
    if (lowest > (*n)->number) {
      lowest = (*n)->number;
      lowest_res = (*n)->lba;
    }
    if (highest < (*n)->number) {
      highest = (*n)->number;
      highest_res = (*n)->lba + (*n)->lba_count;
    }
  }
  if (first) {
    *range = std::make_pair(lowest_res, highest_res);
  } else {
    *range = std::make_pair(ranges_[level].first, ranges_[level].first);
  }
  // TODO: higher levels will need more info.
}

size_t ZNSSSTableManager::FindSSTableIndex(
    const InternalKeyComparator& icmp, const std::vector<SSZoneMetaData*>& ss,
    const Slice& key) {
  size_t left = 0;
  size_t right = ss.size();
  // binary search I guess.
  while (left < right) {
    size_t mid = (left + right) / 2;
    const SSZoneMetaData* m = ss[mid];
    if (icmp.InternalKeyComparator::Compare(m->largest.Encode(), key) < 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  return right;
}

}  // namespace ROCKSDB_NAMESPACE
