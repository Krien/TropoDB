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
ZNSSSTableManager::ZNSSSTableManager(SZD::SZDChannelFactory* channel_factory,
                                     const SZD::DeviceInfo& info,
                                     const RangeArray& ranges)
    : zone_size_(info.zone_size),
      lba_size_(info.lba_size),
      ranges_(ranges),
      channel_factory_(channel_factory) {
  assert(channel_factory_ != nullptr);
  channel_factory_->Ref();
  // Create tables
  sstable_level_[0] = new L0ZnsSSTable(channel_factory_, info, ranges[0].first,
                                       ranges[0].second);
  for (size_t level = 1; level < ZnsConfig::level_count; level++) {
    sstable_level_[level] = new LNZnsSSTable(
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
    if (sstable_level_[i] != nullptr) delete sstable_level_[i];
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
  return sstable_level_[level]->WriteSSTable(content, meta);
}

Status ZNSSSTableManager::CopySSTable(const uint8_t level1,
                                      const uint8_t level2,
                                      const SSZoneMetaData& meta,
                                      SSZoneMetaData* new_meta) const {
  Status s;
  Slice original;
  s = ReadSSTable(level1, &original, meta);
  if (!s.ok()) {
    return s;
  }
  *new_meta = SSZoneMetaData(meta);
  s = sstable_level_[level2]->WriteSSTable(original, new_meta);
  delete[] original.data();
  return s;
}

bool ZNSSSTableManager::EnoughSpaceAvailable(const uint8_t level,
                                             const Slice& slice) const {
  assert(level < ZnsConfig::level_count);
  return sstable_level_[level]->EnoughSpaceAvailable(slice);
}

Status ZNSSSTableManager::InvalidateSSZone(const uint8_t level,
                                           const SSZoneMetaData& meta) const {
  assert(level < ZnsConfig::level_count);
  return sstable_level_[level]->InvalidateSSZone(meta);
}

Status ZNSSSTableManager::SetValidRangeAndReclaim(uint64_t* live_tail,
                                                  uint64_t* blocks) const {
  // TODO: move to sstable
  SSZoneMetaData meta;
  uint64_t written_tail = sstable_level_[0]->GetTail();

  meta.L0.lba = *live_tail;
  uint64_t nexthead = ((meta.L0.lba + *blocks) / zone_size_) * zone_size_;
  meta.lba_count = nexthead - meta.L0.lba;

  printf("test %lu %lu %lu \n", meta.L0.lba, meta.lba_count, written_tail);

  Status s = Status::OK();
  if (meta.lba_count != 0) {
    s = sstable_level_[0]->InvalidateSSZone(meta);
  }
  if (s.ok()) {
    *blocks -= meta.lba_count;
    *live_tail = sstable_level_[0]->GetTail();
  }
  return s;
}

Status ZNSSSTableManager::DeleteLNTable(const uint8_t level,
                                        const SSZoneMetaData& meta) const {
  Status s = sstable_level_[level]->InvalidateSSZone(meta);
  return s;
}

Status ZNSSSTableManager::Get(const uint8_t level,
                              const InternalKeyComparator& icmp,
                              const Slice& key_ptr, std::string* value_ptr,
                              const SSZoneMetaData& meta,
                              EntryStatus* status) const {
  assert(level < ZnsConfig::level_count);
  return sstable_level_[level]->Get(icmp, key_ptr, value_ptr, meta, status);
}

Status ZNSSSTableManager::ReadSSTable(const uint8_t level, Slice* sstable,
                                      const SSZoneMetaData& meta) const {
  assert(level < ZnsConfig::level_count);
  return sstable_level_[level]->ReadSSTable(sstable, meta);
}

L0ZnsSSTable* ZNSSSTableManager::GetL0SSTableLog() const {
  return dynamic_cast<L0ZnsSSTable*>(sstable_level_[0]);
}

Iterator* ZNSSSTableManager::NewIterator(const uint8_t level,
                                         const SSZoneMetaData& meta,
                                         const Comparator* cmp) const {
  assert(level < ZnsConfig::level_count);
  return sstable_level_[level]->NewIterator(meta, cmp);
}

SSTableBuilder* ZNSSSTableManager::NewBuilder(const uint8_t level,
                                              SSZoneMetaData* meta) const {
  assert(level < ZnsConfig::level_count);
  return sstable_level_[level]->NewBuilder(meta);
}

Status ZNSSSTableManager::Recover(
    const std::vector<std::pair<uint8_t, std::string>>& frag) {
  Status s = Status::OK();
  // Recover L0
  s = dynamic_cast<L0ZnsSSTable*>(sstable_level_[0])->Recover();
  if (!s.ok()) {
    printf("Error recovering L0\n");
    return s;
  }
  // Recover LN
  for (auto fragdata : frag) {
    uint8_t level = fragdata.first;
    if (level == 0 || level > ZnsConfig::level_count) {
      return Status::Corruption();
    }
    s = dynamic_cast<LNZnsSSTable*>(sstable_level_[level])
            ->Recover(fragdata.second);
    if (!s.ok()) {
      printf("Error recovering L%u \n", level);
      return s;
    }
  }
  return s;
}

std::string ZNSSSTableManager::GetFragmentedLogData(const uint8_t level) {
  assert(level > 0);  // L0 does not work!!
  LNZnsSSTable* table = dynamic_cast<LNZnsSSTable*>(sstable_level_[level]);
  return table->Encode();
}

double ZNSSSTableManager::GetFractionFilled(const uint8_t level) const {
  assert(level < ZnsConfig::level_count);
  uint64_t space_available =
      sstable_level_[level]->SpaceAvailable() / lba_size_;
  uint64_t total_space = ranges_[level].second - ranges_[level].first;
  // printf("Space available %lu, Total space %lu \n", space_available,
  //        total_space);
  double fract = (double)(total_space - space_available) / (double)total_space;
  return fract;
}

uint64_t ZNSSSTableManager::SpaceRemaining(const uint8_t level) const {
  assert(level < ZnsConfig::level_count);
  return sstable_level_[level]->SpaceAvailable() / lba_size_;
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
  if (level != 0) {
    return;
  }
  uint64_t lowest = 0, lowest_res = ranges_[level].first;
  uint64_t highest = 0, highest_res = ranges_[level].second;
  bool first = false;

  for (auto n = metas.begin(); n != metas.end(); n++) {
    if (!first) {
      lowest = (*n)->number;
      lowest_res = (*n)->L0.lba;
      highest = (*n)->number;
      highest_res = (*n)->L0.lba + (*n)->lba_count;
      first = true;
    }
    if (lowest > (*n)->number) {
      lowest = (*n)->number;
      lowest_res = (*n)->L0.lba;
    }
    if (highest < (*n)->number) {
      highest = (*n)->number;
      highest_res = (*n)->L0.lba + (*n)->lba_count;
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
  uint64_t distr =
      std::accumulate(ZnsConfig::ss_distribution,
                      ZnsConfig::ss_distribution + ZnsConfig::level_count, 0U);
  for (size_t i = 0; i < ZnsConfig::level_count - 1; i++) {
    zone_step = (num_zones / distr) * ZnsConfig::ss_distribution[i];
    zone_step = zone_step < ZnsConfig::min_ss_zone_count
                    ? ZnsConfig::min_ss_zone_count
                    : zone_step;
    ranges[i] = std::make_pair(zone_head, zone_head + zone_step);
    zone_head += zone_step;
  }
  // Last zone will also get the remainer.
  zone_step = max_zone - zone_head;
  ranges[ZnsConfig::level_count - 1] =
      std::make_pair(zone_head, zone_head + zone_step);
  zone_head += zone_step;
  assert(zone_head == max_zone);
  // Create
  return new ZNSSSTableManager(channel_factory, info, ranges);
}

}  // namespace ROCKSDB_NAMESPACE
