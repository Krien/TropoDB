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

ZNSSSTableManager::ZNSSSTableManager(
    SZD::SZDChannelFactory* channel_factory, const SZD::DeviceInfo& info,
    const std::pair<uint64_t, uint64_t> ranges[ZnsConfig::level_count])
    : channel_factory_(channel_factory) {
  assert(channel_factory_ != nullptr);
  channel_factory_->Ref();
  sstable_wal_level_[0] = new L0ZnsSSTable(channel_factory_, info,
                                           ranges[0].first, ranges[0].second);
  ranges_[0] = std::make_pair(ranges[0].first * info.zone_size,
                              ranges[0].second * info.zone_size);
  for (size_t level = 1; level < ZnsConfig::level_count; level++) {
    sstable_wal_level_[level] = new LNZnsSSTable(
        channel_factory_, info, ranges[level].first, ranges[level].second);
    ranges_[level] = std::make_pair(ranges[level].first * info.zone_size,
                                    ranges[level].second * info.zone_size);
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

Status ZNSSSTableManager::SetValidRangeAndReclaim(const uint8_t level,
                                                  const uint64_t tail,
                                                  const uint64_t head) const {
  assert(level < ZnsConfig::level_count);
  SSZoneMetaData meta;

  uint64_t written_head = sstable_wal_level_[level]->GetHead();
  uint64_t written_tail = sstable_wal_level_[level]->GetTail();
  uint64_t max_z = ranges_[level].second;
  uint64_t min_z = ranges_[level].first;

  // we completely looped around, meaning we can invalidate from tail up to end
  // at least
  if (head < written_head && tail <= head) {
    meta.lba = written_tail;
    meta.lba_count = max_z - written_tail;
    // printf("looped %lu %lu %lu %lu\n", meta.lba, meta.lba_count,
    // written_head,
    //       head);
    Status s = sstable_wal_level_[level]->InvalidateSSZone(&meta);
    if (!s.ok()) return s;
    // still the lagging tail
    if (tail < head) {
      meta.lba = min_z;
      meta.lba_count = tail;
      s = sstable_wal_level_[level]->InvalidateSSZone(&meta);
    }
    return s;
  }
  // lagging tail
  else {
    // printf("lagging tail %lu %lu\n", tail, written_tail);
    meta.lba = written_tail;
    meta.lba_count = tail - written_tail;
    if (meta.lba_count > 0) {
      return sstable_wal_level_[level]->InvalidateSSZone(&meta);
    }
  }
  return Status::OK();
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
