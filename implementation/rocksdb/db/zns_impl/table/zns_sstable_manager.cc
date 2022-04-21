#include "db/zns_impl/table/zns_sstable_manager.h"

#include "db/zns_impl/io/device_wrapper.h"
#include "db/zns_impl/io/zns_utils.h"
#include "db/zns_impl/memtable/zns_memtable.h"
#include "db/zns_impl/table/l0_zns_sstable.h"
#include "db/zns_impl/table/ln_zns_sstable.h"
#include "db/zns_impl/table/zns_sstable.h"
#include "db/zns_impl/table/zns_zonemetadata.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "table/internal_iterator.h"

namespace ROCKSDB_NAMESPACE {

ZNSSSTableManager::ZNSSSTableManager(QPairFactory* qpair_factory,
                                     const ZnsDevice::DeviceInfo& info,
                                     std::pair<uint64_t, uint64_t> ranges[7])
    : qpair_factory_(qpair_factory) {
  assert(qpair_factory_ != nullptr);
  qpair_factory_->Ref();
  sstable_wal_level_[0] =
      new L0ZnsSSTable(qpair_factory_, info, ranges[0].first, ranges[0].second);
  ranges_[0] = ranges[0];
  for (int level = 1; level < 7; level++) {
    sstable_wal_level_[level] = new LNZnsSSTable(
        qpair_factory_, info, ranges[level].first, ranges[level].second);
    ranges_[level] = ranges[level];
  }
}

ZNSSSTableManager::~ZNSSSTableManager() {
  //printf("Deleting SSTable manager.\n");
  for (int i = 0; i < 7; i++) {
    if (sstable_wal_level_[i] != nullptr) delete sstable_wal_level_[i];
  }
  qpair_factory_->Unref();
  qpair_factory_ = nullptr;
}

Status ZNSSSTableManager::FlushMemTable(ZNSMemTable* mem,
                                        SSZoneMetaData* meta) {
  return GetL0SSTableLog()->FlushMemTable(mem, meta);
}

Status ZNSSSTableManager::WriteSSTable(size_t level, Slice content,
                                       SSZoneMetaData* meta) {
  assert(level < 7);
  return sstable_wal_level_[level]->WriteSSTable(content, meta);
}

Status ZNSSSTableManager::CopySSTable(size_t l1, size_t l2,
                                      SSZoneMetaData* meta) {
  Status s;
  Slice original;
  SSZoneMetaData tmp(*meta);
  s = ReadSSTable(l1, &original, &tmp);
  if (!s.ok()) {
    return s;
  }
  return sstable_wal_level_[l2]->WriteSSTable(original, meta);
}

bool ZNSSSTableManager::EnoughSpaceAvailable(size_t level, Slice slice) {
  assert(level < 7);
  return sstable_wal_level_[level]->EnoughSpaceAvailable(slice);
}

Status ZNSSSTableManager::InvalidateSSZone(size_t level, SSZoneMetaData* meta) {
  assert(level < 7);
  return sstable_wal_level_[level]->InvalidateSSZone(meta);
}

Status ZNSSSTableManager::InvalidateUpTo(size_t level, uint64_t tail) {
  assert(level < 7);
  SSZoneMetaData meta;
  //printf("delete %lu %lu\n", tail, sstable_wal_level_[level]->GetTail());
  if (tail < ranges_[level].first || tail > ranges_[level].second) {
    return Status::OK();
  }
  if (tail > sstable_wal_level_[level]->GetTail()) {
    meta.lba = sstable_wal_level_[level]->GetTail();
    meta.lba_count = tail - sstable_wal_level_[level]->GetTail();
    return sstable_wal_level_[level]->InvalidateSSZone(&meta);
  } else if (tail < sstable_wal_level_[level]->GetTail()) {
    meta.lba = sstable_wal_level_[level]->GetTail();
    meta.lba_count =
        ranges_[level].second - sstable_wal_level_[level]->GetTail();
    Status s = sstable_wal_level_[level]->InvalidateSSZone(&meta);
    if (!s.ok()) return s;
    meta.lba = ranges_[level].first;
    meta.lba_count = tail - ranges_[level].first;
    return sstable_wal_level_[level]->InvalidateSSZone(&meta);
  }
  return Status::OK();
}

Status ZNSSSTableManager::Get(size_t level, const InternalKeyComparator& icmp,
                              const Slice& key_ptr, std::string* value_ptr,
                              SSZoneMetaData* meta, EntryStatus* status) {
  assert(level < 7);
  return sstable_wal_level_[level]->Get(icmp, key_ptr, value_ptr, meta, status);
}

Status ZNSSSTableManager::ReadSSTable(size_t level, Slice* sstable,
                                      SSZoneMetaData* meta) {
  assert(level < 7);
  return sstable_wal_level_[level]->ReadSSTable(sstable, meta);
}

L0ZnsSSTable* ZNSSSTableManager::GetL0SSTableLog() {
  return dynamic_cast<L0ZnsSSTable*>(sstable_wal_level_[0]);
}

Iterator* ZNSSSTableManager::NewIterator(size_t level, SSZoneMetaData* meta) {
  assert(level < 7);
  return sstable_wal_level_[level]->NewIterator(meta);
}

SSTableBuilder* ZNSSSTableManager::NewBuilder(size_t level,
                                              SSZoneMetaData* meta) {
  assert(level < 7);
  return sstable_wal_level_[level]->NewBuilder(meta);
}

void ZNSSSTableManager::EncodeTo(std::string* dst) {
  for (int i = 0; i < 7; i++) {
    sstable_wal_level_[i]->EncodeTo(dst);
  }
}
Status ZNSSSTableManager::DecodeFrom(const Slice& data) {
  Slice input = Slice(data);
  for (int i = 0; i < 7; i++) {
    if (!sstable_wal_level_[i]->EncodeFrom(&input)) {
      return Status::Corruption("Corrupt level");
    }
  }
  return Status::OK();
}

void ZNSSSTableManager::GetRange(int level,
                                 const std::vector<SSZoneMetaData*>& metas,
                                 std::pair<uint64_t, uint64_t>* range) {
  // if (metas.size() > 1) {
  //   *range = std::make_pair(metas[metas.size()-1]->lba,
  //   metas[0]->lba+metas[0]->lba_count);
  // }
  uint64_t lowest = 0, lowest_res = 0;
  uint64_t highest = 0, highest_res = 0;
  bool first = false;

  for (auto n = metas.begin(); n != metas.end(); n++) {
    if (!first) {
      lowest = (*n)->number;
      lowest_res = (*n)->lba;
      highest = (*n)->number;
      highest_res = (*n)->lba;
      first = true;
    }
    if (lowest > (*n)->number) {
      lowest = (*n)->number;
      lowest_res = (*n)->lba;
    }
    if (highest < (*n)->number) {
      highest = (*n)->number;
      highest_res = (*n)->lba;
    }
  }
  if (first) {
    *range = std::make_pair(lowest_res, highest_res);
  }
  // TODO: higher levels will need more info.
}
}  // namespace ROCKSDB_NAMESPACE
