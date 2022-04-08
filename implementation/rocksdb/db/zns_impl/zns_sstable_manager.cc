#include "db/zns_impl/zns_sstable_manager.h"

#include <iostream>

#include "db/zns_impl/device_wrapper.h"
#include "db/zns_impl/zns_memtable.h"
#include "db/zns_impl/zns_utils.h"
#include "db/zns_impl/zns_zonemetadata.h"
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
  for (int level = 0; level < 7; level++) {
    sstable_wal_level_[level] = new L0ZnsSSTable(
        qpair_factory_, info, ranges[level].first, ranges[level].second);
  }
}

ZNSSSTableManager::~ZNSSSTableManager() {
  printf("Deleting SSTable manager.\n");
  for (int i = 0; i < 7; i++) {
    if (sstable_wal_level_[i] != nullptr) delete sstable_wal_level_[i];
  }
  qpair_factory_->Unref();
  qpair_factory_ = nullptr;
}

Status ZNSSSTableManager::FlushMemTable(ZNSMemTable* mem,
                                        SSZoneMetaData* meta) {
  // printf("Write to L0\n");
  return sstable_wal_level_[0]->FlushMemTable(mem, meta);
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

Status ZNSSSTableManager::Get(size_t level, const Slice& key_ptr,
                              std::string* value_ptr, SSZoneMetaData* meta,
                              EntryStatus* status) {
  assert(level < 7);
  return sstable_wal_level_[level]->Get(key_ptr, value_ptr, meta, status);
}

Status ZNSSSTableManager::ReadSSTable(size_t level, Slice* sstable,
                                      SSZoneMetaData* meta) {
  assert(level < 7);
  return sstable_wal_level_[level]->ReadSSTable(sstable, meta);
}

L0ZnsSSTable* ZNSSSTableManager::GetL0SSTableLog() {
  return sstable_wal_level_[0];
}
}  // namespace ROCKSDB_NAMESPACE
