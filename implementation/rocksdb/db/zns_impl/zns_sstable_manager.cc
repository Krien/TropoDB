#include "db/zns_impl/zns_sstable_manager.h"

#include <iostream>

#include "db/zns_impl/device_wrapper.h"
#include "db/zns_impl/table/l0_zns_sstable.h"
#include "db/zns_impl/table/ln_zns_sstable.h"
#include "db/zns_impl/table/zns_sstable.h"
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
  sstable_wal_level_[0] =
      new L0ZnsSSTable(qpair_factory_, info, ranges[0].first, ranges[0].second);
  for (int level = 1; level < 7; level++) {
    sstable_wal_level_[level] = new LNZnsSSTable(
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

}  // namespace ROCKSDB_NAMESPACE
