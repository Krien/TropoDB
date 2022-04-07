#include "db/zns_impl/zns_sstable_manager.h"

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
                                     const uint64_t min_zone_head,
                                     uint64_t max_zone_head)
    : zone_head_(min_zone_head),
      write_head_(min_zone_head),
      min_zone_head_(min_zone_head),
      max_zone_head_(max_zone_head),
      zone_size_(info.zone_size),
      lba_size_(info.lba_size),
      qpair_factory_(qpair_factory) {
  assert(zone_head_ < info.lba_cap);
  assert(zone_head_ % info.lba_size == 0);
  assert(qpair_factory_ != nullptr);
  qpair_ = new ZnsDevice::QPair*[1];
  qpair_factory_->Ref();
  qpair_factory_->register_qpair(qpair_);
}

ZNSSSTableManager::~ZNSSSTableManager() {
  printf("Deleting SSTable manager.\n");
  if (qpair_ != nullptr) {
    qpair_factory_->unregister_qpair(*qpair_);
    delete qpair_;
  }
  qpair_factory_->Unref();
  qpair_factory_ = nullptr;
}

Status ZNSSSTableManager::FlushMemTable(ZNSMemTable* mem,
                                        SSZoneMetaData* meta) {
  printf("Write to L0\n");
  std::string output;
  // For now, simply iterate over the memtable content and add them uncompressed
  // and directly.
  InternalIterator* iter = mem->NewIterator();
  meta->lba_count = 0;
  iter->SeekToFirst();
  uint64_t numbers = 0;
  if (iter->Valid()) {
    meta->smallest.DecodeFrom(iter->key());
    for (; iter->Valid(); iter->Next()) {
      const Slice& key = iter->key();
      const Slice& value = iter->value();
      PutVarint32(&output, key.size());
      PutVarint32(&output, value.size());
      output.append(key.data(), key.size());
      output.append(value.data(), value.size());
      meta->largest.DecodeFrom(iter->key());
      numbers++;
    }
    meta->numbers = numbers;
    std::string tmp;
    PutVarint32(&tmp, numbers);
    output = tmp.append(output);
    uint64_t zcalloc_size = 0;
    char* payload = ZnsUtils::slice_to_spdkformat(&zcalloc_size, Slice(output),
                                                  *qpair_, lba_size_);
    if (payload == nullptr) {
      delete iter;
      return Status::MemoryLimit();
    }
    ZnsDevice::z_append(*qpair_, zone_head_, payload, zcalloc_size);
    meta->lba = write_head_;
    ZnsUtils::update_zns_heads(&write_head_, &zone_head_, zcalloc_size,
                               lba_size_, zone_size_);
    meta->lba_count = zcalloc_size / lba_size_;
  }
  // delete iter;
  return Status::OK();
}

Status ZNSSSTableManager::Get(const Slice& key_ptr, std::string* value_ptr,
                              SSZoneMetaData* meta, EntryStatus* status) {
  Slice sstable;
  Status s;
  s = ReadSSTable(&sstable, meta);
  if (!s.ok()) {
    return s;
  }
  char* walker = (char*)sstable.data();
  uint32_t keysize, valuesize;
  Slice key, value;
  uint32_t count, counter;
  counter = 0;
  walker = (char*)GetVarint32Ptr(walker, walker + 5, &count);
  while (counter < count) {
    walker = (char*)GetVarint32Ptr(walker, walker + 5, &keysize);
    walker = (char*)GetVarint32Ptr(walker, walker + 5, &valuesize);
    key = Slice(walker, keysize - 8);
    walker = walker + keysize;
    value = Slice(walker, valuesize - 8);
    walker = walker + valuesize;
    if (key_ptr.compare(key) == 0) {
      *value_ptr = std::string(value.data());
      *status = value.size() > 0 ? EntryStatus::found : EntryStatus::deleted;
      return Status::OK();
    }
    counter++;
  }
  *status = EntryStatus::notfound;
  return Status::NotFound();
}

Status ZNSSSTableManager::ReadSSTable(Slice* sstable, SSZoneMetaData* meta) {
  // printf("read from sstable at %lu \n", meta->lba);
  void* payload =
      ZnsDevice::z_calloc(*qpair_, meta->lba_count * lba_size_, sizeof(char));
  ZnsDevice::z_read(*qpair_, meta->lba, payload, meta->lba_count * lba_size_);
  char* payloadc = (char*)calloc(meta->lba_count * lba_size_, sizeof(char));
  memcpy(payloadc, payload, meta->lba_count * lba_size_);
  *sstable = Slice((char*)payloadc);
  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE
