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
                                     const uint64_t min_zone_head,
                                     uint64_t max_zone_head)
    : zone_head_(min_zone_head),
      write_head_(min_zone_head),
      zone_tail_(min_zone_head),
      write_tail_(min_zone_head),
      min_zone_head_(min_zone_head),
      max_zone_head_(max_zone_head),
      zone_size_(info.zone_size),
      lba_size_(info.lba_size),
      pseudo_write_head_(max_zone_head_),
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

void ZNSSSTableManager::PutKVPair(std::string* dst, const Slice& key,
                                  const Slice& value) {
  PutVarint32(dst, key.size());
  PutVarint32(dst, value.size());
  dst->append(key.data(), key.size());
  dst->append(value.data(), value.size());
}

Status ZNSSSTableManager::GenerateSSTableString(std::string* dst,
                                                ZNSMemTable* mem,
                                                SSZoneMetaData* meta) {
  dst->clear();
  meta->lba_count = 0;
  InternalIterator* iter = mem->NewIterator();
  iter->SeekToFirst();
  if (!iter->Valid()) {
    return Status::Corruption("No valid iterator in the memtable");
  }

  // list of (keysize, key, valuesize, value)
  uint64_t kv_pairs = 0;
  meta->smallest.DecodeFrom(iter->key());
  for (; iter->Valid(); iter->Next()) {
    const Slice& key = iter->key();
    const Slice& value = iter->value();
    PutKVPair(dst, key, value);
    meta->largest.DecodeFrom(iter->key());
    kv_pairs++;
  }
  meta->numbers = kv_pairs;

  // Preamble, containing amount of kv_pairs
  std::string preamble;
  PutVarint32(&preamble, kv_pairs);
  *dst = preamble.append(*dst);
  return Status::OK();
}

Status ZNSSSTableManager::SetWriteAddress(Slice slice) {
  uint64_t zcalloc_size = 0;
  ZnsUtils::allign_size(&zcalloc_size, Slice(slice), lba_size_);
  uint64_t blocks_needed = zcalloc_size / lba_size_;

  // head got ahead of tail :)
  if (write_head_ >= write_tail_) {
    // [vvvvTZ-WT----------WZ-WHvvvvv]
    uint64_t space_end = max_zone_head_ - write_head_;
    uint64_t space_begin = zone_tail_ - min_zone_head_;
    if (space_end < blocks_needed && space_begin < blocks_needed) {
      return Status::NoSpace();
    }
    // Cutoff the head (we can not split SSTables at this point, it would
    // fracture the table)
    if (space_end < blocks_needed) {
      pseudo_write_head_ = write_head_;
      write_head_ = min_zone_head_;
      zone_head_ = min_zone_head_;
    }
  } else {
    // [--WZ--WHvvvvvvvvTZ----WT---]
    uint64_t space = zone_tail_ - write_head_;
    if (space < blocks_needed) {
      return Status::NoSpace();
    }
  }
  return Status::OK();
}

Status ZNSSSTableManager::WriteSSTable(Slice content, SSZoneMetaData* meta) {
  // The callee has to check beforehand if there is enough space.
  if (!SetWriteAddress(content).ok()) {
    return Status::IOError("Not enough space available for L0");
  }
  uint64_t zcalloc_size;
  char* payload =
      ZnsUtils::slice_to_spdkformat(&zcalloc_size, content, *qpair_, lba_size_);
  if (payload == nullptr) {
    return Status::MemoryLimit();
  }
  if (ZnsDevice::z_append(*qpair_, write_head_, payload, zcalloc_size) != 0) {
    ZnsDevice::z_free(*qpair_, payload);
    printf("append error %lu %lu %lu\n", zone_head_, write_head_, zcalloc_size);
    return Status::IOError("Error during appending\n");
  }
  ZnsDevice::z_free(*qpair_, payload);
  meta->lba = write_head_;
  ZnsUtils::update_zns_heads(&write_head_, &zone_head_, zcalloc_size, lba_size_,
                             zone_size_);
  meta->lba_count = zcalloc_size / lba_size_;
  return Status::OK();
}

Status ZNSSSTableManager::FlushMemTable(ZNSMemTable* mem,
                                        SSZoneMetaData* meta) {
  printf("Write to L0\n");
  std::string dst;
  GenerateSSTableString(&dst, mem, meta);
  return WriteSSTable(Slice(dst), meta);
}

Status ZNSSSTableManager::RewriteSSTable(SSZoneMetaData* meta) {
  Status s;
  Slice original;
  s = ReadSSTable(&original, meta);
  if (!s.ok()) {
    return s;
  }
  return WriteSSTable(original, meta);
}

bool ZNSSSTableManager::EnoughSpaceAvailable(Slice slice) {
  uint64_t zcalloc_size = 0;
  ZnsUtils::allign_size(&zcalloc_size, Slice(slice), lba_size_);
  uint64_t blocks_needed = zcalloc_size / lba_size_;

  // head got ahead of tail :)
  if (write_head_ >= write_tail_) {
    // [vvvvTZ-WT----------WZ-WHvvvvv]
    uint64_t space_end = max_zone_head_ - write_head_;
    uint64_t space_begin = zone_tail_ - min_zone_head_;
    return space_end > blocks_needed || space_begin > blocks_needed;
  } else {
    // [--WZ--WHvvvvvvvvTZ----WT---]
    uint64_t space = zone_tail_ - write_head_;
    return space > blocks_needed;
  }
  // not possible or false and true are not the only booleans.
  return false;
}

Status ZNSSSTableManager::InvalidateSSZone(SSZoneMetaData* meta) {
  Status s = ConsumeTail(meta->lba, meta->lba + meta->lba_count);
  if (!s.ok()) {
    return s;
  }
  // Slight hack to make sure that no space is lost when there is a gap between
  // max and last sstable.
  if (meta->lba + meta->lba_count == pseudo_write_head_ &&
      pseudo_write_head_ != max_zone_head_) {
    s = ConsumeTail(pseudo_write_head_, max_zone_head_);
    pseudo_write_head_ = max_zone_head_;
  }
  return s;
}

Status ZNSSSTableManager::ConsumeTail(uint64_t begin_lba, uint64_t end_lba) {
  if (begin_lba != write_tail_ || begin_lba > end_lba) {
    return Status::InvalidArgument("begin lba is malformed");
  }
  if (end_lba > max_zone_head_) {
    return Status::InvalidArgument("end lba is malformed");
  }

  write_tail_ = end_lba;
  uint64_t cur_zone = (write_tail_ / zone_size_) * zone_size_;
  for (uint64_t i = zone_tail_; i < cur_zone; i += lba_size_) {
    int rc = ZnsDevice::z_reset(*qpair_, zone_tail_, false);
    if (rc != 0) {
      return Status::IOError("Error resetting SSTable tail");
    }
  }
  zone_tail_ = cur_zone;
  // Wraparound
  if (zone_tail_ == max_zone_head_) {
    zone_tail_ = write_tail_ = min_zone_head_;
  }
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
    value = Slice(walker, valuesize);
    walker = walker + valuesize;
    if (key_ptr.compare(key) == 0) {
      *status = value.size() > 0 ? EntryStatus::found : EntryStatus::deleted;
      *value_ptr = std::string(value.data(), valuesize);
      return Status::OK();
    }
    counter++;
  }
  *status = EntryStatus::notfound;
  return Status::NotFound();
}

bool ZNSSSTableManager::ValidateReadAddress(SSZoneMetaData* meta) {
  if (write_head_ >= write_tail_) {
    // [---------------WTvvvvWH--]
    if (meta->lba < write_tail_ || meta->lba + meta->lba_count > write_head_) {
      return false;
    }
  } else {
    // [vvvvvvvvvvvvvvvvWH---WTvv]
    if ((meta->lba > write_head_ && meta->lba < write_tail_) ||
        (meta->lba + meta->lba_count > write_head_ &&
         meta->lba + meta->lba_count < write_tail_)) {
      return false;
    }
  }
  return true;
}

Status ZNSSSTableManager::ReadSSTable(Slice* sstable, SSZoneMetaData* meta) {
  if (!ValidateReadAddress(meta)) {
    return Status::Corruption("Invalid metadata");
  }
  // printf("read from sstable at %lu \n", meta->lba);
  void* payload =
      ZnsDevice::z_calloc(*qpair_, meta->lba_count * lba_size_, sizeof(char));
  ZnsDevice::z_read(*qpair_, meta->lba, payload, meta->lba_count * lba_size_);
  char* payloadc = (char*)calloc(meta->lba_count * lba_size_, sizeof(char));
  memcpy(payloadc, payload, meta->lba_count * lba_size_);
  *sstable = Slice((char*)payloadc, meta->lba_count * lba_size_);
  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE
