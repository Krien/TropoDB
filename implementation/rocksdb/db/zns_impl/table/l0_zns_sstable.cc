#include "db/zns_impl/device_wrapper.h"
#include "db/zns_impl/qpair_factory.h"
#include "db/zns_impl/table/iterators/sstable_iterator.h"
#include "db/zns_impl/table/zns_sstable.h"
#include "db/zns_impl/zns_utils.h"

namespace ROCKSDB_NAMESPACE {

class L0ZnsSSTable::Builder : public SSTableBuilder {
 public:
  Builder(L0ZnsSSTable* table, SSZoneMetaData* meta)
      : table_(table), meta_(meta), buffer_(""), kv_pairs_(0), started_(false) {
    meta_->lba_count = 0;
    buffer_.clear();
  }
  ~Builder() {}

  Status Apply(const Slice& key, const Slice& value) override {
    if (!started_) {
      meta_->smallest = InternalKey(key, kMaxSequenceNumber, kTypeValue);
      started_ = true;
    }
    table_->PutKVPair(&buffer_, key, value);
    meta_->largest = InternalKey(key, kMaxSequenceNumber, kTypeValue);
    ++kv_pairs_;
    return Status::OK();
  }

  Status Finalise() override {
    meta_->numbers = kv_pairs_;
    table_->GeneratePreamble(&buffer_, meta_->numbers);
    return Status::OK();
  }

  Status Flush() override {
    return table_->WriteSSTable(Slice(buffer_), meta_);
  }

 private:
  L0ZnsSSTable* table_;
  SSZoneMetaData* meta_;
  std::string buffer_;
  uint32_t kv_pairs_;
  bool started_;
};

L0ZnsSSTable::L0ZnsSSTable(QPairFactory* qpair_factory,
                           const ZnsDevice::DeviceInfo& info,
                           const uint64_t min_zone_head, uint64_t max_zone_head)
    : ZnsSSTable(qpair_factory, info, min_zone_head, max_zone_head),
      pseudo_write_head_(max_zone_head) {}

L0ZnsSSTable::~L0ZnsSSTable() {}

SSTableBuilder* L0ZnsSSTable::NewBuilder(SSZoneMetaData* meta) {
  return new L0ZnsSSTable::Builder(this, meta);
}

bool L0ZnsSSTable::EnoughSpaceAvailable(Slice slice) {
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

Status L0ZnsSSTable::SetWriteAddress(Slice slice) {
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

Status L0ZnsSSTable::WriteSSTable(Slice content, SSZoneMetaData* meta) {
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
  mutex_.Lock();
  if (ZnsDevice::z_append(*qpair_, write_head_, payload, zcalloc_size) != 0) {
    ZnsDevice::z_free(*qpair_, payload);
    mutex_.Unlock();
    printf("append error %lu %lu %lu\n", zone_head_, write_head_, zcalloc_size);
    return Status::IOError("Error during appending\n");
  }
  ZnsDevice::z_free(*qpair_, payload);
  mutex_.Unlock();
  meta->lba = write_head_;
  ZnsUtils::update_zns_heads(&write_head_, &zone_head_, zcalloc_size, lba_size_,
                             zone_size_);
  meta->lba_count = zcalloc_size / lba_size_;
  return Status::OK();
}

Status L0ZnsSSTable::FlushMemTable(ZNSMemTable* mem, SSZoneMetaData* meta) {
  Status s = Status::OK();
  SSTableBuilder* builder = NewBuilder(meta);
  {
    InternalIterator* iter = mem->NewIterator();
    iter->SeekToFirst();
    if (!iter->Valid()) {
      return Status::Corruption("No valid iterator in the memtable");
    }
    for (; iter->Valid(); iter->Next()) {
      const Slice& key = iter->user_key();
      const Slice& value = iter->value();
      s = builder->Apply(key, value);
    }
    s = builder->Finalise();
    s = builder->Flush();
  }
  delete builder;
  return s;
}

bool L0ZnsSSTable::ValidateReadAddress(SSZoneMetaData* meta) {
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

Status L0ZnsSSTable::ReadSSTable(Slice* sstable, SSZoneMetaData* meta) {
  if (!ValidateReadAddress(meta)) {
    return Status::Corruption("Invalid metadata");
  }
  // Lba by lba...
  void* payload = ZnsDevice::z_calloc(*qpair_, lba_size_, sizeof(char));
  if (payload == nullptr) {
    return Status::IOError("Error allocating DMA\n");
  }
  char* payloadc = (char*)calloc(meta->lba_count * lba_size_, sizeof(char));
  for (uint64_t i = 0; i < meta->lba_count; i++) {
    mutex_.Lock();
    int rc = ZnsDevice::z_read(*qpair_, meta->lba + i, payload, lba_size_);
    mutex_.Unlock();
    if (rc != 0) {
      ZnsDevice::z_free(*qpair_, payload);
      free(payloadc);
      return Status::IOError("Error reading SSTable");
    }
    memcpy(payloadc + i * lba_size_, payload, lba_size_);
  }
  ZnsDevice::z_free(*qpair_, payload);
  *sstable = Slice((char*)payloadc, meta->lba_count * lba_size_);
  return Status::OK();
}

void L0ZnsSSTable::ParseNext(char** src, Slice* key, Slice* value) {
  uint32_t keysize, valuesize;
  *src = (char*)GetVarint32Ptr(*src, *src + 5, &keysize);
  *src = (char*)GetVarint32Ptr(*src, *src + 5, &valuesize);
  *key = Slice(*src, keysize);
  *src += keysize;
  *value = Slice(*src, valuesize);
  *src += valuesize;
}

Status L0ZnsSSTable::Get(const Slice& key_ptr, std::string* value_ptr,
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
    ParseNext(&walker, &key, &value);
    if (key_ptr.compare(key) == 0) {
      *status = value.size() > 0 ? EntryStatus::found : EntryStatus::deleted;
      *value_ptr = std::string(value.data(), value.size());
      return Status::OK();
    }
    counter++;
  }
  *status = EntryStatus::notfound;
  return Status::OK();
}

Status L0ZnsSSTable::ConsumeTail(uint64_t begin_lba, uint64_t end_lba) {
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

Status L0ZnsSSTable::InvalidateSSZone(SSZoneMetaData* meta) {
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

Iterator* L0ZnsSSTable::NewIterator(SSZoneMetaData* meta) {
  Status s;
  Slice sstable;
  s = ReadSSTable(&sstable, meta);
  if (!s.ok()) {
    return nullptr;
  }
  char* data = new char[sstable.size() + 1];
  memcpy(data, sstable.data(), sstable.size());
  uint32_t count;
  data = (char*)GetVarint32Ptr(data, data + 5, &count);
  return new SSTableIterator(data, (size_t)count, &ParseNext);
}
}  // namespace ROCKSDB_NAMESPACE
