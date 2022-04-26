#include "db/zns_impl/table/ln_zns_sstable.h"

#include "db/zns_impl/io/szd_port.h"
#include "db/zns_impl/io/zns_utils.h"
#include "db/zns_impl/table/iterators/sstable_iterator.h"
#include "db/zns_impl/table/zns_sstable.h"

namespace ROCKSDB_NAMESPACE {

class LNZnsSSTable::Builder : public SSTableBuilder {
 public:
  Builder(LNZnsSSTable* table, SSZoneMetaData* meta)
      : table_(table), meta_(meta), buffer_(""), started_(false) {
    meta_->lba_count = 0;
    buffer_.clear();
  }
  ~Builder() {}

  Status Apply(const Slice& key, const Slice& value) override {
    if (!started_) {
      meta_->smallest.DecodeFrom(key);
      started_ = true;
    }
    table_->PutKVPair(&buffer_, key, value);
    meta_->largest.DecodeFrom(key);
    kv_pair_offsets_.push_back(buffer_.size());
    return Status::OK();
  }

  Status Finalise() override {
    meta_->numbers = kv_pair_offsets_.size();
    table_->GeneratePreamble(&buffer_, kv_pair_offsets_);
    return Status::OK();
  }

  Status Flush() override {
    return table_->WriteSSTable(Slice(buffer_), meta_);
  }

  uint64_t GetSize() override { return (uint64_t)buffer_.size(); }

 private:
  LNZnsSSTable* table_;
  SSZoneMetaData* meta_;
  std::string buffer_;
  std::vector<uint32_t> kv_pair_offsets_;
  bool started_;
};

LNZnsSSTable::LNZnsSSTable(SZD::SZDChannelFactory* channel_factory,
                           const SZD::DeviceInfo& info,
                           const uint64_t min_zone_head, uint64_t max_zone_head)
    : ZnsSSTable(channel_factory, info, min_zone_head, max_zone_head),
      pseudo_write_head_(max_zone_head) {}

LNZnsSSTable::~LNZnsSSTable() = default;

SSTableBuilder* LNZnsSSTable::NewBuilder(SSZoneMetaData* meta) {
  return new LNZnsSSTable::Builder(this, meta);
}

bool LNZnsSSTable::EnoughSpaceAvailable(Slice slice) {
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

Status LNZnsSSTable::SetWriteAddress(Slice slice) {
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

Status LNZnsSSTable::WriteSSTable(Slice content, SSZoneMetaData* meta) {
  // The callee has to check beforehand if there is enough space.
  if (!SetWriteAddress(content).ok()) {
    printf("OUT OF SPACE...\n");
    return Status::IOError("Not enough space available for L0");
  }
  meta->lba = write_head_;
  mutex_.Lock();
  if (!FromStatus(channel_->DirectAppend(&write_head_, (void*)content.data(),
                                         content.size(), false))
           .ok()) {
    mutex_.Unlock();
    return Status::IOError("Error during appending\n");
  }
  mutex_.Unlock();
  zone_head_ = (write_head_ / zone_size_) * zone_size_;
  meta->lba_count = write_head_ - meta->lba;
  return Status::OK();
}

Status LNZnsSSTable::FlushMemTable(ZNSMemTable* mem, SSZoneMetaData* meta) {
  Status s = Status::OK();
  SSTableBuilder* builder = NewBuilder(meta);
  {
    InternalIterator* iter = mem->NewIterator();
    iter->SeekToFirst();
    if (!iter->Valid()) {
      return Status::Corruption("No valid iterator in the memtable");
    }
    for (; iter->Valid(); iter->Next()) {
      const Slice& key = iter->key();
      const Slice& value = iter->value();
      s = builder->Apply(key, value);
    }
    s = builder->Finalise();
    s = builder->Flush();
  }
  delete builder;
  return s;
}

bool LNZnsSSTable::ValidateReadAddress(SSZoneMetaData* meta) {
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

Status LNZnsSSTable::ReadSSTable(Slice* sstable, SSZoneMetaData* meta) {
  Status s = Status::OK();
  if (!ValidateReadAddress(meta)) {
    return Status::Corruption("Invalid metadata");
  }
  char* buffer = (char*)calloc(meta->lba_count * lba_size_, sizeof(char));
  // We are going to reserve some DMA memory for the loop, as we are reading Lba
  // by lba....
  if (!(s = FromStatus(channel_->ReserveBuffer(mdts_))).ok()) {
    return s;
  }
  char* z_buffer;
  if (!(s = FromStatus(channel_->GetBuffer((void**)&z_buffer))).ok()) {
    return s;
  }
  // mdts is always a factor of lba_size, so safe.
  uint64_t stepsize = mdts_ / lba_size_;
  uint64_t steps = (meta->lba_count + stepsize - 1) / stepsize;
  uint64_t current_step_size_bytes = mdts_;
  uint64_t last_step_size =
      mdts_ - (steps * stepsize - meta->lba_count) * lba_size_;
  mutex_.Lock();
  for (uint64_t step = 0; step < steps; ++step) {
    current_step_size_bytes = step == steps - 1 ? last_step_size : mdts_;
    if (!FromStatus(channel_->ReadIntoBuffer(meta->lba + step * stepsize, 0,
                                             current_step_size_bytes))
             .ok()) {
      mutex_.Unlock();
      delete buffer;
      channel_->FreeBuffer();
      return Status::IOError("Error reading SSTable");
    }
    memcpy(buffer + step * stepsize * lba_size_, z_buffer,
           current_step_size_bytes);
  }
  s = FromStatus(channel_->FreeBuffer());
  mutex_.Unlock();
  *sstable = Slice((char*)buffer, meta->lba_count * lba_size_);
  return s;
}

void LNZnsSSTable::ParseNext(char** src, Slice* key, Slice* value) {
  uint32_t keysize, valuesize;
  *src = (char*)GetVarint32Ptr(*src, *src + 5, &keysize);
  *src = (char*)GetVarint32Ptr(*src, *src + 5, &valuesize);
  *key = Slice(*src, keysize);
  *src += keysize;
  *value = Slice(*src, valuesize);
  *src += valuesize;
}

Status LNZnsSSTable::Get(const InternalKeyComparator& icmp,
                         const Slice& key_ptr, std::string* value_ptr,
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
  count = DecodeFixed32(walker);
  walker += sizeof(uint32_t) + count * sizeof(uint32_t);
  const Comparator* user_comparator = icmp.user_comparator();
  Slice key_ptr_stripped = ExtractUserKey(key_ptr);
  while (counter < count) {
    ParseNext(&walker, &key, &value);
    if (user_comparator->Compare(ExtractUserKey(key), key_ptr_stripped) == 0) {
      *status = value.size() > 0 ? EntryStatus::found : EntryStatus::deleted;
      *value_ptr = std::string(value.data(), value.size());
      return Status::OK();
    }
    counter++;
  }
  *status = EntryStatus::notfound;
  return Status::OK();
}

Status LNZnsSSTable::ConsumeTail(uint64_t begin_lba, uint64_t end_lba) {
  if (begin_lba != write_tail_ || begin_lba > end_lba) {
    return Status::InvalidArgument("begin lba is malformed");
  }
  if (end_lba > max_zone_head_) {
    return Status::InvalidArgument("end lba is malformed");
  }
  write_tail_ = end_lba;
  uint64_t cur_zone = (write_tail_ / zone_size_) * zone_size_;
  for (uint64_t slba = zone_tail_; slba < cur_zone; slba += zone_size_) {
    if (!FromStatus(channel_->ResetZone(slba)).ok()) {
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

Status LNZnsSSTable::InvalidateSSZone(SSZoneMetaData* meta) {
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

Iterator* LNZnsSSTable::NewIterator(SSZoneMetaData* meta,
                                    const InternalKeyComparator& icmp) {
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
  return new SSTableIterator(data, sstable.size(), (size_t)count, &ParseNext,
                             icmp);
}

void LNZnsSSTable::EncodeTo(std::string* dst) {
  PutVarint64(dst, zone_head_);
  PutVarint64(dst, write_head_);
  PutVarint64(dst, zone_tail_);
  PutVarint64(dst, write_tail_);
  PutVarint64(dst, pseudo_write_head_);
}

bool LNZnsSSTable::EncodeFrom(Slice* data) {
  bool res =
      GetVarint64(data, &zone_head_) && GetVarint64(data, &write_head_) &&
      GetVarint64(data, &zone_tail_) && GetVarint64(data, &write_tail_) &&
      GetVarint64(data, &pseudo_write_head_);
  return res;
}

}  // namespace ROCKSDB_NAMESPACE
