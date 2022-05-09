#include "db/zns_impl/table/ln_zns_sstable.h"

#include "db/zns_impl/io/szd_port.h"
#include "db/zns_impl/table/iterators/sstable_iterator.h"
#include "db/zns_impl/table/zns_sstable.h"
#include "db/zns_impl/table/zns_sstable_coding.h"

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
    EncodeKVPair(&buffer_, key, value);
    meta_->largest.DecodeFrom(key);
    kv_pair_offsets_.push_back(buffer_.size());
    return Status::OK();
  }

  Status Finalise() override {
    meta_->numbers = kv_pair_offsets_.size();
    EncodeSSTablePreamble(&buffer_, kv_pair_offsets_);
    return Status::OK();
  }

  Status Flush() override {
    return table_->WriteSSTable(Slice(buffer_), meta_);
  }

  uint64_t GetSize() const override { return (uint64_t)buffer_.size(); }

 private:
  LNZnsSSTable* table_;
  SSZoneMetaData* meta_;
  std::string buffer_;
  std::vector<uint32_t> kv_pair_offsets_;
  bool started_;
};

LNZnsSSTable::LNZnsSSTable(SZD::SZDChannelFactory* channel_factory,
                           const SZD::DeviceInfo& info,
                           const uint64_t min_zone_nr,
                           const uint64_t max_zone_nr)
    : ZnsSSTable(channel_factory, info, min_zone_nr, max_zone_nr),
      log_(channel_factory_, info, min_zone_nr, max_zone_nr) {}

LNZnsSSTable::~LNZnsSSTable() = default;

SSTableBuilder* LNZnsSSTable::NewBuilder(SSZoneMetaData* meta) {
  return new LNZnsSSTable::Builder(this, meta);
}

bool LNZnsSSTable::EnoughSpaceAvailable(const Slice& slice) const {
  return log_.SpaceLeft(slice.size());
}

Status LNZnsSSTable::WriteSSTable(const Slice& content, SSZoneMetaData* meta) {
  // The callee has to check beforehand if there is enough space.
  if (!EnoughSpaceAvailable(content)) {
    return Status::IOError("Not enough space available for LN");
  }
  meta->lba = log_.GetWriteHead();
  if (!FromStatus(log_.Append(content.ToString(false), &meta->lba_count, false))
           .ok()) {
    return Status::IOError("Error during appending\n");
  }
  return Status::OK();
}

Status LNZnsSSTable::ReadSSTable(Slice* sstable, const SSZoneMetaData& meta) {
  Status s = Status::OK();
  if (meta.lba > max_zone_head_ || meta.lba < min_zone_head_ ||
      meta.lba_count > max_zone_head_ - min_zone_head_) {
    return Status::Corruption("Invalid metadata");
  }
  // We are going to reserve some DMA memory for the loop, as we are reading Lba
  // by lba....
  uint64_t backed_size = std::min(meta.lba_count * lba_size_, mdts_);
  if (!(s = FromStatus(buffer_.ReallocBuffer(backed_size))).ok()) {
    return s;
  }
  char* raw_buffer;
  if (!(s = FromStatus(buffer_.GetBuffer((void**)&raw_buffer))).ok()) {
    return s;
  }

  char* slice_buffer = (char*)calloc(meta.lba_count * lba_size_, sizeof(char));
  // mdts is always a factor of lba_size, so safe.
  uint64_t stepsize = mdts_ / lba_size_;
  uint64_t steps = (meta.lba_count + stepsize - 1) / stepsize;
  uint64_t current_step_size_bytes = mdts_;
  uint64_t last_step_size =
      mdts_ - (steps * stepsize - meta.lba_count) * lba_size_;
  for (uint64_t step = 0; step < steps; ++step) {
    current_step_size_bytes = step == steps - 1 ? last_step_size : mdts_;
    uint64_t addr = meta.lba + step * stepsize;
    addr =
        addr > max_zone_head_ ? min_zone_head_ + (addr - max_zone_head_) : addr;
    if (!FromStatus(log_.Read(addr, &buffer_, current_step_size_bytes, true))
             .ok()) {
      delete[] slice_buffer;
      return Status::IOError("Error reading SSTable");
    }
    memcpy(slice_buffer + step * stepsize * lba_size_, raw_buffer,
           current_step_size_bytes);
  }
  *sstable = Slice((char*)slice_buffer, meta.lba_count * lba_size_);
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
                         const SSZoneMetaData& meta, EntryStatus* status) {
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
      value_ptr->assign(value.data(), value.size());
      delete[] sstable.data();
      return Status::OK();
    }
    counter++;
  }
  *status = EntryStatus::notfound;
  delete[] sstable.data();
  return Status::OK();
}

Status LNZnsSSTable::InvalidateSSZone(const SSZoneMetaData& meta) {
  return FromStatus(log_.ConsumeTail(meta.lba, meta.lba + meta.lba_count));
}

Iterator* LNZnsSSTable::NewIterator(const SSZoneMetaData& meta,
                                    const InternalKeyComparator& icmp) {
  Status s;
  Slice sstable;
  s = ReadSSTable(&sstable, meta);
  if (!s.ok()) {
    return nullptr;
  }
  char* data = (char*)sstable.data();
  uint32_t count = DecodeFixed32(data);
  return new SSTableIterator(data, sstable.size(), (size_t)count, &ParseNext,
                             icmp);
}

Status LNZnsSSTable::Recover() { return FromStatus(log_.RecoverPointers()); }

}  // namespace ROCKSDB_NAMESPACE
