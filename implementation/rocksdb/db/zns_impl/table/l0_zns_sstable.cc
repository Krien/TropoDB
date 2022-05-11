#include "db/zns_impl/table/l0_zns_sstable.h"

#include "db/zns_impl/config.h"
#include "db/zns_impl/io/szd_port.h"
#include "db/zns_impl/table/iterators/sstable_iterator.h"
#include "db/zns_impl/table/iterators/sstable_iterator_compressed.h"
#include "db/zns_impl/table/zns_sstable.h"
#include "db/zns_impl/table/zns_sstable_builder.h"
#include "db/zns_impl/table/zns_sstable_reader.h"

namespace ROCKSDB_NAMESPACE {

L0ZnsSSTable::L0ZnsSSTable(SZD::SZDChannelFactory* channel_factory,
                           const SZD::DeviceInfo& info,
                           const uint64_t min_zone_nr,
                           const uint64_t max_zone_nr)
    : ZnsSSTable(channel_factory, info, min_zone_nr, max_zone_nr),
      log_(channel_factory_, info, min_zone_nr, max_zone_nr) {}

L0ZnsSSTable::~L0ZnsSSTable() = default;

Status L0ZnsSSTable::Recover() { return FromStatus(log_.RecoverPointers()); }

SSTableBuilder* L0ZnsSSTable::NewBuilder(SSZoneMetaData* meta) {
  return new SSTableBuilder(this, meta, ZnsConfig::use_sstable_encoding);
}

bool L0ZnsSSTable::EnoughSpaceAvailable(const Slice& slice) const {
  return log_.SpaceLeft(slice.size());
}

Status L0ZnsSSTable::WriteSSTable(const Slice& content, SSZoneMetaData* meta) {
  // The callee has to check beforehand if there is enough space.
  if (!EnoughSpaceAvailable(content)) {
    return Status::IOError("Not enough space available for L0");
  }
  meta->lba = log_.GetWriteHead();
  if (!FromStatus(
           log_.Append(content.data(), content.size(), &meta->lba_count, false))
           .ok()) {
    return Status::IOError("Error during appending\n");
  }
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

Status L0ZnsSSTable::ReadSSTable(Slice* sstable, const SSZoneMetaData& meta) {
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
    uint64_t lba = meta.lba + step * stepsize;
    lba = lba > max_zone_head_ ? min_zone_head_ + (lba - max_zone_head_) : lba;
    if (!FromStatus(log_.Read(lba, &buffer_, current_step_size_bytes, true))
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

Status L0ZnsSSTable::InvalidateSSZone(const SSZoneMetaData& meta) {
  return FromStatus(log_.ConsumeTail(meta.lba, meta.lba + meta.lba_count));
}

Iterator* L0ZnsSSTable::NewIterator(const SSZoneMetaData& meta,
                                    const Comparator* cmp) {
  Status s;
  Slice sstable;
  s = ReadSSTable(&sstable, meta);
  if (!s.ok()) {
    return nullptr;
  }
  char* data = (char*)sstable.data();
  if (ZnsConfig::use_sstable_encoding) {
    uint32_t size = DecodeFixed32(data);
    uint32_t count = DecodeFixed32(data + sizeof(uint32_t));
    return new SSTableIteratorCompressed(cmp, data, size, count);
  } else {
    uint32_t count = DecodeFixed32(data);
    return new SSTableIterator(data, sstable.size(), (size_t)count,
                               &ZNSEncoding::ParseNextNonEncoded, cmp);
  }
}

Status L0ZnsSSTable::Get(const InternalKeyComparator& icmp,
                         const Slice& key_ptr, std::string* value_ptr,
                         const SSZoneMetaData& meta, EntryStatus* status) {
  Iterator* it = NewIterator(meta, icmp.user_comparator());
  if (it == nullptr) {
    return Status::Corruption();
  }
  it->Seek(key_ptr);
  if (it->Valid()) {
    ParsedInternalKey parsed_key;
    if (!ParseInternalKey(it->key(), &parsed_key, false).ok()) {
      printf("corrupt key in cache\n");
    }
    if (parsed_key.type == kTypeDeletion) {
      *status = EntryStatus::deleted;
      value_ptr->clear();
    } else {
      *status = EntryStatus::found;
      *value_ptr = it->value().ToString();
    }
  } else {
    *status = EntryStatus::notfound;
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
