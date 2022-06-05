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
      log_(channel_factory_, info, min_zone_nr, max_zone_nr,
           number_of_concurrent_readers),
      committer_(&log_, info, true),
      cv_(&mutex_) {
  // unset
  for (uint8_t i = 0; i < number_of_concurrent_readers; i++) {
    read_queue_[i] = 0;
  }
}

L0ZnsSSTable::~L0ZnsSSTable() = default;

Status L0ZnsSSTable::Recover() { return FromStatus(log_.RecoverPointers()); }

SSTableBuilder* L0ZnsSSTable::NewBuilder(SSZoneMetaData* meta) {
  return new SSTableBuilder(this, meta, ZnsConfig::use_sstable_encoding);
}

bool L0ZnsSSTable::EnoughSpaceAvailable(const Slice& slice) const {
  return committer_.SpaceEnough(slice);
}

uint64_t L0ZnsSSTable::SpaceAvailable() const { return log_.SpaceAvailable(); }

Status L0ZnsSSTable::WriteSSTable(const Slice& content, SSZoneMetaData* meta) {
  // The callee has to check beforehand if there is enough space.
  if (!EnoughSpaceAvailable(content)) {
    printf("Out of space L0\n");
    return Status::IOError("Not enough space available for L0");
  }
  meta->L0.lba = log_.GetWriteHead();
  Status s = committer_.SafeCommit(content, &meta->lba_count);
  // printf("Writing L0 %lu %lu \n", meta->L0.lba, meta->lba_count);
  return s;
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

// TODO: this is better than locking around the entire read, but we have to
// investigate the performance.
uint8_t L0ZnsSSTable::request_read_queue() {
  uint8_t picked_reader = number_of_concurrent_readers;
  mutex_.Lock();
  for (uint8_t i = 0; i < number_of_concurrent_readers; i++) {
    if (read_queue_[i] == 0) {
      picked_reader = i;
      break;
    }
  }
  while (picked_reader >= number_of_concurrent_readers) {
    cv_.Wait();
    for (uint8_t i = 0; i < number_of_concurrent_readers; i++) {
      if (read_queue_[i] == 0) {
        picked_reader = i;
        break;
      }
    }
  }
  printf("Claimed reader %u \n", picked_reader);
  read_queue_[picked_reader] = 1;
  mutex_.Unlock();
  return picked_reader;
}

void L0ZnsSSTable::release_read_queue(uint8_t reader) {
  mutex_.Lock();
  assert(reader < number_of_concurrent_readers && read_queue_[reader] != 0);
  printf("Released reader %u \n", reader);
  read_queue_[reader] = 0;
  cv_.SignalAll();
  mutex_.Unlock();
}

Status L0ZnsSSTable::ReadSSTable(Slice* sstable, const SSZoneMetaData& meta) {
  Status s = Status::OK();
  if (meta.L0.lba > max_zone_head_ || meta.L0.lba < min_zone_head_ ||
      meta.lba_count > max_zone_head_ - min_zone_head_) {
    return Status::Corruption("Invalid metadata");
  }

  sstable->clear();
  Slice record;
  std::string* raw_data = new std::string();
  bool succeeded_once = false;

  ZnsCommitReader reader;
  // mutex_.Lock();
  uint8_t readernr = request_read_queue();
  s = committer_.GetCommitReader(readernr, meta.L0.lba,
                                 meta.L0.lba + meta.lba_count, &reader);

  // printf("reading L0 %lu %lu \n", meta.L0.lba, meta.lba_count);
  if (!s.ok()) {
    release_read_queue(readernr);
    // mutex_.Unlock();
    return s;
  }
  while (committer_.SeekCommitReader(reader, &record)) {
    succeeded_once = true;
    raw_data->append(record.data(), record.size());
  }
  if (!committer_.CloseCommit(reader)) {
    release_read_queue(readernr);
    // mutex_.Unlock();
    return Status::Corruption();
  }
  // mutex_.Unlock();
  release_read_queue(readernr);

  // Committer never succeeded.
  if (!succeeded_once) {
    return Status::Corruption();
  }

  *sstable = Slice(raw_data->data(), raw_data->size());
  return Status::OK();
}

Status L0ZnsSSTable::InvalidateSSZone(const SSZoneMetaData& meta) {
  // printf("Invalidating L0 %lu %lu \n", meta.L0.lba, meta.lba_count);
  return FromStatus(
      log_.ConsumeTail(meta.L0.lba, meta.L0.lba + meta.lba_count));
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
      printf("corrupt key in L0 SSTable\n");
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
    value_ptr->clear();
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
