#include "db/zns_impl/table/l0_zns_sstable.h"

#include "db/zns_impl/config.h"
#include "db/zns_impl/io/szd_port.h"
#include "db/zns_impl/table/iterators/sstable_iterator.h"
#include "db/zns_impl/table/iterators/sstable_iterator_compressed.h"
#include "db/zns_impl/table/zns_sstable.h"
#include "db/zns_impl/table/zns_sstable_builder.h"
#include "db/zns_impl/table/zns_sstable_reader.h"
#include "db/zns_impl/utils/tropodb_logger.h"

namespace ROCKSDB_NAMESPACE {

L0ZnsSSTable::L0ZnsSSTable(SZD::SZDChannelFactory* channel_factory,
                           const SZD::DeviceInfo& info,
                           const uint64_t min_zone_nr,
                           const uint64_t max_zone_nr)
    : ZnsSSTable(channel_factory, info, min_zone_nr, max_zone_nr),
      log_(channel_factory_, info, min_zone_nr, max_zone_nr,
           ZnsConfig::number_of_concurrent_L0_readers),
#ifdef USE_COMMITTER
      committer_(&log_, info, true),
#endif
      zasl_(info.zasl),
      lba_size_(info.lba_size),
      zone_size_(info.zone_size),
      cv_(&mutex_) {
  // unset
  for (uint8_t i = 0; i < ZnsConfig::number_of_concurrent_L0_readers; i++) {
    read_queue_[i] = 0;
  }
}

L0ZnsSSTable::~L0ZnsSSTable() = default;

Status L0ZnsSSTable::Recover() { return FromStatus(log_.RecoverPointers()); }

SSTableBuilder* L0ZnsSSTable::NewBuilder(SSZoneMetaData* meta) {
  return new SSTableBuilder(this, meta, ZnsConfig::use_sstable_encoding);
}

bool L0ZnsSSTable::EnoughSpaceAvailable(const Slice& slice) const {
#ifdef USE_COMMITTER
  return committer_.SpaceEnough(slice);
#else
  return log_.SpaceLeft(slice.size(), false);
#endif
}

uint64_t L0ZnsSSTable::SpaceAvailable() const { return log_.SpaceAvailable(); }

Status L0ZnsSSTable::WriteSSTable(const Slice& content, SSZoneMetaData* meta) {
  // The callee has to check beforehand if there is enough space.
  if (!EnoughSpaceAvailable(content)) {
    TROPODB_ERROR("Out of space L0\n");
    return Status::IOError("Not enough space available for L0");
  }
#ifdef USE_COMMITTER
  meta->L0.lba = log_.GetWriteHead();
  Status s = committer_.SafeCommit(content, &meta->lba_count);
  return s;
#else
  meta->L0.lba = log_.GetWriteHead();
  Status s = FromStatus(
      log_.Append(content.data(), content.size(), &meta->lba_count, false));
  // printf("Added L0 %lu %lu %lu \n", meta->L0.lba, meta->lba_count,
  //        content.size());
  return s;
#endif
}

Status L0ZnsSSTable::FlushMemTable(ZNSMemTable* mem,
                                   std::vector<SSZoneMetaData>& metas,
                                   uint8_t parallel_number) {
  Status s = Status::OK();
  SSZoneMetaData meta;
  meta.L0.log_number = parallel_number;
  SSTableBuilder* builder = NewBuilder(&meta);
  InternalIterator* iter = mem->NewIterator();
  iter->SeekToFirst();
  if (!iter->Valid()) {
    return Status::Corruption("No valid iterator in the memtable");
  }
  for (; iter->Valid(); iter->Next()) {
    const Slice& key = iter->key();
    const Slice& value = iter->value();
    s = builder->Apply(key, value);
    // Swap if necessary, we do not want enormous L0 -> L1 compactions.
    if ((builder->GetSize() + builder->EstimateSizeImpact(key, value) +
         lba_size_ - 1) /
            lba_size_ >=
        (ZnsConfig::max_bytes_sstable_l0 + lba_size_ - 1) / lba_size_) {
      builder->Finalise();
      s = builder->Flush();
      if (!s.ok()) {
        break;
      }
      metas.push_back(meta);
      delete builder;
      builder = NewBuilder(&meta);
    }
  }
  if (s.ok() && builder->GetSize() > 0) {
    s = builder->Finalise();
    s = builder->Flush();
    metas.push_back(meta);
  }
  delete builder;
  return s;
}

// TODO: this is better than locking around the entire read, but we have to
// investigate the performance.
uint8_t L0ZnsSSTable::request_read_queue() {
  uint8_t picked_reader = ZnsConfig::number_of_concurrent_L0_readers;
  mutex_.Lock();
  for (uint8_t i = 0; i < ZnsConfig::number_of_concurrent_L0_readers; i++) {
    if (read_queue_[i] == 0) {
      picked_reader = i;
      break;
    }
  }
  while (picked_reader >= ZnsConfig::number_of_concurrent_L0_readers) {
    cv_.Wait();
    for (uint8_t i = 0; i < ZnsConfig::number_of_concurrent_L0_readers; i++) {
      if (read_queue_[i] == 0) {
        picked_reader = i;
        break;
      }
    }
  }
  read_queue_[picked_reader] += 1;
  // printf("Claimed readerL0 %u %u\n", picked_reader,
  // read_queue_[picked_reader]);
  mutex_.Unlock();
  return picked_reader;
}

void L0ZnsSSTable::release_read_queue(uint8_t reader) {
  mutex_.Lock();
  assert(reader < ZnsConfig::number_of_concurrent_L0_readers &&
         read_queue_[reader] != 0);
  read_queue_[reader] = 0;
  // printf("Released readerL0 %u %u \n", reader, read_queue_[reader]);
  cv_.SignalAll();
  mutex_.Unlock();
}

Status L0ZnsSSTable::ReadSSTable(Slice* sstable, const SSZoneMetaData& meta) {
  Status s = Status::OK();
  if (meta.L0.lba > max_zone_head_ || meta.L0.lba < min_zone_head_ ||
      meta.lba_count > max_zone_head_ - min_zone_head_) {
    printf("Invalid metadata?\n");
    return Status::Corruption("Invalid metadata");
  }
  sstable->clear();
  // printf("Reading L0 from %lu \n", meta.L0.lba);
  // mutex_.Lock();
  uint8_t readernr = request_read_queue();
#ifdef USE_COMMITTER
  ZnsCommitReader reader;
  Slice record;
  std::string* raw_data = new std::string();
  bool succeeded_once = false;
  s = committer_.GetCommitReader(readernr, meta.L0.lba,
                                 meta.L0.lba + meta.lba_count, &reader);

  // printf("reading L0 %lu %lu from reader %u \n", meta.L0.lba, meta.lba_count,
  //        readernr);
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
#else
  char* data = new char[meta.lba_count * lba_size_];
  s = FromStatus(
      log_.Read(meta.L0.lba, data, meta.lba_count * lba_size_, true, readernr));
  release_read_queue(readernr);
  *sstable = Slice(data, meta.lba_count * lba_size_);
  if (!s.ok()) {
    TROPODB_ERROR("Error reading L0 table %lu at location %lu %lu\n",
                  meta.number, meta.L0.lba, meta.lba_count);
    exit(-1);
  }
  return Status::OK();
#endif
}

Status L0ZnsSSTable::TryInvalidateSSZones(
    const std::vector<SSZoneMetaData*>& metas,
    std::vector<SSZoneMetaData*>& remaining_metas) {
  if (metas.size() == 0) {
    return Status::Corruption();
  }
  SSZoneMetaData* prev = metas[0];
  SSZoneMetaData* mock = metas[0];
  // GUARANTEE, first deleted is equal to write tail
  if (log_.GetWriteTail() != prev->L0.lba) {
    uint64_t i = 0;
    while (i < metas.size()) {
      SSZoneMetaData* m = metas[i];
      // printf("Readding first %lu \n", m->number);
      remaining_metas.push_back(m);
      i++;
    }
    return Status::OK();
  }

  uint64_t blocks = prev->L0.lba - (prev->L0.lba / zone_cap_) * zone_cap_;
  blocks += prev->lba_count;

  uint64_t upto = 0;
  uint64_t blocks_to_delete = 0;
  uint64_t i = 1;
  for (; i < metas.size(); i++) {
    SSZoneMetaData* m = metas[i];
    // Get adjacents
    if (prev->number == m->number) {
      exit(-1);
      continue;
    }
    if (log_.wrapped_addr(prev->L0.lba + prev->lba_count) != m->L0.lba) {
      break;
    }
    // printf("Deleting %lu %lu %lu\n", m->number, m->L0.lba,
    //        m->L0.lba + m->lba_count);
    blocks += m->lba_count;
    prev = m;
    if (blocks >= zone_cap_) {
      mock->number = prev->number;
      blocks_to_delete += blocks;
      upto = i + 1;
      blocks = 0;
    }
  }
  if (blocks_to_delete % zone_cap_ != 0) {
    uint64_t safe = (blocks_to_delete / zone_cap_) * zone_cap_;
    mock->lba_count = blocks_to_delete - safe;
    blocks_to_delete = safe;
    mock->L0.lba = log_.wrapped_addr(log_.GetWriteTail() + blocks_to_delete);
    remaining_metas.push_back(mock);
    // printf("Mock delete %lu %lu %lu \n", mock->number, mock->L0.lba,
    //        mock->lba_count);
  }
  Status s = Status::OK();
  blocks_to_delete = (blocks_to_delete / zone_cap_) * zone_cap_;
  if (blocks_to_delete > 0) {
    // printf("Deleting from L0 %lu %lu \n", log_.GetWriteTail(),
    //        log_.GetWriteTail() + blocks_to_delete);
    s = FromStatus(log_.ConsumeTail(log_.GetWriteTail(),
                                    log_.GetWriteTail() + blocks_to_delete));
  }
  i = upto;
  while (i < metas.size()) {
    SSZoneMetaData* m = metas[i];
    // printf("Readding %lu %lu %lu \n", m->number, m->L0.lba, m->lba_count);
    remaining_metas.push_back(m);
    i++;
  }
  return s;
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
  // printf("Reading L0, meta: %lu %lu \n", meta.L0.lba, meta.lba_count);
  s = ReadSSTable(&sstable, meta);
  if (!s.ok()) {
    TROPODB_ERROR("Error reading L0\n");
    return nullptr;
  }
  char* data = (char*)sstable.data();
  if (ZnsConfig::use_sstable_encoding) {
    uint64_t size = DecodeFixed64(data);
    uint64_t count = DecodeFixed64(data + sizeof(uint64_t));
    if (size == 0 || count == 0) {
      TROPODB_ERROR("Reading corrupt L0 header %lu %lu \n", size, count);
    }
    return new SSTableIteratorCompressed(cmp, data, size, count);
  } else {
    uint64_t count = DecodeFixed32(data);
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
      TROPODB_ERROR("corrupt key in L0 SSTable\n");
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
