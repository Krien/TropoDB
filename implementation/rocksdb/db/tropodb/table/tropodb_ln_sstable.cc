#include "db/tropodb/table/tropodb_ln_sstable.h"

#include "db/tropodb/io/szd_port.h"
#include "db/tropodb/table/iterators/sstable_iterator.h"
#include "db/tropodb/table/iterators/sstable_iterator_compressed.h"
#include "db/tropodb/table/tropodb_sstable.h"
#include "db/tropodb/table/tropodb_sstable_builder.h"
#include "db/tropodb/table/tropodb_sstable_reader.h"
#include "db/tropodb/tropodb_config.h"
#include "db/tropodb/utils/tropodb_logger.h"

namespace ROCKSDB_NAMESPACE {

TropoLNSSTable::TropoLNSSTable(SZD::SZDChannelFactory* channel_factory,
                               const SZD::DeviceInfo& info,
                               const uint64_t min_zone_nr,
                               const uint64_t max_zone_nr)
    : TropoSSTable(channel_factory, info, min_zone_nr, max_zone_nr),
      log_(channel_factory_, info, min_zone_nr, max_zone_nr,
           TropoDBConfig::number_of_concurrent_LN_readers, 2),
      cv_(&mutex_) {
  // unset
  for (uint8_t i = 0; i < TropoDBConfig::number_of_concurrent_LN_readers; i++) {
    read_queue_[i] = 0;
  }
}

TropoLNSSTable::~TropoLNSSTable() = default;

Status TropoLNSSTable::Recover() { return Status::OK(); }

Status TropoLNSSTable::Recover(const std::string& from) {
  return FromStatus(log_.DecodeFrom(from.data(), from.size()));
}

std::string TropoLNSSTable::Encode() { return log_.Encode(); }

TropoSSTableBuilder* TropoLNSSTable::NewBuilder(SSZoneMetaData* meta) {
  return new TropoSSTableBuilder(this, meta,
                                 TropoDBConfig::use_sstable_encoding);
}

TropoSSTableBuilder* TropoLNSSTable::NewLNBuilder(SSZoneMetaData* meta) {
  return new TropoSSTableBuilder(this, meta,
                                 TropoDBConfig::use_sstable_encoding, 1);
}

bool TropoLNSSTable::EnoughSpaceAvailable(const Slice& slice) const {
  return log_.SpaceLeft(slice.size(), false);
}

uint64_t TropoLNSSTable::SpaceAvailable() const {
  return log_.SpaceAvailable();
}

Status TropoLNSSTable::WriteSSTable(const Slice& content, SSZoneMetaData* meta,
                                    uint8_t writer) {
  // The callee has to check beforehand if there is enough space.
  if (!EnoughSpaceAvailable(content)) {
    TROPO_LOG_ERROR("ERROR: LN SSTable: out of space LN %lu %lu \n",
                    content.size() / lba_size_,
                    log_.SpaceAvailable() / lba_size_);
    return Status::IOError("Not enough space available for LN");
  }

  std::vector<std::pair<uint64_t, uint64_t>> ptrs;
  if (!FromStatus(
           log_.Append(content.data(), content.size(), ptrs, false, writer))
           .ok()) {
    TROPO_LOG_ERROR("ERROR: LN SSTable: Failed appending to fragmented log\n");
    return Status::IOError("Error during appending\n");
  }
  meta->lba_count = 0;
  meta->LN.lba_regions = 0;
  for (auto ptr : ptrs) {
    meta->LN.lbas[meta->LN.lba_regions] = ptr.first * zone_cap_;
    meta->LN.lba_region_sizes[meta->LN.lba_regions] = ptr.second * zone_cap_;
    // strictly safer, but not really necessary. If errors occur, investigate
    // this line.
    meta->lba_count += meta->LN.lba_region_sizes[meta->LN.lba_regions];
    meta->LN.lba_regions++;
  }
  // meta->lba_count += (content.size() + lba_size_ - 1) / lba_size_;
  return Status::OK();
}

Status TropoLNSSTable::WriteSSTable(const Slice& content,
                                    SSZoneMetaData* meta) {
  return WriteSSTable(content, meta, 0);
}

// TODO: this is better than locking around the entire read, but we have to
// investigate the performance.
uint8_t TropoLNSSTable::request_read_queue() {
  uint8_t picked_reader = TropoDBConfig::number_of_concurrent_LN_readers;
  mutex_.Lock();
  for (uint8_t i = 0; i < TropoDBConfig::number_of_concurrent_LN_readers; i++) {
    if (read_queue_[i] == 0) {
      picked_reader = i;
      break;
    }
  }
  while (picked_reader >= TropoDBConfig::number_of_concurrent_LN_readers) {
    cv_.Wait();
    for (uint8_t i = 0; i < TropoDBConfig::number_of_concurrent_LN_readers;
         i++) {
      if (read_queue_[i] == 0) {
        picked_reader = i;
        break;
      }
    }
  }
  read_queue_[picked_reader] += 1;
  mutex_.Unlock();
  return picked_reader;
}

void TropoLNSSTable::release_read_queue(uint8_t reader) {
  mutex_.Lock();
  assert(reader < TropoDBConfig::number_of_concurrent_LN_readers &&
         read_queue_[reader] != 0);
  read_queue_[reader] = 0;
  cv_.SignalAll();
  mutex_.Unlock();
}

Status TropoLNSSTable::ReadSSTable(Slice* sstable, const SSZoneMetaData& meta) {
  Status s = Status::OK();
  if (meta.LN.lba_regions > 8) {
    return Status::Corruption("Invalid metadata");
  }

  std::vector<std::pair<uint64_t, uint64_t>> ptrs;
  for (size_t i = 0; i < meta.LN.lba_regions; i++) {
    uint64_t from = meta.LN.lbas[i];
    uint64_t blocks = meta.LN.lba_region_sizes[i];
    if (from > max_zone_head_ || from < min_zone_head_) {
      TROPO_LOG_ERROR("ERROR: LN SSTable: Invalid metadata\n");
      return Status::Corruption("Invalid metadata");
    }
    ptrs.push_back(std::make_pair(from / zone_cap_, blocks / zone_cap_));
  }

  char* buffer = new char[meta.lba_count * lba_size_];
  // mutex_.Lock();
  uint8_t readernr = request_read_queue();
  s = FromStatus(
      log_.Read(ptrs, buffer, meta.lba_count * lba_size_, true, readernr));
  release_read_queue(readernr);
  // mutex_.Unlock();
  if (!s.ok()) {
    TROPO_LOG_ERROR("ERROR: LN SSTable: Failed reading\n");
    delete[] buffer;
    return s;
  }
  *sstable = Slice(buffer, meta.lba_count * lba_size_);
  return s;
}

Status TropoLNSSTable::InvalidateSSZone(const SSZoneMetaData& meta) {
  std::vector<std::pair<uint64_t, uint64_t>> ptrs;
  for (size_t i = 0; i < meta.LN.lba_regions; i++) {
    uint64_t from = meta.LN.lbas[i];
    uint64_t blocks = meta.LN.lba_region_sizes[i];
    if (from > max_zone_head_ || from < min_zone_head_) {
      TROPO_LOG_ERROR("ERROR: LN SSTable: LN delete out of range\n");
      return Status::Corruption("LN delete out of range");
    }
    ptrs.push_back(std::make_pair(from / zone_cap_, blocks / zone_cap_));
  }
  return FromStatus(log_.Reset(ptrs, 1));
}

Iterator* TropoLNSSTable::NewIterator(const SSZoneMetaData& meta,
                                      const Comparator* cmp) {
  Status s;
  Slice sstable;
  s = ReadSSTable(&sstable, meta);
  if (!s.ok()) {
    return nullptr;
  }
  char* data = (char*)sstable.data();
  if (TropoDBConfig::use_sstable_encoding) {
    uint64_t size = DecodeFixed64(data);
    uint64_t count = DecodeFixed64(data + sizeof(uint64_t));
    if (size == 0) {
      TROPO_LOG_ERROR("ERROR: LN SSTable: Corrupt table SIZE %lu COUNT %lu \n",
                      size, count);
    }
    return new SSTableIteratorCompressed(cmp, data, size, count);
  } else {
    uint64_t count = DecodeFixed64(data);
    return new SSTableIterator(data, sstable.size(), (size_t)count,
                               &TropoEncoding::ParseNextNonEncoded, cmp);
  }
}

Status TropoLNSSTable::Get(const InternalKeyComparator& icmp,
                           const Slice& key_ptr, std::string* value_ptr,
                           const SSZoneMetaData& meta, EntryStatus* status) {
  Iterator* it = NewIterator(meta, icmp.user_comparator());
  if (it == nullptr) {
    TROPO_LOG_ERROR("ERROR: LN SSTable: Corrupt iterator\n");
    return Status::Corruption();
  }
  it->Seek(key_ptr);
  if (it->Valid()) {
    ParsedInternalKey parsed_key;
    if (!ParseInternalKey(it->key(), &parsed_key, false).ok()) {
      TROPO_LOG_ERROR("ERROR: LN SSTable: Corrupt key found\n");
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
