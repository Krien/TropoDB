#include "db/zns_impl/persistence/zns_committer.h"

#include "db/write_batch_internal.h"
#include "db/zns_impl/config.h"
#include "db/zns_impl/io/szd_port.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/write_batch.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {
static void InitTypeCrc(
    std::array<uint32_t, kZnsRecordTypeLast + 1>& type_crc) {
  for (uint32_t i = 0; i <= type_crc.size(); i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::Value(&t, 1);
  }
}

ZnsCommitter::ZnsCommitter(SZD::SZDLog* log, const SZD::DeviceInfo& info,
                           bool keep_buffer)
    : zone_cap_(info.zone_cap),
      lba_size_(info.lba_size),
      zasl_(info.zasl),
      number_of_readers_(log->GetNumberOfReaders()),
      log_(log),
      read_buffer_(nullptr),
      write_buffer_(0, info.lba_size),
      keep_buffer_(keep_buffer) {
  InitTypeCrc(type_crc_);

  read_buffer_ = new SZD::SZDBuffer*[number_of_readers_];
  for (uint8_t i = 0; i < number_of_readers_; i++) {
    read_buffer_[i] = new SZD::SZDBuffer(0, lba_size_);
  }
}

ZnsCommitter::~ZnsCommitter() {
  for (uint8_t i = 0; i < number_of_readers_; i++) {
    delete read_buffer_[i];
  }
  delete[] read_buffer_;
}

bool ZnsCommitter::SpaceEnough(const Slice& data) const {
  size_t fragcount = data.size() / lba_size_ + 1;
  size_t size_needed = fragcount * kZnsHeaderSize + data.size();
  size_needed = ((size_needed + lba_size_ - 1) / lba_size_) * lba_size_;
  return log_->SpaceLeft(size_needed);
}

Status ZnsCommitter::CommitToString(const Slice& in, std::string* out) {
  Status s = Status::OK();
  const char* ptr = in.data();
  size_t walker = 0;
  size_t left = in.size();

  size_t fragcount = in.size() / lba_size_ + 1;
  size_t size_needed = fragcount * kZnsHeaderSize + in.size();
  size_needed = ((size_needed + lba_size_ - 1) / lba_size_) * lba_size_;

  char* fragment = new char[size_needed];

  bool begin = true;
  do {
    // determine next fragment part.
    size_t avail = lba_size_;
    avail = avail > kZnsHeaderSize ? avail - kZnsHeaderSize : 0;
    const size_t fragment_length = (left < avail) ? left : avail;

    ZnsRecordType type;
    const bool end = (left == fragment_length);
    if (begin && end) {
      type = ZnsRecordType::kFullType;
    } else if (begin) {
      type = ZnsRecordType::kFirstType;
    } else if (end) {
      type = ZnsRecordType::kLastType;
    } else {
      type = ZnsRecordType::kMiddleType;
    }
    size_t frag_begin_addr = walker;

    memset(fragment + frag_begin_addr, 0, lba_size_);  // Ensure no stale bits.
    memcpy(fragment + frag_begin_addr + kZnsHeaderSize, ptr,
           fragment_length);  // new body.
    // Build header
    fragment[frag_begin_addr + 4] = static_cast<char>(fragment_length & 0xffu);
    fragment[frag_begin_addr + 5] =
        static_cast<char>((fragment_length >> 8) & 0xffu);
    fragment[frag_begin_addr + 6] =
        static_cast<char>((fragment_length >> 16) & 0xffu);
    fragment[frag_begin_addr + 7] = static_cast<char>(type);
    // CRC
    uint32_t crc = crc32c::Extend(type_crc_[static_cast<uint32_t>(type)], ptr,
                                  fragment_length);
    crc = crc32c::Mask(crc);
    EncodeFixed32(fragment + frag_begin_addr, crc);
    walker += fragment_length + kZnsHeaderSize;
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);
  out->append(fragment, size_needed);
  return s;
}

Status ZnsCommitter::Commit(const Slice& data, uint64_t* lbas) {
  Status s = Status::OK();
  const char* ptr = data.data();
#ifdef DIRECT_COMMIT
  size_t walker = 0;
#endif
  size_t left = data.size();

  size_t fragcount = data.size() / lba_size_ + 1;
  size_t size_needed = fragcount * kZnsHeaderSize + data.size();
  size_needed = ((size_needed + lba_size_ - 1) / lba_size_) * lba_size_;

  if (!(s = FromStatus(write_buffer_.ReallocBuffer(
#ifdef DIRECT_COMMIT
            size_needed
#else
            lba_size_
#endif
            )))
           .ok()) {
    return s;
  }
  char* fragment;
  if (!(s = FromStatus(write_buffer_.GetBuffer((void**)&fragment))).ok()) {
    return s;
  }

  bool begin = true;
  uint64_t lbas_iter = 0;
  if (lbas != nullptr) {
    *lbas = 0;
  }
  do {
    // determine next fragment part.
    size_t avail = lba_size_;
    avail = avail > kZnsHeaderSize ? avail - kZnsHeaderSize : 0;
    const size_t fragment_length = (left < avail) ? left : avail;

    ZnsRecordType type;
    const bool end = (left == fragment_length);
    if (begin && end) {
      type = ZnsRecordType::kFullType;
    } else if (begin) {
      type = ZnsRecordType::kFirstType;
    } else if (end) {
      type = ZnsRecordType::kLastType;
    } else {
      type = ZnsRecordType::kMiddleType;
    }
    size_t frag_begin_addr = 0;
#ifdef DIRECT_COMMIT
    frag_begin_addr = walker;
#endif

    memset(fragment + frag_begin_addr, 0, lba_size_);  // Ensure no stale bits.
    memcpy(fragment + frag_begin_addr + kZnsHeaderSize, ptr,
           fragment_length);  // new body.
    // Build header
    fragment[frag_begin_addr + 4] = static_cast<char>(fragment_length & 0xffu);
    fragment[frag_begin_addr + 5] =
        static_cast<char>((fragment_length >> 8) & 0xffu);
    fragment[frag_begin_addr + 6] =
        static_cast<char>((fragment_length >> 16) & 0xffu);
    fragment[frag_begin_addr + 7] = static_cast<char>(type);
    // CRC
    uint32_t crc = crc32c::Extend(type_crc_[static_cast<uint32_t>(type)], ptr,
                                  fragment_length);
    crc = crc32c::Mask(crc);
    EncodeFixed32(fragment + frag_begin_addr, crc);
#ifdef DIRECT_COMMIT
    walker += fragment_length + kZnsHeaderSize;
#else
    // Actual commit
    s = FromStatus(log_->Append(
        write_buffer_, 0, fragment_length + kZnsHeaderSize, &lbas_iter, false));
    if (lbas != nullptr) {
      *lbas += lbas_iter;
    }
    if (!s.ok()) {
      printf("Fatal append error\n");
      return s;
    }
#endif
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);
#ifdef DIRECT_COMMIT
  // printf("SIZE %lu \n", data.size());
  s = FromStatus(
      log_->Append(write_buffer_, 0, size_needed, &lbas_iter, false));
  if (lbas != nullptr) {
    *lbas += lbas_iter;
  }
#endif

  if (!keep_buffer_) {
    s = FromStatus(write_buffer_.FreeBuffer());
  }
  return s;
}  // namespace ROCKSDB_NAMESPACE

Status ZnsCommitter::SafeCommit(const Slice& data, uint64_t* lbas) {
  if (!SpaceEnough(data)) {
    printf("No space left\n");
    return Status::IOError("No space left");
  }
  return Commit(data, lbas);
}

Status ZnsCommitter::GetCommitReader(uint8_t reader_number, uint64_t begin,
                                     uint64_t end, ZnsCommitReader* reader) {
  if (begin >= end || reader_number >= number_of_readers_) {
    return Status::InvalidArgument();
  }
  reader->commit_start = begin;
  reader->commit_end = end;
  reader->commit_ptr = reader->commit_start;
  reader->reader_nr = reader_number;
  reader->scratch = ZnsConfig::deadbeef;
  if (!FromStatus(read_buffer_[reader->reader_nr]->ReallocBuffer(lba_size_))
           .ok()) {
    return Status::MemoryLimit();
  }

  return Status::OK();
}

bool ZnsCommitter::SeekCommitReader(ZnsCommitReader& reader, Slice* record) {
  // buffering issue
  if (read_buffer_[reader.reader_nr]->GetBufferSize() == 0) {
    printf("FATAL, be sure to first get a commit\n");
    return false;
  }
  if (reader.commit_ptr >= reader.commit_end) {
    return false;
  }
  reader.scratch.clear();
  record->clear();
  bool in_fragmented_record = false;

  while (reader.commit_ptr < reader.commit_end &&
         reader.commit_ptr >= reader.commit_start) {
    const size_t to_read =
        (reader.commit_end - reader.commit_ptr) * lba_size_ > lba_size_
            ? lba_size_
            : (reader.commit_end - reader.commit_ptr) * lba_size_;
    // first read header (prevents reading too much)
    log_->Read(reader.commit_ptr, *(&read_buffer_[reader.reader_nr]), 0,
               lba_size_, true, reader.reader_nr);
    // printf("Reading with reader %u \n", reader.reader_nr);
    // parse header
    const char* header;
    read_buffer_[reader.reader_nr]->GetBuffer((void**)&header);
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
    const uint32_t c = static_cast<uint32_t>(header[6]) & 0xff;
    const uint32_t d = static_cast<uint32_t>(header[7]);
    ZnsRecordType type = d > kZnsRecordTypeLast ? ZnsRecordType::kInvalid
                                                : static_cast<ZnsRecordType>(d);
    const uint32_t length = a | (b << 8) | (c << 16);
    // read potential body
    if (length > lba_size_ && length <= to_read - kZnsHeaderSize) {
      read_buffer_[reader.reader_nr]->ReallocBuffer(to_read);
      read_buffer_[reader.reader_nr]->GetBuffer((void**)&header);
      // TODO: Could also skip first block, but atm addr is bugged.
      log_->Read(reader.commit_ptr, *(&read_buffer_[reader.reader_nr]), 0,
                 to_read, true, reader.reader_nr);
    }
    // TODO: we need better error handling at some point than setting to wrong
    // tag.
    if (kZnsHeaderSize + length > to_read) {
      type = ZnsRecordType::kInvalid;
    }
    // Validate CRC
    {
      uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
      uint32_t actual_crc = crc32c::Value(header + 7, 1 + length);
      if (actual_crc != expected_crc) {
        printf("Corrupt crc %u %u %lu %lu\n", length, d, reader.commit_ptr,
               reader.commit_end);
        type = ZnsRecordType::kInvalid;
      }
    }
    reader.commit_ptr += (length + lba_size_ - 1) / lba_size_;
    switch (type) {
      case ZnsRecordType::kFullType:
        reader.scratch.assign(header + kZnsHeaderSize, length);
        *record = Slice(reader.scratch);
        return true;
      case ZnsRecordType::kFirstType:
        reader.scratch.assign(header + kZnsHeaderSize, length);
        in_fragmented_record = true;
        break;
      case ZnsRecordType::kMiddleType:
        if (!in_fragmented_record) {
        } else {
          reader.scratch.append(header + kZnsHeaderSize, length);
        }
        break;
      case ZnsRecordType::kLastType:
        if (!in_fragmented_record) {
        } else {
          reader.scratch.append(header + kZnsHeaderSize, length);
          *record = Slice(reader.scratch);
          return true;
        }
        break;
      default:
        in_fragmented_record = false;
        reader.scratch.clear();
        return false;
        break;
    }
  }
  return false;
}
bool ZnsCommitter::CloseCommit(ZnsCommitReader& reader) {
  if (!keep_buffer_) {
    read_buffer_[reader.reader_nr]->FreeBuffer();
  }
  reader.scratch.clear();
  return true;
}
}  // namespace ROCKSDB_NAMESPACE
