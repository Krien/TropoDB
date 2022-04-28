#include "db/zns_impl/persistence/zns_committer.h"

#include "db/write_batch_internal.h"
#include "db/zns_impl/io/szd_port.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/write_batch.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {
static void InitTypeCrc(uint32_t* type_crc) {
  for (uint32_t i = 0; i <= kZnsRecordTypeLast; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::Value(&t, 1);
  }
}

ZnsCommitter::ZnsCommitter(SZD::SZDChannel* channel,
                           const SZD::DeviceInfo& info)
    : channel_(channel),
      zone_size_(info.zone_size),
      lba_size_(info.lba_size),
      zasl_(info.zasl),
      buffer_(nullptr),
      scratch_(nullptr) {
  InitTypeCrc(type_crc_);
}
ZnsCommitter::~ZnsCommitter() {
  if (buffer_ != nullptr) {
    channel_->FreeBuffer();
    buffer_ = nullptr;
  }
  if (scratch_ != nullptr) {
    delete scratch_;
    scratch_ = nullptr;
  }
}

bool ZnsCommitter::SpaceEnough(const Slice& data, uint64_t min, uint64_t max) {
  size_t fragcount = data.size() / zasl_ + 1;
  size_t size_needed = fragcount * kZnsHeaderSize + data.size();
  size_needed = ((size_needed + lba_size_ - 1) / lba_size_) * lba_size_;
  size_t size_available = (max - min) * lba_size_;
  return size_needed < size_available;
}

Status ZnsCommitter::Commit(const Slice& data, uint64_t* addr) {
  Status s;
  const char* ptr = data.data();
  size_t left = data.size();

  if (!(s = FromStatus(channel_->ReserveBuffer(zasl_))).ok()) {
    return s;
  }
  char* fragment;
  if (!(s = FromStatus(channel_->GetBuffer((void**)&fragment))).ok()) {
    return s;
  }

  uint64_t write_head = *addr;
  uint64_t zone_head = (*addr / zone_size_) * zone_size_;

  bool begin = true;
  do {
    // determine next fragment part.
    size_t avail = ((zone_head + zone_size_) - write_head) * lba_size_;
    avail = (avail > zasl_ ? zasl_ : avail) - kZnsHeaderSize;
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
    memset(fragment, '\0', zasl_);  // Ensure no stale bits.
    fragment[4] = static_cast<char>(fragment_length & 0xffu);
    fragment[5] = static_cast<char>(fragment_length >> 8);
    fragment[6] = static_cast<char>(type);
    // CRC
    uint32_t crc = crc32c::Extend(type_crc_[static_cast<uint32_t>(type)], ptr,
                                  fragment_length);
    crc = crc32c::Mask(crc);
    EncodeFixed32(fragment, crc);
    // Actual commit
    memcpy(fragment + kZnsHeaderSize, ptr, fragment_length);
    s = FromStatus(channel_->FlushBufferSection(
        &write_head, 0, fragment_length + kZnsHeaderSize, false));
    zone_head = (write_head / zone_size_) * zone_size_;
    if (!s.ok()) {
      return s;
    }
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);
  s = FromStatus(channel_->FreeBuffer());
  *addr = write_head;
  return s;
}

Status ZnsCommitter::SafeCommit(const Slice& data, uint64_t* addr, uint64_t min,
                                uint64_t max) {
  if (*addr < min || *addr > max || min > max) {
    return Status::Corruption("Corrupt pointers");
  }
  if (!SpaceEnough(data, *addr, max)) {
    return Status::IOError("No space left");
  }
  return Commit(data, addr);
}

bool ZnsCommitter::GetCommitReader(uint64_t begin, uint64_t end) {
  if (begin >= end) {
    return false;
  }
  has_commit_ = true;
  commit_start_ = begin;
  commit_end_ = end;
  commit_ptr_ = commit_start_;
  if (scratch_ == nullptr) {
    scratch_ = new std::string;
  }
  if (buffer_ == nullptr) {
    if (!FromStatus(channel_->ReserveBuffer(zasl_)).ok()) {
      return false;
    }
    if (!FromStatus(channel_->GetBuffer((void**)&buffer_)).ok()) {
      return false;
    }
  }
  if (buffer_ == nullptr) {
    return false;
  }
  return true;
}

bool ZnsCommitter::SeekCommitReader(Slice* record) {
  if (!has_commit_) {
    printf("FATAL, be sure to first get a commit\n");
    return false;
  }
  if (commit_ptr_ >= commit_end_) {
    return false;
  }
  scratch_->clear();
  record->clear();
  bool in_fragmented_record = false;

  while (commit_ptr_ < commit_end_ && commit_ptr_ >= commit_start_) {
    const size_t to_read = (commit_end_ - commit_ptr_) * lba_size_ > zasl_
                               ? zasl_
                               : (commit_end_ - commit_ptr_) * lba_size_;
    // first read header (prevents reading too much)
    channel_->ReadIntoBuffer(commit_ptr_, 0, lba_size_, true);
    // parse header
    const char* header = buffer_;
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
    const uint32_t c = static_cast<uint32_t>(header[6]);
    ZnsRecordType type = c > kZnsRecordTypeLast
                             ? ZnsRecordType::kInvalid
                             : static_cast<ZnsRecordType>(header[6]);
    const uint32_t length = a | (b << 8);
    // read potential body
    if (length > lba_size_ && length <= to_read - kZnsHeaderSize) {
      channel_->ReadIntoBuffer(commit_ptr_ + 1, lba_size_, to_read - lba_size_,
                               true);
    }
    // TODO: we need better error handling at some point than setting to wrong
    // tag.
    if (kZnsHeaderSize + length > to_read) {
      type = ZnsRecordType::kInvalid;
    }
    {
      uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
      uint32_t actual_crc = crc32c::Value(header + 6, 1 + length);
      if (actual_crc != expected_crc) {
        printf("Corrupt crc %u %u %u\n", length, a, b);
        type = ZnsRecordType::kInvalid;
      }
    }
    commit_ptr_ += (length + lba_size_ - 1) / lba_size_;
    switch (type) {
      case ZnsRecordType::kFullType:
        scratch_->assign(header + kZnsHeaderSize, length);
        *record = Slice(*scratch_);
        return true;
      case ZnsRecordType::kFirstType:
        scratch_->assign(header + kZnsHeaderSize, length);
        in_fragmented_record = true;
        break;
      case ZnsRecordType::kMiddleType:
        if (!in_fragmented_record) {
        } else {
          scratch_->append(header + kZnsHeaderSize, length);
        }
        break;
      case ZnsRecordType::kLastType:
        if (!in_fragmented_record) {
        } else {
          scratch_->append(header + kZnsHeaderSize, length);
          *record = Slice(*scratch_);
          return true;
        }
        break;
      default:
        in_fragmented_record = false;
        scratch_->clear();
        return false;
        break;
    }
  }
  return false;
}
bool ZnsCommitter::CloseCommit() {
  if (!has_commit_) {
    return false;
  }
  has_commit_ = false;
  if (buffer_ != nullptr) {
    channel_->FreeBuffer();
    buffer_ = nullptr;
  }
  if (scratch_ != nullptr) {
    delete scratch_;
    scratch_ = nullptr;
  }
  return true;
}
}  // namespace ROCKSDB_NAMESPACE
