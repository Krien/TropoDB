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
    : log_(log),
      zone_size_(info.zone_size),
      lba_size_(info.lba_size),
      zasl_(info.zasl),
      buffer_(0, info.lba_size),
      keep_buffer_(keep_buffer),
      scratch_(ZnsConfig::deadbeef) {
  InitTypeCrc(type_crc_);
}

ZnsCommitter::~ZnsCommitter() {}

bool ZnsCommitter::SpaceEnough(const Slice& data) const {
  size_t fragcount = data.size() / zasl_ + 1;
  size_t size_needed = fragcount * kZnsHeaderSize + data.size();
  size_needed = ((size_needed + lba_size_ - 1) / lba_size_) * lba_size_;
  return log_->SpaceLeft(size_needed);
}

Status ZnsCommitter::Commit(const Slice& data, uint64_t* lbas) {
  Status s = Status::OK();
  const char* ptr = data.data();
  size_t left = data.size();

  if (!(s = FromStatus(buffer_.ReallocBuffer(zasl_))).ok()) {
    return s;
  }
  char* fragment;
  if (!(s = FromStatus(buffer_.GetBuffer((void**)&fragment))).ok()) {
    return s;
  }

  bool begin = true;
  uint64_t lbas_iter = 0;
  if (lbas != nullptr) {
    *lbas = 0;
  }
  do {
    uint64_t write_head = log_->GetWriteHead();
    uint64_t zone_head = (write_head / zone_size_) * zone_size_;
    // determine next fragment part.
    size_t avail = ((zone_head + zone_size_) - write_head) * lba_size_;
    avail = (avail > zasl_ ? zasl_ : avail);
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
    memset(fragment, '\0', zasl_);  // Ensure no stale bits.
    memcpy(fragment + kZnsHeaderSize, ptr, fragment_length);  // new body.
    // Build header
    fragment[4] = static_cast<char>(fragment_length & 0xffu);
    fragment[5] = static_cast<char>((fragment_length >> 8) & 0xffu);
    fragment[6] = static_cast<char>((fragment_length >> 16) & 0xffu);
    fragment[7] = static_cast<char>(type);
    // CRC
    uint32_t crc = crc32c::Extend(type_crc_[static_cast<uint32_t>(type)], ptr,
                                  fragment_length);
    crc = crc32c::Mask(crc);
    EncodeFixed32(fragment, crc);
    // Actual commit
    s = FromStatus(log_->Append(buffer_, 0, fragment_length + kZnsHeaderSize,
                                &lbas_iter, false));
    if (lbas != nullptr) {
      *lbas += lbas_iter;
    }
    if (!s.ok()) {
      return s;
    }
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);
  if (!keep_buffer_) {
    s = FromStatus(buffer_.FreeBuffer());
  }
  return s;
}  // namespace ROCKSDB_NAMESPACE

Status ZnsCommitter::SafeCommit(const Slice& data, uint64_t* lbas) {
  if (!SpaceEnough(data)) {
    return Status::IOError("No space left");
  }
  return Commit(data, lbas);
}

bool ZnsCommitter::GetCommitReader(uint64_t begin, uint64_t end) {
  if (begin >= end) {
    return false;
  }
  has_commit_ = true;
  commit_start_ = begin;
  commit_end_ = end;
  commit_ptr_ = commit_start_;
  if (!FromStatus(buffer_.ReallocBuffer(zasl_)).ok()) {
    return false;
  }
  return true;
}

bool ZnsCommitter::SeekCommitReader(Slice* record) {
  if (!has_commit_ || buffer_.GetBufferSize() == 0) {
    printf("FATAL, be sure to first get a commit\n");
    return false;
  }
  if (commit_ptr_ >= commit_end_) {
    return false;
  }
  scratch_.clear();
  record->clear();
  bool in_fragmented_record = false;

  while (commit_ptr_ < commit_end_ && commit_ptr_ >= commit_start_) {
    const size_t to_read = (commit_end_ - commit_ptr_) * lba_size_ > zasl_
                               ? zasl_
                               : (commit_end_ - commit_ptr_) * lba_size_;
    // first read header (prevents reading too much)
    log_->Read(commit_ptr_, &buffer_, 0, lba_size_, true);
    // parse header
    const char* header;
    buffer_.GetBuffer((void**)&header);
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
    const uint32_t c = static_cast<uint32_t>(header[6]) & 0xff;
    const uint32_t d = static_cast<uint32_t>(header[7]);
    ZnsRecordType type = d > kZnsRecordTypeLast ? ZnsRecordType::kInvalid
                                                : static_cast<ZnsRecordType>(d);
    const uint32_t length = a | (b << 8) | (c << 16);
    // read potential body
    if (length > lba_size_ && length <= to_read - kZnsHeaderSize) {
      buffer_.ReallocBuffer(to_read);
      buffer_.GetBuffer((void**)&header);
      // TODO: Could also skip first block, but atm addr is bugged.
      log_->Read(commit_ptr_, &buffer_, 0, to_read, true);
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
        printf("Corrupt crc %u %u\n", length, d);
        type = ZnsRecordType::kInvalid;
      }
    }
    commit_ptr_ += (length + lba_size_ - 1) / lba_size_;
    switch (type) {
      case ZnsRecordType::kFullType:
        scratch_.assign(header + kZnsHeaderSize, length);
        *record = Slice(scratch_);
        return true;
      case ZnsRecordType::kFirstType:
        scratch_.assign(header + kZnsHeaderSize, length);
        in_fragmented_record = true;
        break;
      case ZnsRecordType::kMiddleType:
        if (!in_fragmented_record) {
        } else {
          scratch_.append(header + kZnsHeaderSize, length);
        }
        break;
      case ZnsRecordType::kLastType:
        if (!in_fragmented_record) {
        } else {
          scratch_.append(header + kZnsHeaderSize, length);
          *record = Slice(scratch_);
          return true;
        }
        break;
      default:
        in_fragmented_record = false;
        scratch_.clear();
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
  if (!keep_buffer_) {
    buffer_.FreeBuffer();
  }
  scratch_.clear();
  return true;
}
}  // namespace ROCKSDB_NAMESPACE
