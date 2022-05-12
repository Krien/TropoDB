#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_COMMITER_H
#define ZNS_COMMITER_H

#include "db/zns_impl/io/szd_port.h"
#include "db/zns_impl/memtable/zns_memtable.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {
// prepended Zns for namespaces issues
enum class ZnsRecordType : uint32_t {
  // All types that we do not support are coalesced to invalid
  kInvalid = 0,
  kFullType = 1,

  // for fragments
  kFirstType = 2,
  kMiddleType = 3,
  kLastType = 4
};
// Header is checksum (4 bytes), length (3 bytes), type (1 byte).
static const uint32_t kZnsHeaderSize = 4 + 3 + 1;
static const uint32_t kZnsRecordTypeLast =
    static_cast<uint32_t>(ZnsRecordType::kLastType) + 1;
static_assert(kZnsRecordTypeLast == 5);

/**
 * @brief ZnsCommiter is a helper class that can be used for persistent commits
 * in a log. It requires external synchronisation and verification!
 */
class ZnsCommitter {
 public:
  ZnsCommitter(SZD::SZDLog* log, const SZD::DeviceInfo& info, bool keep_buffer);
  // No copying or implicits
  ZnsCommitter(const ZnsCommitter&) = delete;
  ZnsCommitter& operator=(const ZnsCommitter&) = delete;
  ~ZnsCommitter();

  bool SpaceEnough(const Slice& data) const;
  Status Commit(const Slice& data, uint64_t* lbas = nullptr);
  Status SafeCommit(const Slice& data, uint64_t* lbas = nullptr);

  // Get the commit
  bool GetCommitReader(uint64_t begin, uint64_t end);
  // Can not be called without first getting the commit
  bool SeekCommitReader(Slice* record);
  // Can not be called without first getting the commit
  bool CloseCommit();

  // Clears buffer if it is filled.
  void ClearBuffer() {
    // buffer_.FreeBuffer();
  }

 private:
  SZD::SZDLog* log_;
  uint64_t zone_size_;
  uint64_t lba_size_;
  uint64_t zasl_;
  // amortise copying
  SZD::SZDBuffer buffer_;
  bool keep_buffer_;
  // CRC
  std::array<uint32_t, kZnsRecordTypeLast + 1> type_crc_;
  // Used for reading
  std::string scratch_;
  uint64_t commit_start_, commit_ptr_, commit_end_;
  bool has_commit_;
};

}  // namespace ROCKSDB_NAMESPACE
#endif
#endif