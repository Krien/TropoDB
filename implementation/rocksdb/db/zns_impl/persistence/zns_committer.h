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

struct ZnsCommitReader {
  uint64_t commit_start;
  uint64_t commit_end;
  uint64_t commit_ptr;
  uint8_t reader_nr;
  std::string scratch;
};

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
  Status CommitToString(const Slice& in, std::string* out);
  Status Commit(const Slice& data, uint64_t* lbas = nullptr);
  Status SafeCommit(const Slice& data, uint64_t* lbas = nullptr);

  // Get the commit
  Status GetCommitReader(uint8_t reader_number, uint64_t begin, uint64_t end,
                         ZnsCommitReader* reader);
  // Can not be called without first getting the commit
  bool SeekCommitReader(ZnsCommitReader& reader, Slice* record);
  // Can not be called without first getting the commit
  bool CloseCommit(ZnsCommitReader& reader);

  // Clears buffer if it is filled.
  void ClearBuffer() {
    // buffer_.FreeBuffer();
  }

 private:
  const uint64_t zone_cap_;
  const uint64_t lba_size_;
  const uint64_t zasl_;
  const uint8_t number_of_readers_;
  SZD::SZDLog* log_;
  // amortise copying
  SZD::SZDBuffer** read_buffer_;
  SZD::SZDBuffer write_buffer_;
  bool keep_buffer_;
  // CRC
  std::array<uint32_t, kZnsRecordTypeLast + 1> type_crc_;
};

}  // namespace ROCKSDB_NAMESPACE
#endif
#endif