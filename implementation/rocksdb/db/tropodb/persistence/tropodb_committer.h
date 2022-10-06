#pragma once
#ifdef TROPODB_PLUGIN_ENABLED
#ifndef ZNS_COMMITER_H
#define ZNS_COMMITER_H

#include "db/tropodb/tropodb_config.h"
#include "db/tropodb/io/szd_port.h"
#include "db/tropodb/memtable/tropodb_memtable.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {
// prepended Zns for namespaces issues
enum class TropoRecordType : uint32_t {
  // All types that we do not support are coalesced to invalid
  kInvalid = 0,
  kFullType = 1,

  // for fragments
  kFirstType = 2,
  kMiddleType = 3,
  kLastType = 4
};
// Header is checksum (4 bytes), length (3 bytes), type (1 byte).
static const uint32_t kTropoHeaderSize = 4 + 3 + 1;
static const uint32_t kTropoRecordTypeLast =
    static_cast<uint32_t>(TropoRecordType::kLastType) + 1;
static_assert(kTropoRecordTypeLast == 5);

struct TropoCommitReader {
  uint64_t commit_start;
  uint64_t commit_end;
  uint64_t commit_ptr;
  uint8_t reader_nr;
  std::string scratch;
};

struct TropoCommitReaderString {
  uint64_t commit_start;
  uint64_t commit_end;
  uint64_t commit_ptr;
  std::string* in;
  std::string scratch;
};

/**
 * @brief ZnsCommiter is a helper class that can be used for persistent commits
 * in a log. It requires external synchronisation and verification!
 */
class TropoCommitter {
 public:
  TropoCommitter(SZD::SZDLog* log, const SZD::DeviceInfo& info, bool keep_buffer);
  // No copying or implicits
  TropoCommitter(const TropoCommitter&) = delete;
  TropoCommitter& operator=(const TropoCommitter&) = delete;
  ~TropoCommitter();

  size_t SpaceNeeded(size_t data_size) const;
  bool SpaceEnough(size_t size) const;
  bool SpaceEnough(const Slice& data) const;
  Status CommitToCharArray(const Slice& in, char** out);
  Status Commit(const Slice& data, uint64_t* lbas = nullptr);
  Status SafeCommit(const Slice& data, uint64_t* lbas = nullptr);

  // Get the commit
  Status GetCommitReader(uint8_t reader_number, uint64_t begin, uint64_t end,
                         TropoCommitReader* reader);
  // Can not be called without first getting the commit
  bool SeekCommitReader(TropoCommitReader& reader, Slice* record);
  // Can not be called without first getting the commit
  bool CloseCommit(TropoCommitReader& reader);

  Status GetCommitReaderString(std::string* in, TropoCommitReaderString* reader);
  bool SeekCommitReaderString(TropoCommitReaderString& reader, Slice* record);
  bool CloseCommitString(TropoCommitReaderString& reader);

  //TODO: Remove?
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
  std::array<uint32_t, kTropoRecordTypeLast + 1> type_crc_;
};

}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
