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
static const uint32_t kZnsHeaderSize = 4 + 2 + 1;
static const uint32_t kZnsRecordTypeLast =
    static_cast<uint32_t>(ZnsRecordType::kLastType) + 1;
static_assert(kZnsRecordTypeLast == 5);

/**
 * @brief ZnsCommiter is a helper class that can be used for persistent commits
 * in a log. It requires external synchronisation and verification!
 */
class ZnsCommitter {
 public:
  ZnsCommitter(SZD::SZDLog* log, const SZD::DeviceInfo& info);
  // No copying or implicits
  ZnsCommitter(const ZnsCommitter&) = delete;
  ZnsCommitter& operator=(const ZnsCommitter&) = delete;
  ~ZnsCommitter();

  bool SpaceEnough(const Slice& data);
  Status Commit(const Slice& data);
  Status SafeCommit(const Slice& data);

  // Get the commit
  bool GetCommitReader(uint64_t begin, uint64_t end);
  // Can not be called without first getting the commit
  bool SeekCommitReader(Slice* record);
  // Can not be called without first getting the commit
  bool CloseCommit();

 private:
  SZD::SZDLog* log_;
  uint64_t zone_size_;
  uint64_t lba_size_;
  uint64_t zasl_;
  // amortise copying
  SZD::SZDBuffer buffer_;
  // CRC
  uint32_t type_crc_[5];
  // Used for reading
  std::string* scratch_;
  uint64_t commit_start_, commit_ptr_, commit_end_;
  bool has_commit_;
};

}  // namespace ROCKSDB_NAMESPACE
#endif
#endif