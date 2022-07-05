#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_SSTABLE_ITERATOR_COMPRESSED_H
#define ZNS_SSTABLE_ITERATOR_COMPRESSED_H

#include "db/dbformat.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
/**
 * @brief Simple iterator, that reads the data of an SSTable once but parses it
 * lazily.
 */
class SSTableIteratorCompressed : public Iterator {
 public:
  SSTableIteratorCompressed(const Comparator* comparator, char* data,
                            uint64_t data_size, uint64_t num_restarts);
  ~SSTableIteratorCompressed();
  bool Valid() const override {
    return current_ < data_size_ && current_ >= kv_pairs_offset_ &&
           restart_index_ <= num_restarts_;
  }
  Status status() const override { return status_; }
  Slice key() const override {
    assert(Valid());
    return key_;
  }
  Slice value() const override {
    assert(Valid());
    return value_;
  }
  void Next() override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void SeekForPrev(const Slice& target) override;
  void Prev() override;
  void Seek(const Slice& target) override;

 private:
  void CorruptionError();
  bool ParseNextKey();
  inline int Compare(const Slice& a, const Slice& b) const {
    return comparator_->Compare(a, b);
  }
  // Return the offset in data_ just past the end of the current entry.
  inline uint64_t NextEntryOffset() const {
    return (value_.data() + value_.size()) - data_;
  }
  uint64_t GetRestartPoint(uint64_t index);
  void SeekToRestartPoint(uint64_t index);

  const Comparator* const comparator_;
  char* data_;                   // underlying block contents
  uint64_t const num_restarts_;  // Number of uint32_t entries in restart array
  const uint64_t kv_pairs_offset_;

  // current_ is offset in data_ of current entry.  >= restarts_ if !Valid
  uint64_t current_;
  uint64_t restart_index_;  // Index of restart block in which current_ falls
  uint64_t data_size_;      // size of data array
  std::string key_;
  Slice value_;
  Status status_;
};
}  // namespace ROCKSDB_NAMESPACE

#endif
#endif
