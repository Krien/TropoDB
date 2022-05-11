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
                            uint32_t data_size, uint32_t num_restarts);
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

  void Next() override {
    assert(Valid());
    ParseNextKey();
  }

  void SeekToFirst() override {
    SeekToRestartPoint(0);
    ParseNextKey();
  }

  void SeekToLast() override {
    SeekToRestartPoint(num_restarts_ - 1);
    while (ParseNextKey() && NextEntryOffset() <= data_size_) {
      // Keep skipping
    }
  }
  void SeekForPrev(const Slice& target) {
    Seek(target);
    Prev();
  }

  void Prev() override;
  void Seek(const Slice& target) override;

 private:
  void CorruptionError();
  bool ParseNextKey();
  inline int Compare(const Slice& a, const Slice& b) const {
    return comparator_->Compare(a, b);
  }
  // Return the offset in data_ just past the end of the current entry.
  inline uint32_t NextEntryOffset() const {
    return (value_.data() + value_.size()) - data_;
  }
  uint32_t GetRestartPoint(uint32_t index) {
    assert(index < num_restarts_);
    return DecodeFixed32(data_ + (index + 2) * sizeof(uint32_t)) +
           kv_pairs_offset_;
  }
  void SeekToRestartPoint(uint32_t index) {
    key_.clear();
    restart_index_ = index;
    // current_ will be fixed by ParseNextKey();

    // ParseNextKey() starts at the end of value_, so set value_ accordingly
    uint32_t offset = GetRestartPoint(index);
    value_ = Slice(data_ + offset, 0);
  }

  const Comparator* const comparator_;
  char* data_;                   // underlying block contents
  uint32_t const num_restarts_;  // Number of uint32_t entries in restart array
  const uint32_t kv_pairs_offset_;

  // current_ is offset in data_ of current entry.  >= restarts_ if !Valid
  uint32_t current_;
  uint32_t restart_index_;  // Index of restart block in which current_ falls
  uint32_t data_size_;      // size of data array
  std::string key_;
  Slice value_;
  Status status_;
};
}  // namespace ROCKSDB_NAMESPACE

#endif
#endif
