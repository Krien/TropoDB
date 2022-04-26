#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_SSTABLE_ITERATOR_H
#define ZNS_SSTABLE_ITERATOR_H

#include "db/dbformat.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
/**
 @brief Should make progress in the string and return the next kv pair.
 Generic to differentiate between different SSTable implementations.
*/
typedef void (*NextPair)(char** src, Slice* key, Slice* value);
/**
 * @brief Simple iterator, that reads the data of an SSTable once but parses it
 * lazily.
 */
class SSTableIterator : public Iterator {
 public:
  SSTableIterator(char* data, size_t data_size, size_t count, NextPair nextf,
                  const InternalKeyComparator& icmp);
  ~SSTableIterator();
  bool Valid() const override { return index_ <= count_ && count_ > 0; }
  Slice key() const override {
    assert(Valid());
    return current_key_;
  }
  Slice value() const override {
    assert(Valid());
    return current_val_;
  }
  Status status() const override { return Status::OK(); }
  void Seek(const Slice& target) override;
  void SeekForPrev(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Next() override;
  void Prev() override;

 private:
  bool ParseNextKey();
  void SeekToRestartPoint(uint32_t index);
  uint32_t GetRestartPoint(uint32_t index);
  inline uint32_t NextEntryOffset() const {
    return (current_val_.data() + current_val_.size()) - data_;
  }

  char* data_;  // all data of sstable
  size_t data_size_;
  uint32_t kv_pairs_offset_;  // offset in data where kv_pairs start
  size_t index_;              // index of current kv_pair
  char* walker_;              // pointer to current data element
  NextPair nextf_;            // Decoding function to retrieve kvpairs
  size_t count_;              // Number of kv_pairs
  Slice current_val_;         // value present at data pointer
  Slice current_key_;         // key present at data pointer
  const InternalKeyComparator icmp_;
  uint32_t restart_index_;
};
}  // namespace ROCKSDB_NAMESPACE

#endif
#endif
