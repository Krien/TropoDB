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
  SSTableIterator(char* data, size_t count, NextPair nextf,
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
  char* data_;
  char* walker_;
  NextPair nextf_;
  size_t index_;
  size_t count_;
  Slice current_val_;
  Slice current_key_;
  const InternalKeyComparator icmp_;
};
}  // namespace ROCKSDB_NAMESPACE

#endif
#endif
