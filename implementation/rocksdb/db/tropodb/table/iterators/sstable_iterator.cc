#include "db/tropodb/table/iterators/sstable_iterator.h"

#include "db/tropodb/tropodb_config.h"
#include "db/tropodb/utils/tropodb_logger.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {
SSTableIterator::SSTableIterator(char* data, const uint64_t data_size,
                                 const uint64_t count, NextPair nextf,
                                 const Comparator* cmp)
    : data_(data),
      data_size_(data_size),
      count_(count),
      cmp_(cmp),
      nextf_(nextf),
      index_(count + 1),
      walker_(data_),
      current_val_(TropoDBConfig::deadbeef),
      current_key_(TropoDBConfig::deadbeef),
      restart_index_(0) {}

SSTableIterator::~SSTableIterator() { free(data_); };

void SSTableIterator::Seek(const Slice& target) {
  Slice target_ptr_stripped = ExtractUserKey(target);
  int current_key_compare = 0;

  if (Valid()) {
    current_key_compare = Compare(ExtractUserKey(current_key_), target_ptr_stripped);
    if (current_key_compare < 0) {
    } else if (current_key_compare > 0) {
      SeekToFirst();
    } else {
      return;
    }
  } else {
    SeekToFirst();
  }

  while (index_ <= count_) {
    int cmp = Compare(ExtractUserKey(current_key_), target_ptr_stripped);
    if (cmp < 0) {
    } else if (cmp > 0) {
      current_key_.clear();
      current_val_.clear();
      index_ = count_ + 1;
      return;
    } else {
      return;
    }
    ParseNextKey();
  }

  index_ = count_ + 1;
  current_key_.clear();
  current_val_.clear();
  return;
}

void SSTableIterator::SeekForPrev(const Slice& target) {
  Seek(target);
  Prev();
}

void SSTableIterator::SeekToFirst() {
  index_ = 0;
  walker_ = data_;
  ParseNextKey();
}

void SSTableIterator::SeekToLast() {
  if (index_ == count_ - 1) {
    return;
  }
  while (index_ <= count_ - 1 && Valid()) {
    ParseNextKey();
  }  
  ParseNextKey();
}

void SSTableIterator::Next() {
  assert(Valid());
  ParseNextKey();
}

// Avoid using prev!
void SSTableIterator::Prev() {
  assert(Valid());
  uint64_t n = index_;
  if (n == 0) {
    return;
  }
  SeekToFirst();
  while (index_ < n-1) {
    ParseNextKey();  
  }
  ParseNextKey();
}

bool SSTableIterator::ParseNextKey() {
  if (index_ > count_) {
    return false;
  }
  if ((uint64_t)(walker_ - data_) >= data_size_) {
    return false;
  }
  nextf_(&walker_, &current_key_, &current_val_);
  index_++;
  return true;
}
}  // namespace ROCKSDB_NAMESPACE
