#include "db/zns_impl/table/iterators/sstable_iterator.h"

#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {
SSTableIterator::SSTableIterator(char* data, size_t count, NextPair nextf)
    : data_(data),
      walker_(data),
      nextf_(nextf),
      index_(0),
      count_(count),
      current_val_("deadbeef"),
      current_key_("deadbeef") {}

SSTableIterator::~SSTableIterator() = default;

void SSTableIterator::Seek(const Slice& target) {
  walker_ = data_;
  index_ = 0;
  while (Valid()) {
    index_++;
    nextf_(&walker_, &current_key_, &current_val_);
    if (target.compare(current_key_) == 0) {
      break;
    }
  }
}

void SSTableIterator::SeekForPrev(const Slice& target) {
  Seek(target);
  Prev();
}

void SSTableIterator::SeekToFirst() {
  walker_ = data_;
  index_ = 0;
  Next();
}

void SSTableIterator::SeekToLast() {
  while (index_ < count_) {
    Next();
  }
}

void SSTableIterator::Next() {
  assert(Valid());
  nextf_(&walker_, &current_key_, &current_val_);
  index_++;
}

// Avoid using prev!
void SSTableIterator::Prev() {
  // We can not read backwards, so we have to start from the beginning.
  size_t target = index_ - 1;
  SeekToFirst();
  while (index_ < target) {
    Next();
  }
  // set to invalid next iteration.
  if (index_ == 1) {
    index_ = count_ + 1;
  }
}
}  // namespace ROCKSDB_NAMESPACE
