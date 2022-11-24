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
      kv_pairs_offset_(sizeof(uint64_t) * count),
      count_(count),
      cmp_(cmp),
      nextf_(nextf),
      index_(count + 1),
      walker_(data_ + kv_pairs_offset_),
      current_val_(TropoDBConfig::deadbeef),
      current_key_(TropoDBConfig::deadbeef),
      restart_index_(0) {}

SSTableIterator::~SSTableIterator() { free(data_); };

void SSTableIterator::Seek(const Slice& target) {
  Slice target_ptr_stripped = ExtractUserKey(target);
  // binary search as seen in LevelDB.
  uint64_t left = 0;
  uint64_t right = count_ - 1;

  int current_key_compare = 0;

  if (Valid()) {
    current_key_compare = Compare(ExtractUserKey(current_key_), target_ptr_stripped);
    if (current_key_compare < 0) {
      left = restart_index_;  // index_ > 2 ? index_ - 2 : 0;
    } else if (current_key_compare > 0) {
      right = index_;
    } else {
      return;
    }
  }

  uint64_t mid = 0;
  while (left != right) {
    mid = (left + right) / 2;
    SeekToRestartPoint(mid);
    ParseNextKey();
    int cmp = Compare(ExtractUserKey(current_key_), target_ptr_stripped);
    if (cmp < 0) {
      left = mid + 1;
    } else if (cmp > 0) {
      right = mid - 1;
    } else {
      left = mid;
      break;
    }
  }
  SeekToRestartPoint(left);
  ParseNextKey();
  size_t steps = 0;
  while (Valid()) {
    if (Compare(ExtractUserKey(current_key_), target_ptr_stripped) == 0) {
      restart_index_ = left;
      break;
    }
    if (!ParseNextKey()) {
      break;
    }
    steps++;
  }
  // OLD lineair
  // SeekToRestartPoint(0);
  // while (Valid()) {
  //   ParseNextKey();
  //   if (Compare(ExtractUserKey(current_key_), target_ptr_stripped) == 0) {
  //     break;
  //   }
  // }
}

void SSTableIterator::SeekForPrev(const Slice& target) {
  Seek(target);
  Prev();
}

void SSTableIterator::SeekToFirst() {
  SeekToRestartPoint(0);
  ParseNextKey();
}

void SSTableIterator::SeekToLast() {
  SeekToRestartPoint(count_ - 1);
  ParseNextKey();
}

void SSTableIterator::Next() {
  assert(Valid());
  ParseNextKey();
}

// Avoid using prev!
void SSTableIterator::Prev() {
  assert(Valid());
  SeekToRestartPoint(index_ - 1);
  ParseNextKey();
}

bool SSTableIterator::ParseNextKey() {
  if ((uint64_t)(walker_ - data_) >= data_size_) {
    return false;
  }
  nextf_(&walker_, &current_key_, &current_val_);
  index_++;
  return true;
}

void SSTableIterator::SeekToRestartPoint(const uint64_t index) {
  current_key_.clear();
  current_val_.clear();
  index_ = index;
  walker_ = data_ + GetRestartPoint(index_);
}

uint64_t SSTableIterator::GetRestartPoint(const uint64_t index) const {
  uint64_t point = index == 0 ? 0 : DecodeFixed64(data_ + index * sizeof(uint64_t));
  point += kv_pairs_offset_;
  return point;
}

}  // namespace ROCKSDB_NAMESPACE
