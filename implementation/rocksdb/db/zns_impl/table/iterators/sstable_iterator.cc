#include "db/zns_impl/table/iterators/sstable_iterator.h"

#include "db/zns_impl/config.h"
#include "db/zns_impl/utils/tropodb_logger.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {
SSTableIterator::SSTableIterator(char* data, const size_t data_size,
                                 const size_t count, NextPair nextf,
                                 const Comparator* cmp)
    : data_(data),
      data_size_(data_size),
      kv_pairs_offset_(sizeof(uint32_t) * (count + 1)),
      count_(count),
      cmp_(cmp),
      nextf_(nextf),
      index_(count + 1),
      walker_(data_ + kv_pairs_offset_),
      current_val_(ZnsConfig::deadbeef),
      current_key_(ZnsConfig::deadbeef),
      restart_index_(0) {}

SSTableIterator::~SSTableIterator() { free(data_); };

void SSTableIterator::Seek(const Slice& target) {
  Slice target_ptr_stripped = ExtractUserKey(target);
  // binary search as seen in LevelDB.
  size_t left = 0;
  size_t right = count_ - 1;
  int current_key_compare = 0;
  ParsedInternalKey parsed_key;

  if (Valid()) {
    if (!ParseInternalKey(current_key_, &parsed_key, false).ok()) {
      TROPODB_ERROR("corrupt key %lu %lu\n", index_, count_);
    }
    current_key_compare =
        cmp_->Compare(parsed_key.user_key, target_ptr_stripped);
    if (current_key_compare < 0) {
      left = restart_index_;  // index_ > 2 ? index_ - 2 : 0;
    } else if (current_key_compare > 0) {
      right = index_;
    } else {
      return;
    }
  }

  size_t mid = 0;
  size_t region_offset = 0;
  while (left < right) {
    mid = (left + right + 1) / 2;
    SeekToRestartPoint(mid);
    ParseNextKey();
    if (!ParseInternalKey(current_key_, &parsed_key, false).ok()) {
      TROPODB_ERROR("corrupt key %lu %lu\n", index_, count_);
    }
    if (cmp_->Compare(parsed_key.user_key, target_ptr_stripped) < 0) {
      left = mid;
    } else {
      right = mid - 1;
    }
  }
  SeekToRestartPoint(left);
  ParseNextKey();
  while (Valid()) {
    if (!ParseInternalKey(current_key_, &parsed_key, false).ok()) {
      TROPODB_ERROR("corrupt key %lu %lu\n", index_, count_);
    }
    if (cmp_->Compare(parsed_key.user_key, target_ptr_stripped) == 0) {
      restart_index_ = left;
      break;
    }
    if (!ParseNextKey()) {
      break;
    }
  }
  // OLD lineair
  // SeekToRestartPoint(0);
  // while (Valid()) {
  //   ParseNextKey();
  //   if (icmp_.Compare(ExtractUserKey(current_key_), target_ptr_stripped) ==
  //   0) {
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
  if ((size_t)(walker_ - data_) >= data_size_) {
    return false;
  }
  nextf_(&walker_, &current_key_, &current_val_);
  index_++;
  return true;
}

void SSTableIterator::SeekToRestartPoint(const uint32_t index) {
  current_key_.clear();
  index_ = index;
  walker_ = data_ + GetRestartPoint(index_);
  current_val_ = Slice(walker_, 0);
}

uint32_t SSTableIterator::GetRestartPoint(const uint32_t index) const {
  return index == 0 ? kv_pairs_offset_
                    : DecodeFixed32(data_ + index * sizeof(uint32_t)) +
                          kv_pairs_offset_;
}

}  // namespace ROCKSDB_NAMESPACE
