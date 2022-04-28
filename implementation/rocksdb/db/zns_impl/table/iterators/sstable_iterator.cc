#include "db/zns_impl/table/iterators/sstable_iterator.h"

#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {
SSTableIterator::SSTableIterator(char* data, size_t data_size, size_t count,
                                 NextPair nextf,
                                 const InternalKeyComparator& icmp)
    : data_(data),
      data_size_(data_size),
      kv_pairs_offset_(sizeof(uint32_t) * (count + 1)),
      index_(count),
      walker_(data_ + kv_pairs_offset_),
      nextf_(nextf),
      count_(count),
      current_val_("deadbeef"),
      current_key_("deadbeef"),
      icmp_(icmp),
      restart_index_(0) {}

SSTableIterator::~SSTableIterator() { free(data_); };

void SSTableIterator::Seek(const Slice& target) {
  Slice target_ptr_stripped = ExtractUserKey(target);

  // // binary search as seen in LevelDB.
  uint32_t left = 0;
  uint32_t right = count_ - 1;
  int current_key_compare = 0;

  if (Valid()) {
    current_key_compare =
        icmp_.Compare(ExtractUserKey(current_key_), target_ptr_stripped);
    if (current_key_compare < 0) {
      left = restart_index_;  // index_ > 2 ? index_ - 2 : 0;
    } else if (current_key_compare > 0) {
      right = index_;
    } else {
      return;
    }
  }

  uint32_t mid = 0;
  uint32_t region_offset = 0;
  while (left < right) {
    mid = (left + right + 1) / 2;
    SeekToRestartPoint(mid);
    ParseNextKey();
    if (icmp_.Compare(ExtractUserKey(current_key_), target_ptr_stripped) < 0) {
      left = mid;
    } else {
      right = mid - 1;
    }
  }
  SeekToRestartPoint(left);
  ParseNextKey();
  while (Valid()) {
    if (icmp_.Compare(ExtractUserKey(current_key_), target_ptr_stripped) == 0) {
      restart_index_ = left;
      break;
    }
    ParseNextKey();
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

void SSTableIterator::SeekToRestartPoint(uint32_t index) {
  current_key_.clear();
  index_ = index;
  walker_ = data_ + GetRestartPoint(index_);
  current_val_ = Slice(walker_, 0);
}

uint32_t SSTableIterator::GetRestartPoint(uint32_t index) {
  return index == 0 ? kv_pairs_offset_
                    : DecodeFixed32(data_ + index * sizeof(uint32_t)) +
                          kv_pairs_offset_;
}

}  // namespace ROCKSDB_NAMESPACE
