#include "db/tropodb/table/iterators/sstable_iterator_compressed.h"

#include "db/tropodb/table/tropodb_sstable_reader.h"
#include "db/tropodb/utils/tropodb_logger.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

SSTableIteratorCompressed::SSTableIteratorCompressed(
    const Comparator* comparator, char* data, uint64_t data_size,
    uint64_t num_restarts)
    : comparator_(comparator),
      data_(data),
      num_restarts_(num_restarts),
      kv_pairs_offset_(sizeof(uint64_t) * (num_restarts + 2)),
      current_(0),
      restart_index_(0),
      data_size_(data_size) {
  assert(num_restarts_ > 0);
}

SSTableIteratorCompressed::~SSTableIteratorCompressed() { free(data_); }

uint64_t SSTableIteratorCompressed::GetRestartPoint(uint64_t index) {
  assert(index < num_restarts_);
  return DecodeFixed64(data_ + (index + 2) * sizeof(uint64_t)) +
         kv_pairs_offset_;
}

void SSTableIteratorCompressed::SeekToRestartPoint(uint64_t index) {
  key_.clear();
  restart_index_ = index;
  // current_ will be fixed by ParseNextKey();

  // ParseNextKey() starts at the end of value_, so set value_ accordingly
  uint64_t offset = GetRestartPoint(index);
  value_ = Slice(data_ + offset, 0);
}

void SSTableIteratorCompressed::Prev() {
  assert(Valid());

  // Scan backwards to a restart point before current_
  const uint64_t original = current_;
  while (GetRestartPoint(restart_index_) >= original) {
    if (restart_index_ == 0) {
      // No more entries
      current_ = kv_pairs_offset_;
      restart_index_ = 0;
      return;
    }
    restart_index_--;
  }

  SeekToRestartPoint(restart_index_);
  do {
    // Loop until end of current entry hits the start of original entry
  } while (ParseNextKey() && NextEntryOffset() < original);
}

void SSTableIteratorCompressed::Next() {
  assert(Valid());
  ParseNextKey();
}

void SSTableIteratorCompressed::SeekToFirst() {
  SeekToRestartPoint(0);
  ParseNextKey();
}

void SSTableIteratorCompressed::SeekToLast() {
  SeekToRestartPoint(num_restarts_ - 1);
  while (ParseNextKey() && NextEntryOffset() <= data_size_) {
    // Keep skipping
  }
}
void SSTableIteratorCompressed::SeekForPrev(const Slice& target) {
  Seek(target);
  Prev();
}

void SSTableIteratorCompressed::Seek(const Slice& target) {
  Slice target_ptr_stripped = ExtractUserKey(target);
  // Binary search in restart array to find the last restart point
  // with a key < target
  uint64_t left = 0;
  uint64_t right = num_restarts_ - 1;

  int current_key_compare = 0;

  if (Valid()) {
    // If we're already scanning, use the current position as a starting
    // point. This is beneficial if the key we're seeking to is ahead of the
    // current position.
    current_key_compare = Compare(ExtractUserKey(key_), target_ptr_stripped);
    if (current_key_compare < 0) {
      // key_ is smaller than target
      left = restart_index_;
    } else if (current_key_compare > 0) {
      right = restart_index_;
    } else {
      // We're seeking to the key we're already at.
      return;
    }
  }

  while (left < right) {
    uint64_t mid = (left + right + 1) / 2;
    uint64_t region_offset = GetRestartPoint(mid);
    uint32_t shared, non_shared, value_length;
    const char* key_ptr = TropoEncoding::DecodeEncodedEntry(
        data_ + region_offset, data_ + data_size_, &shared, &non_shared,
        &value_length);
    if (key_ptr == nullptr || (shared != 0)) {
      CorruptionError();
      return;
    }
    Slice mid_key(key_ptr, non_shared);
    if (Compare(ExtractUserKey(mid_key), target_ptr_stripped) < 0) {
      // Key at "mid" is smaller than "target".  Therefore all
      // blocks before "mid" are uninteresting.
      left = mid;
    } else {
      // Key at "mid" is >= "target".  Therefore all blocks at or
      // after "mid" are uninteresting.
      right = mid - 1;
    }
  }

  // We might be able to use our current position within the restart block.
  // This is true if we determined the key we desire is in the current
  // block
  // and is after than the current key.
  assert(current_key_compare == 0 || Valid());
  bool skip_seek = left == restart_index_ && current_key_compare < 0;
  if (!skip_seek) {
    SeekToRestartPoint(left);
  }

  // Linear search (within restart block) for first key >= target
  while (true) {
    if (!ParseNextKey()) {
      return;
    }
    if (Compare(ExtractUserKey(key_), target_ptr_stripped) >= 0) {
      if (Compare(ExtractUserKey(key_), target_ptr_stripped) != 0) {
        // Not sure why LevelDB does not do this if. If key not found, it should
        // invalidate right?
        current_ = data_size_;
        restart_index_ = num_restarts_;
        key_.clear();
        value_.clear();
      }
      return;
    }
  }
}

void SSTableIteratorCompressed::CorruptionError() {
  TROPO_LOG_ERROR(
      "ERROR: SSTableIteratorCompressed: Corrupt entry in SSTable block "
      "%lu/%lu \n",
      current_, data_size_);
  current_ = data_size_;
  restart_index_ = num_restarts_;
  status_ = Status::Corruption("bad entry in block");
  key_.clear();
  value_.clear();
}

bool SSTableIteratorCompressed::ParseNextKey() {
  current_ = NextEntryOffset();
  const char* p = data_ + current_;
  const char* limit = data_ + data_size_;
  if (p >= limit) {
    // No more entries to return.  Mark as invalid.
    current_ = data_size_;
    restart_index_ = 0;
    return false;
  }

  // Decode next entry
  uint32_t shared, non_shared, value_length;
  p = TropoEncoding::DecodeEncodedEntry(p, limit, &shared, &non_shared,
                                      &value_length);
  if (p == nullptr || key_.size() < shared) {
    CorruptionError();
    return false;
  } else {
    key_.resize(shared);
    key_.append(p, non_shared);
    value_ = Slice(p + non_shared, value_length);
    while (restart_index_ + 1 < num_restarts_ &&
           GetRestartPoint(restart_index_ + 1) < current_) {
      ++restart_index_;
    }
    return true;
  }
}
};  // namespace ROCKSDB_NAMESPACE
