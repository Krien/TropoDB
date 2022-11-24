#include "db/tropodb/table/tropodb_sstable_builder.h"

#include "db/tropodb/table/tropodb_ln_sstable.h"
#include "db/tropodb/tropodb_config.h"
#include "db/tropodb/utils/tropodb_logger.h"

namespace ROCKSDB_NAMESPACE {

TropoSSTableBuilder::TropoSSTableBuilder(TropoSSTable* table,
                                         SSZoneMetaData* meta,
                                         bool use_encoding, int8_t writer)
    : started_(false),
      kv_numbers_(0),
      counter_(0),
      use_encoding_(use_encoding),
      table_(table),
      meta_(meta),
      writer_(writer) {
  meta_->lba_count = 0;
  buffer_.clear();
  buffer_.reserve(TropoDBConfig::max_bytes_sstable_);
  kv_pair_offsets_.clear();
  if (use_encoding_) {
    kv_pair_offsets_.push_back(0);
    last_key_.clear();
  }
}

TropoSSTableBuilder::~TropoSSTableBuilder() {}

uint64_t TropoSSTableBuilder::EstimateSizeImpact(const Slice& key,
                                                 const Slice& value) const {
  // TODO: this is hardcoded, not maintainable.
  return key.size() + value.size() + 5 * sizeof(uint32_t);
}

Status TropoSSTableBuilder::Apply(const Slice& key, const Slice& value) {
  if (!started_) {
    meta_->smallest.DecodeFrom(key);
    started_ = true;
  }

  if (use_encoding_) {
    Slice last_key_piece(last_key_);
    size_t shared = 0;
    if (counter_ < TropoDBConfig::max_sstable_encoding) {
      const size_t min_length = std::min(last_key_piece.size(), key.size());
      while ((shared < min_length) && (last_key_piece[shared] == key[shared])) {
        shared++;
      }
    } else {
      // Restart compression
      kv_pair_offsets_.push_back(buffer_.size());
      counter_ = 0;
    }
    const size_t non_shared = key.size() - shared;
    // Add "<shared><non_shared><value_size>" to buffer_
    PutVarint32(&buffer_, shared);
    PutVarint32(&buffer_, non_shared);
    PutVarint32(&buffer_, value.size());

    // Add string delta to buffer_ followed by value
    buffer_.append(key.data() + shared, non_shared);
    buffer_.append(value.data(), value.size());

    // Update state
    last_key_.resize(shared);
    last_key_.append(key.data() + shared, non_shared);
    assert(Slice(last_key_) == key);
    counter_++;
  } else {
    PutVarint32(&buffer_, key.size());
    PutVarint32(&buffer_, value.size());
    buffer_.append(key.data(), key.size());
    buffer_.append(value.data(), value.size());
    kv_pair_offsets_.push_back(buffer_.size());
  }
  meta_->largest.DecodeFrom(key);
  kv_numbers_++;
  return Status::OK();
}

Status TropoSSTableBuilder::Finalise() {
  meta_->numbers = kv_numbers_;
  // TODO: this is not a bottleneck, but it is ugly...
  std::string preamble;
  uint64_t expect_size =
    buffer_.size() + (kv_pair_offsets_.size() + 2) * sizeof(uint64_t);
  PutFixed64(&preamble, expect_size);
  PutFixed64(&preamble, kv_pair_offsets_.size());
  for (size_t i = 0; i < kv_pair_offsets_.size(); i++) {
    PutFixed64(&preamble, kv_pair_offsets_[i]);
  }
  buffer_ = preamble.append(buffer_);
  return Status::OK();
}

Status TropoSSTableBuilder::Flush() {
  if (writer_ != -1) {
    return static_cast<TropoLNSSTable*>(table_)->WriteSSTable(Slice(buffer_),
                                                              meta_, writer_);
  } else {
    return table_->WriteSSTable(Slice(buffer_), meta_);
  }
}
}  // namespace ROCKSDB_NAMESPACE
