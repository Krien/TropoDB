#include "db/zns_impl/table/iterators/sstable_ln_iterator.h"

#include "db/dbformat.h"
#include "db/zns_impl/table/zns_sstable.h"
#include "db/zns_impl/table/zns_sstable_manager.h"
#include "db/zns_impl/table/zns_zonemetadata.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {
LNZoneIterator::LNZoneIterator(const InternalKeyComparator& icmp,
                               const std::vector<SSZoneMetaData*>* slist,
                               const uint16_t level)
    : icmp_(icmp), slist_(slist), index_(slist->size()), level_(level) {}

LNZoneIterator::~LNZoneIterator() = default;

void LNZoneIterator::Seek(const Slice& target) {
  index_ = FindSS(icmp_, *slist_, target);
}

void LNZoneIterator::SeekForPrev(const Slice& target) {
  Seek(target);
  Prev();
}

void LNZoneIterator::SeekToFirst() { index_ = 0; }

void LNZoneIterator::SeekToLast() {
  index_ = slist_->empty() ? 0 : slist_->size() - 1;
}

void LNZoneIterator::Next() {
  assert(Valid());
  index_++;
}

void LNZoneIterator::Prev() {
  assert(Valid());
  index_ = index_ == 0 ? slist_->size() : index_ - 1;
}

LNIterator::LNIterator(Iterator* ln_iterator,
                       NewZoneIteratorFunction zone_function, void* arg)
    : zone_function_(zone_function),
      arg_(arg),
      index_iter_(ln_iterator),
      data_iter_(nullptr) {}

LNIterator::~LNIterator() = default;

void LNIterator::Seek(const Slice& target) {
  index_iter_.Seek(target);
  InitDataZone();
  if (data_iter_.iter() != nullptr) data_iter_.Seek(target);
  SkipEmptyDataLbasForward();
}

void LNIterator::SeekForPrev(const Slice& target) {
  Seek(target);
  Prev();
}

void LNIterator::SeekToFirst() {
  index_iter_.SeekToFirst();
  InitDataZone();
  if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  SkipEmptyDataLbasForward();
}

void LNIterator::SeekToLast() {
  index_iter_.SeekToLast();
  InitDataZone();
  if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  SkipEmptyDataLbasForward();
}

void LNIterator::Next() {
  assert(Valid());
  data_iter_.Next();
  SkipEmptyDataLbasForward();
}

void LNIterator::Prev() {
  assert(Valid());
  data_iter_.Next();
  SkipEmptyDataLbasBackward();
}

void LNIterator::SkipEmptyDataLbasForward() {
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    index_iter_.Next();
    InitDataZone();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  }
}

void LNIterator::SkipEmptyDataLbasBackward() {
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    index_iter_.Prev();
    InitDataZone();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  }
}

void LNIterator::SetDataIterator(Iterator* data_iter) {
  data_iter_.Set(data_iter);
}

void LNIterator::InitDataZone() {
  if (!index_iter_.Valid()) {
    SetDataIterator(nullptr);
    return;
  }
  Slice handle = index_iter_.value();
  if (data_iter_.iter() != nullptr && handle.compare(data_zone_handle_) == 0) {
    return;
  }
  Iterator* iter = (*zone_function_)(arg_, handle);
  data_zone_handle_.assign(handle.data(), handle.size());
  SetDataIterator(iter);
}
}  // namespace ROCKSDB_NAMESPACE
