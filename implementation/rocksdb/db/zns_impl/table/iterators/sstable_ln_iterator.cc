#include "db/zns_impl/table/iterators/sstable_ln_iterator.h"

#include "db/dbformat.h"
#include "db/zns_impl/config.h"
#include "db/zns_impl/table/zns_sstable.h"
#include "db/zns_impl/table/zns_sstable_manager.h"
#include "db/zns_impl/table/zns_zonemetadata.h"
#include "db/zns_impl/utils/tropodb_logger.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {
LNZoneIterator::LNZoneIterator(const Comparator* cmp,
                               const std::vector<SSZoneMetaData*>* slist,
                               const uint8_t level)
    : cmp_(cmp), level_(level), slist_(slist), index_(slist->size()) {}

LNZoneIterator::~LNZoneIterator() = default;

Slice LNZoneIterator::value() const {
  assert(Valid());
  // printf("Encoding %u %lu %lu %u ", (*slist_)[index_]->LN.lba_regions,
  //        (*slist_)[index_]->number, (*slist_)[index_]->lba_count, level_);
  // This is necessary to prevent leaking data into buf. This causes validation
  // checks to fail (corruption etc.), but should not cause failures itself.
  memset(value_buf_, 0, sizeof(value_buf_));
  EncodeFixed8(value_buf_, (*slist_)[index_]->LN.lba_regions);
  for (size_t i = 0; i < (*slist_)[index_]->LN.lba_regions; i++) {
    EncodeFixed64(value_buf_ + 1 + i * 16, (*slist_)[index_]->LN.lbas[i]);
    EncodeFixed64(value_buf_ + 9 + i * 16,
                  (*slist_)[index_]->LN.lba_region_sizes[i]);
    // printf(" - %lu %lu ", (*slist_)[index_]->LN.lbas[i],
    //        (*slist_)[index_]->LN.lba_region_sizes[i]);
  }
  // printf("\n");
  EncodeFixed64(value_buf_ + 1 + 16 * (*slist_)[index_]->LN.lba_regions,
                (*slist_)[index_]->lba_count);
  EncodeFixed8(value_buf_ + 9 + 16 * (*slist_)[index_]->LN.lba_regions, level_);
  EncodeFixed64(value_buf_ + 10 + 16 * (*slist_)[index_]->LN.lba_regions,
                (*slist_)[index_]->number);
  return Slice(value_buf_, sizeof(value_buf_));
}

void LNZoneIterator::Seek(const Slice& target) {
  index_ = ZNSSSTableManager::FindSSTableIndex(cmp_, *slist_, target);
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

static void LNZonePrefetcher(void* prefetch) {
  ZonePrefetcher* zone_prefetcher = reinterpret_cast<ZonePrefetcher*>(prefetch);
  while (true) {
    zone_prefetcher->mut_.Lock();

    // Wait for tasks
    while (zone_prefetcher->index_ - zone_prefetcher->tail_read_ >
               ZnsConfig::compaction_maximum_prefetches ||
           zone_prefetcher->index_ == zone_prefetcher->its.size()) {
      // printf("Prefetcher awaiting new task %lu %lu %lu\n",
      //        zone_prefetcher->tail_read_, zone_prefetcher->index_,
      //        zone_prefetcher->its.size());
      if (zone_prefetcher->done_) {
        break;
      }
      zone_prefetcher->waiting_.Wait();
    }

    // Cleanup
    while (zone_prefetcher->tail_ + 1 < zone_prefetcher->tail_read_ &&
           zone_prefetcher->tail_ < zone_prefetcher->its.size()) {
      // printf("Prefetching cleaning %lu \n", zone_prefetcher->tail_);
      // TODO: The iterator takes ownership of the data. Therefore, we can not
      // do manual deletion. This leads to anti-patterns and strange behaviour.
      // Investigate if there is chance of a memory-leak.
      zone_prefetcher->tail_++;
    }

    // No more work to do
    if (zone_prefetcher->done_) {
      // printf("Prefetching done \n");
      zone_prefetcher->mut_.Unlock();
      break;
    }

    // Get more iterators
    // printf("Prefetch %lu \n", zone_prefetcher->index_);
    std::string handle = zone_prefetcher->its[zone_prefetcher->index_].first;
    zone_prefetcher->mut_.Unlock();
    Iterator* iter = (*(zone_prefetcher->zonefunc_))(
        zone_prefetcher->arg_, Slice(handle), zone_prefetcher->cmp_);
    zone_prefetcher->mut_.Lock();
    zone_prefetcher->its[zone_prefetcher->index_].second = iter;
    zone_prefetcher->index_++;
    // printf("Prefetched %lu \n", zone_prefetcher->index_);

    // Make progress
    zone_prefetcher->waiting_.SignalAll();
    zone_prefetcher->mut_.Unlock();
  }
  zone_prefetcher->mut_.Lock();
  zone_prefetcher->quit_ = true;
  // printf("Quiting zone prefetcher \n");
  zone_prefetcher->waiting_.SignalAll();
  zone_prefetcher->mut_.Unlock();
}

LNIterator::LNIterator(Iterator* ln_iterator,
                       NewZoneIteratorFunction zone_function, void* arg,
                       const Comparator* cmp, Env* env)
    : zone_function_(zone_function),
      arg_(arg),
      index_iter_(ln_iterator),
      data_iter_(nullptr),
      cmp_(cmp),
      env_(env) {
  if (ZnsConfig::compaction_allow_prefetching && env_ != nullptr) {
    index_iter_.SeekToFirst();
    while (index_iter_.Valid()) {
      Slice handle = index_iter_.value();
      std::string assigned_;
      assigned_.assign(handle.data(), handle.size());
      prefetcher_.its.push_back(std::make_pair(assigned_, nullptr));
      index_iter_.Next();
    }
    index_iter_.SeekToFirst();
    // No prefetch when size is <= 1, in that case what is there to prefetch?
    if (prefetcher_.its.size() > 1) {
      prefetcher_.arg_ = arg_;
      prefetcher_.cmp_ = cmp_;
      prefetcher_.zonefunc_ = zone_function_;
      env_->Schedule(&LNZonePrefetcher, &(this->prefetcher_),
                     rocksdb::Env::LOW);
      prefetching_ = true;
    }
  }
}

LNIterator::~LNIterator() {
  if (prefetching_) {
    prefetcher_.mut_.Lock();
    prefetcher_.done_ = true;
    // printf("Prefetch done!?\n");
    prefetcher_.waiting_.SignalAll();
    while (!prefetcher_.quit_) {
      prefetcher_.waiting_.Wait();
    }
    prefetcher_.mut_.Unlock();
    // printf("Prefetch Quit!?\n");
  }
}

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
    // prefetch hack
    if (index_iter_.Valid() && prefetching_) {
      prefetcher_.mut_.Lock();
      prefetcher_.tail_read_++;
      prefetcher_.waiting_.SignalAll();
      while (prefetcher_.tail_read_ >= prefetcher_.index_) {
        // printf("Waiting for read to complete...\n");
        prefetcher_.waiting_.Wait();
      }
      Slice handle = prefetcher_.its[prefetcher_.tail_read_].first;
      if (handle.compare(index_iter_.value()) != 0) {
        printf(
            "FATAL error, LN iterator handle changed. This is "
            "unrecoverable.\n");
        exit(-1);
      }
      if (data_iter_.iter() != nullptr &&
          handle.compare(data_zone_handle_) == 0) {
        prefetcher_.mut_.Unlock();
      } else {
        SetDataIterator(prefetcher_.its[prefetcher_.tail_read_].second);
        data_zone_handle_.assign(handle.data(), handle.size());
        // printf("Read %lu...\n", prefetcher_.tail_read_);
        prefetcher_.mut_.Unlock();
      }
    } else {
      InitDataZone();
    }
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
  Iterator* iter = (*zone_function_)(arg_, handle, cmp_);
  data_zone_handle_.assign(handle.data(), handle.size());
  SetDataIterator(iter);
}
}  // namespace ROCKSDB_NAMESPACE
