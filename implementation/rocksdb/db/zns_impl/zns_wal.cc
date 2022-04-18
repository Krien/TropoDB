#include "db/zns_impl/zns_wal.h"

#include "db/write_batch_internal.h"
#include "db/zns_impl/device_wrapper.h"
#include "db/zns_impl/zns_memtable.h"
#include "db/zns_impl/zns_utils.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/write_batch.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {
ZNSWAL::ZNSWAL(QPairFactory* qpair_factory, const ZnsDevice::DeviceInfo& info,
               const uint64_t min_zone_head, uint64_t max_zone_head)
    : zone_head_(min_zone_head),
      write_head_(min_zone_head),
      min_zone_head_(min_zone_head),
      max_zone_head_(max_zone_head),
      zone_size_(info.zone_size),
      lba_size_(info.lba_size),
      qpair_factory_(qpair_factory) {
  assert(zone_head_ < info.lba_cap);
  assert(zone_head_ % info.lba_size == 0);
  assert(qpair_factory_ != nullptr);
  qpair_ = new ZnsDevice::QPair*[1];
  qpair_factory_->Ref();
  qpair_factory_->register_qpair(qpair_);
}

ZNSWAL::~ZNSWAL() {
  printf("Deleting WAL.\n");
  if (qpair_ != nullptr) {
    qpair_factory_->unregister_qpair(*qpair_);
    delete qpair_;
  }
  qpair_factory_->Unref();
  qpair_factory_ = nullptr;
}

void ZNSWAL::Append(Slice data) {
  uint64_t zcalloc_size = 0;
  std::string data_transformed;
  PutLengthPrefixedSlice(&data_transformed, data);
  char* payload = ZnsUtils::slice_to_spdkformat(
      &zcalloc_size, Slice(data_transformed.data(), data.size() + 8), *qpair_,
      lba_size_);
  if (payload == nullptr) {
    return;
  }
  if (write_head_ + zcalloc_size / lba_size_ > max_zone_head_) {
    printf("WARNING WAL is filled to the border, data is lost!\n");
    return;
  }
  int rc = ZnsDevice::z_append(*qpair_, zone_head_, payload, zcalloc_size);
  ZnsUtils::update_zns_heads(&write_head_, &zone_head_, zcalloc_size, lba_size_,
                             zone_size_);
  ZnsDevice::z_free(*qpair_, payload);
}

Status ZNSWAL::Reset() {
  Status s;
  for (uint64_t slba = min_zone_head_; slba < max_zone_head_;
       slba += lba_size_) {
    s = ZnsDevice::z_reset(*qpair_, slba, false) == 0
            ? Status::OK()
            : Status::IOError("Reset WAL zone error");
    if (!s.ok()) {
      return s;
    }
  }
  s = Status::OK();
  write_head_ = min_zone_head_;
  zone_head_ = min_zone_head_;
  return s;
}

Status ZNSWAL::Recover() {
  uint64_t write_head = min_zone_head_;
  uint64_t zone_head = min_zone_head_, old_zone_head = min_zone_head_;
  for (uint64_t slba = min_zone_head_; slba < max_zone_head_;
       slba += zone_size_) {
    if (ZnsDevice::z_get_zone_head(*qpair_, slba, &zone_head) != 0) {
      return Status::IOError("Error getting zonehead for WAL");
    }
    // head is at last zone that is not empty
    if (zone_head > slba) {
      write_head = zone_head;
    }
    // end has been reached.
    if (write_head > min_zone_head_ && zone_head == slba) {
      break;
    }
  }
  write_head_ = write_head;
  zone_head_ = (write_head_ / zone_size_) * zone_size_;
  return Status::OK();
}

Status ZNSWAL::Replay(ZNSMemTable* mem, SequenceNumber* seq) {
  if (write_head_ == min_zone_head_) {
    return Status::OK();
  }
  char* input = (char*)ZnsDevice::z_calloc(*qpair_, 1, lba_size_);
  Slice res;
  Slice input_res;
  WriteOptions wo;
  for (uint64_t batch = min_zone_head_; batch < write_head_; batch++) {
    ZnsDevice::z_read(*qpair_, batch, input, lba_size_);
    input_res = Slice(input, lba_size_);
    if (!GetLengthPrefixedSlice(&input_res, &res)) {
      return Status::Corruption();
    }
    WriteBatch batcha;
    WriteBatchInternal::SetContents(&batcha, res);
    Status s = mem->Write(wo, &batcha);
    if (!s.ok()) {
      return s;
    }
    const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batcha) +
                                    WriteBatchInternal::Count(&batcha) - 1;
    if (last_seq > *seq) {
      *seq = last_seq;
    }
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
