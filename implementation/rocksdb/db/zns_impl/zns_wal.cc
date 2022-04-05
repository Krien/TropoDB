#include "db/zns_impl/zns_wal.h"

#include "db/zns_impl/device_wrapper.h"
#include "db/zns_impl/zns_utils.h"
#include "rocksdb/slice.h"

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
  assert(zone_head < info.lba_cap);
  assert(zone_head % info.lba_size == 0);
  assert(qpair_factory_ != nullptr);
  qpair_ = new ZnsDevice::QPair*[1];
  qpair_factory_->register_qpair(qpair_);
}

ZNSWAL::~ZNSWAL() {
  if (qpair_ != nullptr) {
    qpair_factory_->unregister_qpair(*qpair_);
    delete qpair_;
  }
}

void ZNSWAL::Append(Slice data) {
  uint64_t zcalloc_size = 0;
  char* payload =
      ZnsUtils::slice_to_spdkformat(&zcalloc_size, data, *qpair_, lba_size_);
  if (payload == nullptr) {
    return;
  }
  int rc = ZnsDevice::z_append(*qpair_, zone_head_, payload, zcalloc_size);
  ZnsUtils::update_zns_heads(&write_head_, &zone_head_, zcalloc_size, lba_size_,
                             zone_size_);
  ZnsDevice::z_free(*qpair_, payload);
}

}  // namespace ROCKSDB_NAMESPACE

// uint64_t slice_size = (uint64_t)data.size();
// uint64_t zcalloc_size = (slice_size / lba_size_) * lba_size_;
// zcalloc_size += slice_size % lba_size_ != 0 ? lba_size_ : 0;
// char* payload = (char*)ZnsDevice::z_calloc(*qpair_, zcalloc_size,
// sizeof(char)); const char* payload_c = data.data(); memcpy(payload,
// payload_c, slice_size); int rc = ZnsDevice::z_append(*qpair_, zone_head_,
// payload, zcalloc_size); write_head_ += zcalloc_size / lba_size_; if
// (write_head_ % zone_size_ == 0) {
//     zone_head_ = write_head_;
// }
// ZnsDevice::z_free(*qpair_, payload);