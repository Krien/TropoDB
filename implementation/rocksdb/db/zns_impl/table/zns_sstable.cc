#include "db/zns_impl/device_wrapper.h"
#include "db/zns_impl/qpair_factory.h"
#include "db/zns_impl/zns_sstable_manager.h"

namespace ROCKSDB_NAMESPACE {
ZnsSSTable::ZnsSSTable(QPairFactory* qpair_factory,
                       const ZnsDevice::DeviceInfo& info,
                       const uint64_t min_zone_head, uint64_t max_zone_head)
    : zone_head_(min_zone_head),
      write_head_(min_zone_head),
      zone_tail_(min_zone_head),
      write_tail_(min_zone_head),
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

ZnsSSTable::~ZnsSSTable() {
  printf("Deleting SSTable WAL.\n");
  if (qpair_ != nullptr) {
    qpair_factory_->unregister_qpair(*qpair_);
    delete qpair_;
  }
  qpair_factory_->Unref();
  qpair_factory_ = nullptr;
}

void ZnsSSTable::PutKVPair(std::string* dst, const Slice& key,
                           const Slice& value) {
  PutVarint32(dst, key.size());
  PutVarint32(dst, value.size());
  dst->append(key.data(), key.size());
  dst->append(value.data(), value.size());
}
}  // namespace ROCKSDB_NAMESPACE
