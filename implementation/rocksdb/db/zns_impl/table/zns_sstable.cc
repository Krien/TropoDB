#include "db/zns_impl/table/zns_sstable.h"

#include "db/zns_impl/io/device_wrapper.h"
#include "db/zns_impl/io/qpair_factory.h"

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
  // printf("Deleting SSTable WAL.\n");
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

void ZnsSSTable::GeneratePreamble(std::string* dst, uint32_t count) {
  std::string preamble;
  PutVarint32(&preamble, count);
  *dst = preamble.append(*dst);
}

int FindSS(const InternalKeyComparator& icmp,
           const std::vector<SSZoneMetaData*>& ss, const Slice& key) {
  uint32_t left = 0;
  uint32_t right = ss.size();
  // binary search I guess.
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    const SSZoneMetaData* m = ss[mid];
    if (icmp.InternalKeyComparator::Compare(m->largest.Encode(), key) < 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  return right;
}

}  // namespace ROCKSDB_NAMESPACE
