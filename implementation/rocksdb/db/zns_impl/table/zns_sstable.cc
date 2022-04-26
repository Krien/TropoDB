#include "db/zns_impl/table/zns_sstable.h"

#include <memory>

#include "db/zns_impl/io/szd_port.h"

namespace ROCKSDB_NAMESPACE {
ZnsSSTable::ZnsSSTable(SZD::SZDChannelFactory* channel_factory,
                       const SZD::DeviceInfo& info,
                       const uint64_t min_zone_head, uint64_t max_zone_head)
    : zone_head_(min_zone_head),
      write_head_(min_zone_head),
      zone_tail_(min_zone_head),
      write_tail_(min_zone_head),
      min_zone_head_(min_zone_head),
      max_zone_head_(max_zone_head),
      zone_size_(info.zone_size),
      lba_size_(info.lba_size),
      mdts_(info.mdts),
      channel_factory_(channel_factory) {
  assert(zone_head_ < info.lba_cap);
  assert(zone_head_ % info.lba_size == 0);
  assert(channel_factory_ != nullptr);
  channel_factory_->Ref();
  channel_factory_->register_channel(&channel_, min_zone_head, max_zone_head);
}

ZnsSSTable::~ZnsSSTable() {
  printf("Deleting SSTable manager.\n");
  channel_factory_->unregister_channel(channel_);
  channel_factory_->Unref();
  channel_factory_ = nullptr;
}

void ZnsSSTable::PutKVPair(std::string* dst, const Slice& key,
                           const Slice& value) {
  PutVarint32(dst, key.size());
  PutVarint32(dst, value.size());
  dst->append(key.data(), key.size());
  dst->append(value.data(), value.size());
}

void ZnsSSTable::GeneratePreamble(std::string* dst,
                                  std::vector<uint32_t> kv_pair_offsets_) {
  std::string preamble;
  PutFixed32(&preamble, kv_pair_offsets_.size());
  for (auto entry = begin(kv_pair_offsets_); entry != end(kv_pair_offsets_);
       entry++) {
    PutFixed32(&preamble, (*entry));
  }
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
