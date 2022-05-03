#include "db/zns_impl/table/zns_sstable.h"

#include <memory>

#include "db/zns_impl/io/szd_port.h"

namespace ROCKSDB_NAMESPACE {
ZnsSSTable::ZnsSSTable(SZD::SZDChannelFactory* channel_factory,
                       const SZD::DeviceInfo& info,
                       const uint64_t min_zone_head,
                       const uint64_t max_zone_head)
    : min_zone_head_(min_zone_head),
      max_zone_head_(max_zone_head),
      zone_size_(info.zone_size),
      lba_size_(info.lba_size),
      mdts_(info.mdts),
      channel_factory_(channel_factory),
      buffer_(0, lba_size_) {
  assert(channel_factory_ != nullptr);
  channel_factory_->Ref();
}

ZnsSSTable::~ZnsSSTable() {
  printf("Deleting SSTable manager.\n");
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

void ZnsSSTable::GeneratePreamble(
    std::string* dst, const std::vector<uint32_t>& kv_pair_offsets_) {
  // TODO: this is not a bottleneck, but it is ugly...
  std::string preamble;
  PutFixed32(&preamble, kv_pair_offsets_.size());
  for (auto entry = begin(kv_pair_offsets_); entry != end(kv_pair_offsets_);
       entry++) {
    PutFixed32(&preamble, (*entry));
  }
  *dst = preamble.append(*dst);
}

size_t FindSS(const InternalKeyComparator& icmp,
              const std::vector<SSZoneMetaData*>& ss, const Slice& key) {
  size_t left = 0;
  size_t right = ss.size();
  // binary search I guess.
  while (left < right) {
    size_t mid = (left + right) / 2;
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
