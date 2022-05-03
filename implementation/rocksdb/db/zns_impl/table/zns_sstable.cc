#include "db/zns_impl/table/zns_sstable.h"

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
  channel_factory_->Unref();
  channel_factory_ = nullptr;
}

}  // namespace ROCKSDB_NAMESPACE
