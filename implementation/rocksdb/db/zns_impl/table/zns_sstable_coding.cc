#include "db/zns_impl/table/zns_sstable_coding.h"

#include "db/zns_impl/config.h"

namespace ROCKSDB_NAMESPACE {
void EncodeKVPair(std::string* dst, const Slice& key, const Slice& value) {
  PutVarint32(dst, key.size());
  PutVarint32(dst, value.size());
  dst->append(key.data(), key.size());
  dst->append(value.data(), value.size());
}

void EncodeSSTablePreamble(std::string* dst,
                           const std::vector<uint32_t>& kv_pair_offsets_) {
  // TODO: this is not a bottleneck, but it is ugly...
  std::string preamble;
  if (ZnsConfig::use_sstable_encoding) {
    PutFixed32(&preamble,
               dst->size() + (kv_pair_offsets_.size() + 2) * sizeof(uint32_t));
  }
  PutFixed32(&preamble, kv_pair_offsets_.size());
  for (size_t i = 0; i < kv_pair_offsets_.size(); i++) {
    PutFixed32(&preamble, kv_pair_offsets_[i]);
  }
  *dst = preamble.append(*dst);
}
}  // namespace ROCKSDB_NAMESPACE
