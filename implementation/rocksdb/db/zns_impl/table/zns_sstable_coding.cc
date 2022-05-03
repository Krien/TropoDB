#include "db/zns_impl/table/zns_sstable_coding.h"

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
  PutFixed32(&preamble, kv_pair_offsets_.size());
  for (auto entry = begin(kv_pair_offsets_); entry != end(kv_pair_offsets_);
       entry++) {
    PutFixed32(&preamble, (*entry));
  }
  *dst = preamble.append(*dst);
}
}  // namespace ROCKSDB_NAMESPACE
