#include "db/zns_impl/zns_version.h"

#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
Status ZnsVersion::Get(const ReadOptions& options, const Slice& key,
                       std::string* value) {
  Status s;
  const Comparator* ucmp = vset_->icmp_.user_comparator();
  ZNSSSTableManager* znssstable = vset_->znssstable_;
  znssstable->Ref();

  // L0 (no sorting of L0 yet, because it is an append-only log. Earlier zones
  // are guaranteed to be older). So start from end to begin
  std::vector<SSZoneMetaData*> tmp;
  tmp.reserve(ss_[0].size());
  for (size_t i = ss_[0].size(); i != 0; --i) {
    SSZoneMetaData* z = ss_[0][i - 1];
    if (ucmp->Compare(key, z->smallest.user_key()) >= 0 &&
        ucmp->Compare(key, z->largest.user_key()) <= 0) {
      tmp.push_back(z);
    }
  }
  for (uint32_t i = 0; i < tmp.size(); i++) {
    s = znssstable->Get(key, value, tmp[i]);
    if (s.ok()) {
      znssstable->Unref();
      return s;
    }
  }
  znssstable->Unref();
  return Status::NotFound();
}

Status ZnsVersionSet::LogAndApply(ZnsVersionEdit* edit) {
  Status s = Status::OK();
  // TODO sanity checking...

  // TODO improve...
  ZnsVersion* v = new ZnsVersion(this);
  for (size_t i = 0; i < 7; i++) {
    const std::vector<SSZoneMetaData*>& base_ss = current_->ss_[i];
    std::vector<SSZoneMetaData*>::const_iterator base_iter = base_ss.begin();
    std::vector<SSZoneMetaData*>::const_iterator base_end = base_ss.end();
    for (; base_iter != base_end; ++base_iter) {
      SSZoneMetaData* m = *base_iter;
      m->refs++;
      v->ss_[i].push_back(m);
    }
  }
  for (size_t i = 0; i < edit->new_ss_.size(); i++) {
    const int level = edit->new_ss_[i].first;
    SSZoneMetaData* m = new SSZoneMetaData(edit->new_ss_[i].second);
    m->refs = 1;
    v->ss_[level].push_back(m);
  }
  AppendVersion(v);
  // MANIFEST STUFF...

  // Installing?
  return s;
}

void ZnsVersionSet::AppendVersion(ZnsVersion* v) {
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != nullptr) {
    current_->Unref();
  }
  current_ = v;
  v->Ref();
}

void ZnsVersionEdit::AddSSDefinition(int level, uint64_t lba,
                                     uint64_t lba_count, uint64_t numbers,
                                     const InternalKey& smallest,
                                     const InternalKey& largest) {
  SSZoneMetaData f;
  f.lba = lba;
  f.numbers = numbers;
  f.lba_count = lba_count;
  f.smallest = smallest;
  f.largest = largest;
  new_ss_.push_back(std::make_pair(level, f));
}
}  // namespace ROCKSDB_NAMESPACE