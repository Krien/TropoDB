#include "db/zns_impl/persistence/zns_manifest.h"

#include "db/zns_impl/io/szd_port.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

static constexpr const char* current_preamble = "CURRENT:";
static constexpr const size_t current_preamble_size = strlen(current_preamble);

ZnsManifest::ZnsManifest(SZD::SZDChannelFactory* channel_factory,
                         const SZD::DeviceInfo& info,
                         const uint64_t min_zone_nr, const uint64_t max_zone_nr)
    : current_lba_(min_zone_nr * info.zone_cap),
      manifest_start_(max_zone_nr * info.zone_cap),  // enforce corruption
      manifest_end_(min_zone_nr * info.zone_cap),    // enforce corruption
      log_(channel_factory, info, min_zone_nr, max_zone_nr, 1),
      committer_(&log_, info, true),
      min_zone_head_(min_zone_nr * info.zone_cap),
      max_zone_head_(max_zone_nr * info.zone_cap),
      zone_cap_(info.zone_cap),
      lba_size_(info.lba_size),
      channel_factory_(channel_factory) {
  assert(max_zone_head_ < info.lba_cap);
  assert(channel_factory_ != nullptr);
  channel_factory_->Ref();
  assert(min_zone_head_ < max_zone_head_);
}

ZnsManifest::~ZnsManifest() { channel_factory_->Unref(); }

Status ZnsManifest::NewManifest(const Slice& record) {
  if (!committer_.SpaceEnough(record)) {
    printf("Not enough space in Manifest\n");
    return Status::NoSpace();
  }
  return committer_.SafeCommit(record);
}

Status ZnsManifest::GetCurrentWriteHead(uint64_t* current) {
  *current = log_.GetWriteHead();
  return Status::OK();
}

Status ZnsManifest::SetCurrent(uint64_t current) {
  assert(current > min_zone_head_ && current < max_zone_head_);
  Status s;
  int64_t tmp_manifest_start = current;
  uint64_t tmp_manifest_end = log_.GetWriteHead();
  // then install on storage
  std::string current_name = current_preamble;
  PutFixed64(&current_name, current);
  s = committer_.SafeCommit(Slice(current_name));
  if (!s.ok()) {
    printf("error setting current\n");
    return s;
  }
  // Reclaim old space
  uint64_t tail = log_.GetWriteTail();
  uint64_t new_tail = 0;
  // wraparound
  if (current - 1 < tail) {
    new_tail += max_zone_head_ - tail;
    if (current > min_zone_head_) {
      new_tail += current - min_zone_head_ - 1;
    }
  } else {
    new_tail += current - tail - 1;
  }
  // printf("current %lu tail %lu new tail %lu, max %lu min %lu, head %lu\n",
  //        current, tail, new_tail, max_zone_head_, min_zone_head_,
  //        log_.GetWriteHead());
  new_tail = (new_tail / zone_cap_) * zone_cap_;
  if (new_tail != 0 && manifest_start_ != manifest_end_) {
    // printf("tail %lu new tail %lu \n", tail, new_tail);
    s = FromStatus(log_.ConsumeTail(tail, tail + new_tail));
    if (!s.ok()) {
      printf("log eagain %lu %lu %lu\n", tail, new_tail, log_.GetWriteHead());
    }
  }
  // Only install locally if succesful
  manifest_start_ = current_lba_ = tmp_manifest_start;
  manifest_end_ = tmp_manifest_end;
  return s;
}

Status ZnsManifest::TryParseCurrent(uint64_t slba, uint64_t* start_manifest,
                                    ZnsCommitReader& reader) {
  Slice potential;
  committer_.GetCommitReader(0, slba, slba + 1, &reader);
  if (!committer_.SeekCommitReader(reader, &potential)) {
    return Status::Corruption("CURRENT", "Invalid block");
  }
  // prevent memory errors on half reads
  if (potential.size() < current_preamble_size) {
    return Status::Corruption("CURRENT", "Header too small");
  }
  if (memcmp(potential.data(), current_preamble, current_preamble_size) != 0) {
    return Status::Corruption("CURRENT", "Broken header");
  }
  const char* current_text_header = potential.data() + current_preamble_size;
  Slice current_text_header_slice(current_text_header, sizeof(uint64_t) + 1);
  if (!GetFixed64(&current_text_header_slice, start_manifest)) {
    return Status::Corruption("CURRENT", "Corrupt pointers");
  }
  if (*start_manifest < min_zone_head_ || *start_manifest > max_zone_head_) {
    return Status::Corruption("CURRENT", "Invalid pointers");
  }
  return Status::OK();
}

Status ZnsManifest::TryGetCurrent(uint64_t* start_manifest,
                                  uint64_t* end_manifest) {
  if (log_.Empty()) {
    *start_manifest = *end_manifest = 0;
    return Status::NotFound("No current when empty");
  }
  // It is possible that a previous manifest was written, but not yet installed.
  // Therefore move back from head till tail until it is found.
  bool found = false;
  ZnsCommitReader reader;
  uint64_t slba = log_.GetWriteHead() == min_zone_head_
                      ? max_zone_head_ - 1
                      : log_.GetWriteHead() - 1;
  for (; slba != log_.GetWriteTail();
       slba = slba == min_zone_head_ ? max_zone_head_ - 1 : slba - 1) {
    if (TryParseCurrent(slba, start_manifest, reader).ok()) {
      current_lba_ = slba;
      *end_manifest = current_lba_;
      found = true;
      break;
    }
  }
  committer_.CloseCommit(reader);
  if (!found) {
    return Status::NotFound("Did not find a valid CURRENT");
  }
  return Status::OK();
}

Status ZnsManifest::Recover() {
  Status s = Status::OK();
  s = RecoverLog();
  // printf("HEAD %lu TAIL %lu \n", log_.GetWriteHead(), log_.GetWriteTail());
  if (!s.ok()) {
    return s;
  }
  s = TryGetCurrent(&manifest_start_, &manifest_end_);
  if (!s.ok()) return s;
  return s;
}

Status ZnsManifest::ValidateManifestPointers() const {
  const uint64_t write_head_ = log_.GetWriteHead();
  const uint64_t zone_tail_ = log_.GetWriteTail();
  if (write_head_ > zone_tail_) {
    if (manifest_start_ > write_head_ || manifest_end_ > write_head_ ||
        manifest_start_ < zone_tail_ || manifest_end_ < zone_tail_) {
      return Status::Corruption("manifest pointers");
    }
  } else {
    if ((manifest_start_ > write_head_ && manifest_start_ < zone_tail_) ||
        (manifest_end_ > write_head_ && manifest_end_ < zone_tail_)) {
      return Status::Corruption("manifest pointers");
    }
  }
  return Status::OK();
}

Status ZnsManifest::ReadManifest(std::string* manifest) {
  printf("read manifest\n");
  Status s = ValidateManifestPointers();
  if (!s.ok()) {
    return s;
  }
  if (manifest_start_ == manifest_end_) {
    return Status::IOError();
  }

  Slice record;
  // Read data from commits. If necessary wraparound from end to start.
  ZnsCommitReader reader;
  s = committer_.GetCommitReader(0, manifest_start_, manifest_end_, &reader);
  if (!s.ok()) {
    return s;
  }
  while (committer_.SeekCommitReader(reader, &record)) {
    manifest->append(record.ToString());
  }
  committer_.CloseCommit(reader);
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
