#include "db/zns_impl/persistence/zns_manifest.h"

#include "db/zns_impl/io/szd_port.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {
ZnsManifest::ZnsManifest(SZD::SZDChannelFactory* channel_factory,
                         const SZD::DeviceInfo& info,
                         const uint64_t min_zone_head,
                         const uint64_t max_zone_head)
    : current_lba_(min_zone_head_),
      manifest_start_(max_zone_head_),  // enforce corruption
      manifest_end_(min_zone_head_),    // enforce corruption
      zone_head_(min_zone_head),
      write_head_(min_zone_head),
      zone_tail_(min_zone_head),
      min_zone_head_(min_zone_head),
      max_zone_head_(max_zone_head),
      zone_size_(info.zone_size),
      lba_size_(info.lba_size),
      channel_factory_(channel_factory) {
  assert(max_zone_head_ < info.lba_cap);
  assert(zone_head_ % info.lba_size == 0);
  assert(channel_factory_ != nullptr);
  channel_factory_->Ref();
  assert(min_zone_head_ < max_zone_head_);
  channel_factory_->register_channel(&channel_, min_zone_head_, max_zone_head_);
  committer_ = new ZnsCommitter(channel_, info);
}

ZnsManifest::~ZnsManifest() {
  // printf("Deleting manifest\n");
  delete committer_;
  if (channel_ != nullptr) {
    channel_factory_->unregister_channel(channel_);
  }
  channel_factory_->Unref();
  channel_factory_ = nullptr;
}

Status ZnsManifest::Scan() { return Status::NotSupported(); }

Status ZnsManifest::NewManifest(const Slice& record) {
  // TODO: wraparound
  if (!committer_->SpaceEnough(record, write_head_, max_zone_head_)) {
    return Status::NoSpace();
  }
  Status s = committer_->SafeCommit(record, &write_head_, min_zone_head_,
                                    max_zone_head_);
  zone_head_ = (write_head_ / zone_size_) * zone_size_;
  return s;
}

Status ZnsManifest::ValidateManifestPointers() const {
  if (manifest_start_ >= manifest_end_) {
    return Status::Corruption("manifest pointers");
  }
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
  Slice record;
  // Read data from commits. If necessary wraparound from end to start.
  if (manifest_end_ > manifest_start_) {
    committer_->GetCommitReader(manifest_start_, manifest_end_);
    while (committer_->SeekCommitReader(&record)) {
      manifest->append(record.ToString());
    }
    committer_->CloseCommit();
  } else {
    committer_->GetCommitReader(manifest_start_, max_zone_head_);
    while (committer_->SeekCommitReader(&record)) {
      manifest->append(record.ToString());
    }
    committer_->CloseCommit();
    committer_->GetCommitReader(min_zone_head_, manifest_end_);
    while (committer_->SeekCommitReader(&record)) {
      manifest->append(record.ToString());
    }
    committer_->CloseCommit();
  }
  return s;
}

Status ZnsManifest::GetCurrentWriteHead(uint64_t* current) {
  *current = write_head_;
  return Status::OK();
}

Status ZnsManifest::SetCurrent(uint64_t current) {
  assert(current > min_zone_head_ && current < max_zone_head_);
  Status s;
  int64_t tmp_manifest_start = current;
  uint64_t tmp_manifest_end = write_head_;
  // then install on storage
  std::string current_name = "CURRENT:";
  PutFixed64(&current_name, current);
  // printf("Setting current to %lu: %s", write_head_, current_name.data());
  s = committer_->SafeCommit(Slice(current_name), &write_head_, min_zone_head_,
                             max_zone_head_);
  zone_head_ = (write_head_ / zone_size_) * zone_size_;
  if (!s.ok()) {
    return s;
  }
  // Only install locally if succesful
  manifest_start_ = current_lba_ = tmp_manifest_start;
  manifest_end_ = tmp_manifest_end;
  return s;
}

Status ZnsManifest::RecoverLog() {
  Status s;
  uint64_t log_tail = min_zone_head_, log_head = min_zone_head_;
  // Scan for tail
  uint64_t slba;
  uint64_t zone_head = min_zone_head_, old_zone_head = min_zone_head_;
  for (slba = min_zone_head_; slba < max_zone_head_; slba += zone_size_) {
    if (!(s = FromStatus(channel_->ZoneHead(slba, &zone_head))).ok()) {
      return Status::IOError("Error getting zonehead for CURRENT");
    }
    // tail is at first zone that is not empty
    if (zone_head > slba) {
      log_tail = slba;
      break;
    }
    old_zone_head = zone_head;
  }
  // Scan for head
  for (; slba < max_zone_head_; slba += zone_size_) {
    if (!(s = FromStatus(channel_->ZoneHead(slba, &zone_head))).ok()) {
      return Status::IOError("Error getting zonehead for CURRENT");
    }
    // The first zone with a head more than 0 and less than max_zone, holds the
    // head of the manifest.
    if (zone_head > slba && zone_head < slba + zone_size_) {
      log_head = zone_head;
      break;
    }
    // Or the last zone that is completely filled.
    if (zone_head < slba + zone_size_ && old_zone_head > slba - zone_size_) {
      if (zone_head == old_zone_head) {
        continue;
      }
      log_head = slba;
      break;
    }
    old_zone_head = zone_head;
  }
  // if head < end and tail == 0, we need to be sure that the tail does not
  // start AFTER head.
  if (log_head > 0 && log_tail == 0) {
    for (slba += zone_size_; slba < max_zone_head_; slba += zone_size_) {
      if (!(s = FromStatus(channel_->ZoneHead(slba, &zone_head))).ok()) {
        return Status::IOError("Error getting zonehead for CURRENT");
      }
      if (zone_head > slba) {
        log_tail = slba;
        break;
      }
    }
  }
  write_head_ = log_head;
  zone_head_ = (log_head / zone_size_) * zone_size_;
  zone_tail_ = log_tail;
  return Status::OK();
}

Status ZnsManifest::TryParseCurrent(uint64_t slba, uint64_t* start_manifest) {
  Slice potential;
  committer_->GetCommitReader(slba, slba + 1);
  if (!committer_->SeekCommitReader(&potential)) {
    return Status::Corruption("CURRENT", "Invalid block");
  }
  // prevent memory errors on half reads
  if (potential.size() < sizeof("CURRENT:") - 1) {
    return Status::Corruption("CURRENT", "Header too small");
  }
  if (memcmp(potential.data(), "CURRENT:", sizeof("CURRENT:") - 1) != 0) {
    return Status::Corruption("CURRENT", "Broken header");
  }
  const char* current_text_header = potential.data() + sizeof("CURRENT:") - 1;
  Slice current_text_header_slice(current_text_header, 8);
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
  if (write_head_ == zone_tail_) {
    *start_manifest = *end_manifest = 0;
    return Status::NotFound("No current when empty");
  }
  // It is possible that a previous manifest was written, but not yet installed.
  // Therefore move back from head till tail until it is found.
  bool found = false;
  uint64_t slba = write_head_ == 0 ? max_zone_head_ - 1 : write_head_ - 1;
  for (; slba != zone_tail_;
       slba = slba == min_zone_head_ ? max_zone_head_ - 1 : slba - 1) {
    if (TryParseCurrent(slba, start_manifest).ok()) {
      current_lba_ = slba;
      *end_manifest = current_lba_;
      found = true;
      break;
    }
  }
  committer_->CloseCommit();
  if (!found) {
    return Status::NotFound("Did not find a valid CURRENT");
  }
  return Status::OK();
}

Status ZnsManifest::Recover() {
  Status s = Status::OK();
  s = RecoverLog();
  if (!s.ok()) return s;
  s = TryGetCurrent(&manifest_start_, &manifest_end_);
  if (!s.ok()) return s;
  return s;
}

Status ZnsManifest::RemoveObsoleteZones() { return Status::NotSupported(); }

Status ZnsManifest::Reset() {
  Status s = FromStatus(channel_->ResetAllZones());
  current_lba_ = min_zone_head_;
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
