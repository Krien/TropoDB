#include "db/zns_impl/persistence/zns_manifest.h"

#include "db/zns_impl/io/szd_port.h"
#include "db/zns_impl/utils/tropodb_logger.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

static constexpr const char* current_preamble = "CURRENT:";
static constexpr const size_t current_preamble_size = strlen(current_preamble);

ZnsManifest::ZnsManifest(SZD::SZDChannelFactory* channel_factory,
                         const SZD::DeviceInfo& info,
                         const uint64_t min_zone_nr, const uint64_t max_zone_nr)
    : manifest_start_(max_zone_nr * info.zone_cap),  // enforce corruption
      manifest_blocks_(0),                           // enforce corruption
      manifest_start_new_(0),
      manifest_blocks_new_(0),
      deleted_range_begin_(0),
      deleted_range_blocks_(0),
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
    printf("Not enough space in Manifest: needed %lu available %lu\n",
           record.size() / lba_size_, log_.SpaceAvailable() / lba_size_);
    return Status::NoSpace();
  }
  // printf("Space available %lu %lu\n", log_.SpaceAvailable(), record.size());
  manifest_start_new_ = log_.GetWriteHead();
  Status s = committer_.SafeCommit(record, &manifest_blocks_new_);
  // printf("MANIFEST BLOCKS %lu \n", manifest_blocks_new_);
  return s;
}

Status ZnsManifest::SetCurrent() {
  assert(current > min_zone_head_ && current < max_zone_head_);
  Status s;
  // Reclaim old space
  uint64_t delete_blocks_ = (deleted_range_blocks_ / zone_cap_) * zone_cap_;
  // printf("DELETING IN MAN %lu %lu %lu \n", delete_blocks_,
  //        deleted_range_blocks_, zone_cap_);
  if (delete_blocks_ != 0) {
    s = FromStatus(log_.ConsumeTail(deleted_range_begin_,
                                    deleted_range_begin_ + delete_blocks_));
    if (!s.ok()) {
      printf("log eagain %lu %lu %lu\n", log_.GetWriteTail(),
             deleted_range_begin_, delete_blocks_);
    }

    deleted_range_blocks_ -= delete_blocks_;
    // printf("Space available after delete %lu %lu %lu\n",
    // log_.SpaceAvailable(),
    //        deleted_range_blocks_, delete_blocks_);
  }

  deleted_range_begin_ = log_.GetWriteTail();
  // +1 because of previous current
  deleted_range_blocks_ += manifest_blocks_ + 1;

  // then install on storage
  std::string current_name = current_preamble;
  PutFixed64(&current_name, manifest_start_new_);
  PutFixed64(&current_name, manifest_blocks_new_);
  PutFixed64(&current_name, deleted_range_begin_);
  PutFixed64(&current_name, deleted_range_blocks_);
  // printf("Set new current %lu %lu %lu %lu %lu %lu\n", manifest_start_new_,
  //        manifest_blocks_new_, deleted_range_begin_, deleted_range_blocks_,
  //        log_.SpaceAvailable() / lba_size_, (max_zone_head_ -
  //        min_zone_head_));
  s = committer_.SafeCommit(Slice(current_name));
  if (!s.ok()) {
    TROPODB_ERROR("error setting current\n");
    return s;
  }

  // Only install locally if succesful
  manifest_start_ = manifest_start_new_;
  manifest_blocks_ = manifest_blocks_new_;
  return s;
}

Status ZnsManifest::TryParseCurrent(uint64_t slba, uint64_t* start_manifest,
                                    uint64_t* end_manifest,
                                    uint64_t* start_manifest_delete,
                                    uint64_t* end_manifest_delete,
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
  Slice current_text_header_slice(current_text_header,
                                  sizeof(uint64_t) * 4 + 1);
  if (!(GetFixed64(&current_text_header_slice, start_manifest) &&
        GetFixed64(&current_text_header_slice, end_manifest) &&
        GetFixed64(&current_text_header_slice, start_manifest_delete) &&
        GetFixed64(&current_text_header_slice, end_manifest_delete))) {
    return Status::Corruption("CURRENT", "Corrupt pointers");
  }
  // printf("Gotten current %lu %lu %lu %lu \n", *start_manifest, *end_manifest,
  //        *start_manifest_delete, *end_manifest_delete);
  if (*start_manifest < min_zone_head_ || *start_manifest > max_zone_head_ ||
      *end_manifest > (max_zone_head_ - min_zone_head_)) {
    return Status::Corruption("CURRENT", "Invalid pointers");
  }
  return Status::OK();
}

Status ZnsManifest::TryGetCurrent(uint64_t* start_manifest,
                                  uint64_t* end_manifest,
                                  uint64_t* start_manifest_delete,
                                  uint64_t* end_manifest_delete) {
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
    if (TryParseCurrent(slba, start_manifest, end_manifest,
                        start_manifest_delete, end_manifest_delete, reader)
            .ok()) {
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
  s = TryGetCurrent(&manifest_start_, &manifest_blocks_, &deleted_range_begin_,
                    &deleted_range_blocks_);
  if (!s.ok()) return s;
  // printf(
  //     "Manifest start %lu Manifest end %lu deleted range begin %lu end %lu
  //     \n", manifest_start_, manifest_blocks_, deleted_range_begin_,
  //     deleted_range_blocks_);
  return s;
}

Status ZnsManifest::ValidateManifestPointers() const {
  const uint64_t write_head_ = log_.GetWriteHead();
  const uint64_t zone_tail_ = log_.GetWriteTail();
  if (write_head_ > zone_tail_) {
    if (manifest_start_ > write_head_ ||
        manifest_start_ + manifest_blocks_ > write_head_ ||
        manifest_start_ < zone_tail_ ||
        manifest_start_ + manifest_blocks_ < zone_tail_) {
      return Status::Corruption("manifest pointers");
    }
  } else {
    if ((manifest_start_ > write_head_ && manifest_start_ < zone_tail_) ||
        (manifest_start_ + manifest_blocks_ > write_head_ &&
         manifest_start_ + manifest_blocks_ < zone_tail_)) {
      return Status::Corruption("manifest pointers");
    }
  }
  return Status::OK();
}

Status ZnsManifest::ReadManifest(std::string* manifest) {
  TROPODB_INFO("read manifest\n");
  Status s = ValidateManifestPointers();
  if (!s.ok()) {
    return s;
  }
  if (manifest_blocks_ == 0) {
    return Status::IOError();
  }

  Slice record;
  // Read data from commits. If necessary wraparound from end to start.
  ZnsCommitReader reader;
  s = committer_.GetCommitReader(0, manifest_start_,
                                 manifest_start_ + manifest_blocks_, &reader);
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
