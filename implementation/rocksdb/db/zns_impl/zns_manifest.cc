#include "db/zns_impl/zns_manifest.h"

#include "db/zns_impl/device_wrapper.h"
#include "db/zns_impl/zns_utils.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {
ZnsManifest::ZnsManifest(QPairFactory* qpair_factory,
                         const ZnsDevice::DeviceInfo& info,
                         const uint64_t min_zone_head, uint64_t max_zone_head)
    : current_lba_(min_zone_head_),
      current_text_(nullptr),
      manifest_start_(max_zone_head_),  // enforce corruption
      manifest_end_(min_zone_head_),    // enforce corruption
      zone_head_(min_zone_head),
      write_head_(min_zone_head),
      zone_tail_(min_zone_head),
      min_zone_head_(min_zone_head),
      max_zone_head_(max_zone_head),
      zone_size_(info.zone_size),
      lba_size_(info.lba_size),
      qpair_factory_(qpair_factory) {
  assert(max_zone_head_ < info.lba_cap);
  assert(zone_head_ % info.lba_size == 0);
  assert(qpair_factory_ != nullptr);
  assert(min_zone_head_ < max_zone_head_);
  qpair_ = new ZnsDevice::QPair*[1];
  qpair_factory_->Ref();
  qpair_factory_->register_qpair(qpair_);
}

ZnsManifest::~ZnsManifest() {
  printf("Deleting manifest\n");
  if (current_text_ != nullptr) {
    ZnsDevice::z_free(*qpair_, current_text_);
    current_text_ = nullptr;
  }
  if (qpair_ != nullptr) {
    qpair_factory_->unregister_qpair(*qpair_);
    delete qpair_;
  }
  qpair_factory_->Unref();
}

Status ZnsManifest::Scan() { return Status::NotSupported(); }

Status ZnsManifest::NewManifest(const Slice& record) {
  uint64_t zcalloc_size = 0;
  char* payload =
      ZnsUtils::slice_to_spdkformat(&zcalloc_size, record, *qpair_, lba_size_);
  if (payload == nullptr) {
    return Status::IOError("Error in SPDK malloc for Manifest");
  }
  // TODO REQUEST SPACE!!!
  int rc = ZnsDevice::z_append(*qpair_, zone_head_, payload, zcalloc_size);
  ZnsDevice::z_free(*qpair_, payload);
  if (rc != 0) {
    return Status::IOError("Error in writing Manifest");
  }
  ZnsUtils::update_zns_heads(&write_head_, &zone_head_, zcalloc_size, lba_size_,
                             zone_size_);
  return Status::OK();
}

Status ZnsManifest::ReadManifest(std::string* manifest) {
  if (manifest_start_ >= manifest_end_) {
    return Status::Corruption("Corrupted manifest pointers");
  }
  if (write_head_ > zone_tail_) {
    if (manifest_start_ > write_head_ || manifest_end_ > write_head_ ||
        manifest_start_ < zone_tail_ || manifest_end_ < zone_tail_) {
      return Status::Corruption("Corrupted manifest pointers");
    }
  } else {
    if ((manifest_start_ > write_head_ && manifest_start_ < zone_tail_) ||
        (manifest_end_ > write_head_ && manifest_end_ < zone_tail_)) {
      return Status::Corruption("Corrupted manifest pointers");
    }
  }
  uint64_t lbas =
      manifest_end_ > manifest_start_
          ? manifest_end_ - manifest_start_
          : max_zone_head_ - manifest_start_ + manifest_end_ - min_zone_head_;
  char* manifest_buffer_ =
      (char*)ZnsDevice::z_calloc(*qpair_, 1, lbas * lba_size_);
  if (manifest_buffer_ == nullptr) {
    return Status::MemoryLimit("Problems allocating DMA");
  }
  int rc;
  if (manifest_end_ > manifest_start_) {
    rc = ZnsDevice::z_read(*qpair_, manifest_start_, manifest_buffer_,
                           lbas * lba_size_);
    if (rc != 0) {
      ZnsDevice::z_free(*qpair_, manifest_buffer_);
      return Status::IOError("Error reading manifest");
    }
    manifest->append(manifest_buffer_, lbas * lba_size_);
    ZnsDevice::z_free(*qpair_, manifest_buffer_);
  } else {
    rc = ZnsDevice::z_read(*qpair_, manifest_start_, manifest_buffer_,
                           (max_zone_head_ - manifest_start_) * lba_size_);
    if (rc != 0) {
      ZnsDevice::z_free(*qpair_, manifest_buffer_);
      return Status::IOError("Error reading manifest");
    }
    rc = ZnsDevice::z_read(
        *qpair_, min_zone_head_,
        manifest_buffer_ + (max_zone_head_ - manifest_start_) * lba_size_,
        (manifest_end_ - min_zone_head_) * lba_size_);
    if (rc != 0) {
      ZnsDevice::z_free(*qpair_, manifest_buffer_);
      return Status::IOError("Error reading manifest");
    }
    manifest->append(manifest_buffer_, lbas * lba_size_);
    ZnsDevice::z_free(*qpair_, manifest_buffer_);
  }
  return Status::OK();
}

Status ZnsManifest::GetCurrentWriteHead(uint64_t* current) {
  *current = write_head_;
  return Status::OK();
}

Status ZnsManifest::SetCurrent(uint64_t current) {
  assert(current > min_zone_head_ && current < max_zone_head_);
  current_lba_ = current;
  std::string current_name = "CURRENT:";
  PutFixed64(&current_name, current);
  uint64_t zcalloc_size = 0;
  char* payload = ZnsUtils::slice_to_spdkformat(
      &zcalloc_size, Slice(current_name), *qpair_, lba_size_);
  if (payload == nullptr) {
    return Status::IOError("Error in SPDK malloc for CURRENT");
  }
  // TODO REQUEST SPACE!!!
  printf("Setting current to %lu: %s", write_head_, current_name.data());
  int rc = ZnsDevice::z_append(*qpair_, zone_head_, payload, zcalloc_size);
  if (rc != 0) {
    return Status::IOError("Error in writing CURRENT");
  }
  // install locally as well
  manifest_start_ = current;
  manifest_end_ = write_head_;
  // make progress
  ZnsUtils::update_zns_heads(&write_head_, &zone_head_, zcalloc_size, lba_size_,
                             zone_size_);
  ZnsDevice::z_free(*qpair_, payload);
  return Status::OK();
}

Status ZnsManifest::RecoverLog() {
  uint64_t log_tail = min_zone_head_, log_head = min_zone_head_;
  // Scan for tail
  uint64_t slba;
  uint64_t zone_head = min_zone_head_, old_zone_head = min_zone_head_;
  for (slba = min_zone_head_; slba < max_zone_head_; slba += zone_size_) {
    if (ZnsDevice::z_get_zone_head(*qpair_, slba, &zone_head) != 0) {
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
    if (ZnsDevice::z_get_zone_head(*qpair_, slba, &zone_head) != 0) {
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
      if (ZnsDevice::z_get_zone_head(*qpair_, slba, &zone_head) != 0) {
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
  int rc = 0;
  if (current_text_ == nullptr) {
    current_text_ = (char*)ZnsDevice::z_calloc(*qpair_, 1, lba_size_);
  }
  if (current_text_ == nullptr) {
    return Status::MemoryLimit("Problems allocating DMA");
  }
  rc = ZnsDevice::z_read(*qpair_, slba, current_text_, lba_size_);
  if (rc != 0) {
    return Status::IOError("Error reading potential CURRENT address");
  }
  if (memcmp(current_text_, "CURRENT:", sizeof("CURRENT:") - 1) != 0) {
    return Status::Corruption("Corrupt CURRENT block");
  }
  char* current_text_header = current_text_ + sizeof("CURRENT:") - 1;
  Slice current_text_header_slice(current_text_header, 8);
  if (!GetFixed64(&current_text_header_slice, start_manifest)) {
    return Status::Corruption("Corrupt CURRENT header");
  }
  if (*start_manifest < min_zone_head_ || *start_manifest > max_zone_head_) {
    return Status::Corruption("Corrupt CURRENT header");
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
  // Therefore move back till found.
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
  if (current_text_ != nullptr) {
    ZnsDevice::z_free(*qpair_, current_text_);
    current_text_ = nullptr;
  }
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
  for (uint64_t slba = min_zone_head_; slba < max_zone_head_;
       slba += zone_size_) {
    int rc = ZnsDevice::z_reset(*qpair_, slba, false);
    if (rc != 0) {
      return Status::IOError("Error during zone reset of Manifest");
    }
  }
  current_lba_ = min_zone_head_;
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE