#include "db/zns_impl/zns_manifest.h"

#include "db/zns_impl/device_wrapper.h"
#include "db/zns_impl/zns_utils.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
    ZnsManifest::ZnsManifest(QPairFactory* qpair_factory, const ZnsDevice::DeviceInfo& info,
               const uint64_t min_zone_head, uint64_t max_zone_head)
            :   current_lba_(min_zone_head_),
                zone_head_(min_zone_head),
                write_head_(min_zone_head),
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
        printf("Deleting maifest\n");
        if (qpair_ != nullptr) {
            qpair_factory_->unregister_qpair(*qpair_);
            delete qpair_;
        }
        qpair_factory_->Unref();
    }

    Status
    ZnsManifest::Scan() {
        return Status::NotSupported();
    }

    Status
    ZnsManifest::NewManifest(const Slice& record) {
        uint64_t zcalloc_size = 0;
        char* payload =
            ZnsUtils::slice_to_spdkformat(&zcalloc_size, record, *qpair_, lba_size_);
        if (payload == nullptr) {
            return Status::IOError("Error in SPDK malloc for Manifest");
        }
        // TODO REQUEST SPACE!!!
        int rc = ZnsDevice::z_append(*qpair_, zone_head_, payload, zcalloc_size);
        if (rc != 0 ) {
            return Status::IOError("Error in writing Manifest");
        }
        ZnsUtils::update_zns_heads(&write_head_, &zone_head_, zcalloc_size, lba_size_,
                             zone_size_);
        ZnsDevice::z_free(*qpair_, payload);
        return Status::OK();
    }

    Status
    ZnsManifest::GetCurrentWriteHead(uint64_t* current) {
        *current = write_head_;
        return Status::OK();
    }


    Status
    ZnsManifest::SetCurrent(uint64_t current) {
        assert(current > min_zone_head_ && current < max_zone_head_);
        current_lba_ = current;
        Slice current_name = "CURRENT:" + std::to_string(current);
        uint64_t zcalloc_size = 0;
        char* payload =
            ZnsUtils::slice_to_spdkformat(&zcalloc_size, current_name, *qpair_, lba_size_);
        if (payload == nullptr) {
            return Status::IOError("Error in SPDK malloc for CURRENT");
        }
        // TODO REQUEST SPACE!!!
        printf("Setting current to %lu: %s", write_head_, current_name.data());
        int rc = ZnsDevice::z_append(*qpair_, zone_head_, payload, zcalloc_size);
        if (rc != 0 ) {
            return Status::IOError("Error in writing CURRENT");
        }
        ZnsUtils::update_zns_heads(&write_head_, &zone_head_, zcalloc_size, lba_size_,
                             zone_size_);
        ZnsDevice::z_free(*qpair_, payload);
        return Status::OK();
    }

    Status
    ZnsManifest::RemoveObsoleteZones() {
        return Status::NotSupported();
    }

    Status
    ZnsManifest::Reset() {
        for (uint64_t slba = min_zone_head_; slba < max_zone_head_; slba+=zone_size_) {
            int rc = ZnsDevice::z_reset(*qpair_, slba, false);
            if (rc != 0) {
                return Status::IOError("Error during zone reset of Manifest");
            }
        }
        current_lba_ = min_zone_head_;
        return Status::OK();
    }

}