#ifndef LSM_ZNS_DEVICE_H
#define LSM_ZNS_DEVICE H

#include "spdk/stdinc.h"
#include "spdk/endian.h"
#include "spdk/log.h"
#include "spdk/nvme.h"
#include "spdk/vmd.h"
#include "spdk/nvme_ocssd.h"
#include "spdk/nvme_zns.h"
#include "spdk/env.h"
#include "spdk/nvme_intel.h"
#include "spdk/nvmf_spec.h"
#include "spdk/pci_ids.h"
#include "spdk/string.h"
#include "spdk/util.h"
#include "spdk/uuid.h"


namespace ZnsDevice{
    extern "C" {
        typedef struct {

        } Zone;

        typedef struct {
            bool done = false;
            int err = 0;
        } Completion;

        typedef struct {
            uint64_t lba_size;
            uint64_t zone_size;
            uint64_t mdts;
            uint64_t zasl;
            uint64_t lba_cap;
        } DeviceInfo;

        typedef struct {
            struct spdk_nvme_transport_id g_trid = {};
            struct spdk_nvme_ctrlr *ctrlr;
            spdk_nvme_ns *ns;
            DeviceInfo info = {};
        } DeviceManager;

        typedef struct {
            DeviceManager *manager;
            const char *traddr;
            const u_int8_t traddr_len;
            bool found;
        } DeviceProber;

        // Create 1 QPair for each thread that uses I/O.
        typedef struct {
            spdk_nvme_qpair *qpair;
            DeviceManager *man;
        } QPair;

        int
        z_init(DeviceManager **man);

        int
        z_shutdown(DeviceManager *man);

        int
        z_open(DeviceManager *man, const char* traddr);

        int
        z_close(DeviceManager *man);

        int
        z_get_device_info(DeviceInfo *info, DeviceManager *manager);

        int
        z_create_qpair(DeviceManager *man, QPair **qpair);

        int
        z_destroy_qpair(QPair *qpair);

        void*
        z_calloc(QPair *qpair, int nr, int size);

        void
        z_free(QPair *qpair, void * buffer);

        int
        z_append(QPair *qpair, uint64_t slba, void *buffer, uint64_t size);

        int
        z_read(QPair *qpair, uint64_t slba, void *buffer, uint64_t size);

        int
        z_reset(QPair *qpair, uint64_t slba, bool all);

        int
        z_get_zone_head(QPair *qpair, uint64_t slba, uint64_t *head);

        bool
        __probe_devices_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
	        struct spdk_nvme_ctrlr_opts *opts);

        void
        __attach_devices__cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
	        struct spdk_nvme_ctrlr *ctrlr, const struct spdk_nvme_ctrlr_opts *opts);  

        void*
        __reserve_dma(uint64_t size);

        int
        __get_block_alligned_size(uint64_t* size, uint64_t* blocks);

        static void
        __operation_complete(void *arg, const struct spdk_nvme_cpl *completion);

        static void
        __append_complete(void *arg, const struct spdk_nvme_cpl *completion);

        static void
        __read_complete(void *arg, const struct spdk_nvme_cpl *completion);

         static void
        __reset_zone_complete(void *arg, const struct spdk_nvme_cpl *completion);

        static void
        __get_zone_head_complete(void *arg, const struct spdk_nvme_cpl *completion);

        int
        __probe();

        int
        __attach();

        int
        __TEST_interface();
    }
}
#endif