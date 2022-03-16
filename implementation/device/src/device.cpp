#include "device.h"
#include "utils.h"

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
        int
        z_init(DeviceManager **manager) {
            *manager = (DeviceManager*)calloc(1,sizeof(DeviceManager));
            ERROR_ON_NULL(*manager, 1);
            // Setup options
            struct spdk_env_opts opts;
            opts.name = "m1";
            spdk_env_opts_init(&opts);
            // Setup SPDK
            (*manager)->g_trid = {};
            spdk_nvme_trid_populate_transport(&(*manager)->g_trid, SPDK_NVME_TRANSPORT_PCIE);
            if (spdk_env_init(&opts) < 0) {
                free(*manager);
		        return 2;
	        }
            // setup stub info
            (*manager)->info = {
                .lba_size = 0,
                .zone_size = 0,
                .mdts = 0,
                .zasl = 0
            };
            return 0;
        }

        int
        z_shutdown(DeviceManager *manager) {
            ERROR_ON_NULL(manager, 1);
            int rc = 0;
            if (manager->ctrlr != NULL) {
                rc = z_close(manager) | rc;
            }
            free(manager);
            return rc;
        }

        int
        z_open(DeviceManager *manager, const char* traddr) {
            DeviceProber prober = {
                .manager = manager,
                .traddr = traddr,
                .traddr_len = strlen(traddr),
                .found = false
            };
            // Find and open device
            struct spdk_nvme_probe_ctx *probe_ctx;
	        probe_ctx = (struct spdk_nvme_probe_ctx*)spdk_nvme_probe(&manager->g_trid, &prober, 
                __probe_devices_cb, (spdk_nvme_attach_cb)__attach_devices__cb, NULL);
            if (probe_ctx != 0) {
                spdk_env_fini();
		        return 1;
	        }
            if (!prober.found) {
                return 2;
            }
            int rc = z_get_device_info(&manager->info, manager);
            return 0;
        }

        int
        z_close(DeviceManager *manager) {
            ERROR_ON_NULL(manager->ctrlr,1);
            spdk_nvme_detach_async(manager->ctrlr, nullptr);
            manager->ctrlr = nullptr;
            manager->ns = nullptr;
            manager->info = {
                .lba_size = 0,
                .zone_size = 0,
                .mdts = 0,
                .zasl = 0
            };
            return 0;
        }

        int
        z_get_device_info(DeviceInfo *info, DeviceManager *manager) {
            ERROR_ON_NULL(info, 1);
            ERROR_ON_NULL(manager, 1);
            ERROR_ON_NULL(manager->ctrlr, 1);
            ERROR_ON_NULL(manager->ns, 1);
            const struct spdk_nvme_ns_data * ns_data = spdk_nvme_ns_get_data(manager->ns);
            const struct spdk_nvme_zns_ns_data *ns_data_zns = spdk_nvme_zns_ns_get_data(manager->ns);
            const struct spdk_nvme_ctrlr_data *ctrlr_data = spdk_nvme_ctrlr_get_data(manager->ctrlr);
            const spdk_nvme_zns_ctrlr_data *ctrlr_data_zns = spdk_nvme_zns_ctrlr_get_data(manager->ctrlr);
    	    union spdk_nvme_cap_register cap = spdk_nvme_ctrlr_get_regs_cap(manager->ctrlr);
            info->lba_size = 1 << ns_data->lbaf[ns_data->flbas.format].lbads;
            info->zone_size = ns_data_zns->lbafe[ns_data->flbas.format].zsze;
            info->mdts = (uint64_t)1 << (12 + cap.bits.mpsmin + ctrlr_data->mdts);
            info->zasl = ctrlr_data_zns->zasl;
            info->zasl = info->zasl == 0 ? info->mdts : (uint64_t)1 << (12 + cap.bits.mpsmin + info->zasl);
            return 0;
        }

        bool
        __probe_devices_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
	        struct spdk_nvme_ctrlr_opts *opts) {
                DeviceProber *prober = (DeviceProber*)cb_ctx;
                if (!prober->traddr) {
                    return false;
                }
                if (strlen((const char*)trid->traddr) < prober->traddr_len) {
                    return false;
                }
                if (strncmp((const char*)trid->traddr, prober->traddr, prober->traddr_len) != 0) {
                    return false;
                }
                return true;
        }

        void
        __attach_devices__cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
	        struct spdk_nvme_ctrlr *ctrlr, const struct spdk_nvme_ctrlr_opts *opts) {
            DeviceProber *prober = (DeviceProber*)cb_ctx;     
            if (prober == NULL) {
                return;
            }
            prober->manager->ctrlr = ctrlr;
            // take any ZNS namespace, we do not care which.
            for (int nsid = spdk_nvme_ctrlr_get_first_active_ns(ctrlr); nsid != 0;
	            nsid = spdk_nvme_ctrlr_get_next_active_ns(ctrlr, nsid)) {
                struct spdk_nvme_ns *ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
                if (ns == NULL) {
                    continue;
                }
                if (spdk_nvme_ns_get_csi(ns) != SPDK_NVME_CSI_ZNS) {
                    continue;
                }
                prober->manager->ns = ns;
                prober->found = true;
                break;
            }
            return;
        }

        int
        z_create_qpair(DeviceManager *man, QPair **qpair) {
            ERROR_ON_NULL(man, 1);
            *qpair = (QPair*)calloc(1,sizeof(QPair));
            (*qpair)->qpair = spdk_nvme_ctrlr_alloc_io_qpair(man->ctrlr, NULL, 0);
            (*qpair)->man = man;
            ERROR_ON_NULL((*qpair)->qpair, 1);
            return 0;
        }

        int
        z_destroy_qpair(QPair *qpair) {
            ERROR_ON_NULL(qpair, 1);
            ERROR_ON_NULL(qpair->qpair, 1);
            spdk_nvme_ctrlr_free_io_qpair(qpair->qpair);
            qpair->man = NULL;
            free(qpair);
            return 0;
        }

        void *
        __reserve_dma(uint64_t size) {
            return (char*)spdk_zmalloc(size, 0, NULL, SPDK_ENV_SOCKET_ID_ANY,
                SPDK_MALLOC_DMA);
        }

        static void
        __operation_complete(void *arg, const struct spdk_nvme_cpl *completion) {
            bool *completed = (bool*)arg;
            if (spdk_nvme_cpl_is_error(completion)) {
                fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
                fprintf(stderr, "Read I/O failed, aborting run\n");
                exit(1);
            }
            *completed = true;
        }

        static void
        __append_complete(void *arg, const struct spdk_nvme_cpl *completion) {
            __operation_complete(arg, completion);
        }

        static void
        __read_complete(void *arg, const struct spdk_nvme_cpl *completion) {
            __operation_complete(arg, completion);
        }

        static void
        __reset_zone_complete(void *arg, const struct spdk_nvme_cpl *completion) {
            __operation_complete(arg, completion);   
        }

        int
        z_read(QPair *qpair, uint64_t slba, void *buffer, uint64_t size) {
            // Completion completion = {
            //     .done = false
            // };
            // int rc = spdk_nvme_ns_cmd_read(qpair->man->ns, qpair, temp_buffer,
            //     slba, /* LBA start */
            //     0, /* number of LBAs */
            //     __read_complete, &completion, 0);
            // if (rc != 0) {
            //     return 1;
            // }
            // POLL_QPAIR(qpair->qpair, completion.done);
            // return rc;
            return 0;
        }

        int
        z_reset(QPair *qpair, uint64_t slba, bool all) {
            ERROR_ON_NULL(qpair, 1);
            Completion completion = {
                .done = false
            };
            int rc = spdk_nvme_zns_reset_zone(qpair->man->ns, qpair->qpair,
				     slba, /* starting LBA of the zone to reset */
				     all, /* don't reset all zones */
				     __reset_zone_complete,
				     &completion);
            if (rc != 0)
                return rc;
            POLL_QPAIR(qpair->qpair, completion.done);
            return rc;
        }

        /* Function used for validating linking and build issues.*/
        int
        __TEST_interface() {
            printf("TEST TEXT PRINTING\n");
            return 2022;
        }
    }
}