#include "device.h"

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
        #define POLL_QPAIR(qair, target) do{                    \
            while(!(target)) {                                  \
                spdk_nvme_qpair_process_completions(qpair,0);   \
            }                                                   \
        } while(0)

        void *
        __reserve_dma(uint64_t size) {
            return (char*)spdk_zmalloc(size, lba_size_in_use, NULL, SPDK_ENV_SOCKET_ID_ANY,
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

        static int
        read(void *buffer, uint64_t slba, uint64_t size) {
            spdk_nvme_qpair qpair;
            struct Completion completion = {
                .done = false
            };
            int rc = spdk_nvme_ns_cmd_read(ns_entry->ns, qpair, temp_buffer,
                slba, /* LBA start */
                lbas, /* number of LBAs */
                __read_complete, &completion, 0);
            POLL_QPAIR(qpair, completion.done);
        }

    }
}