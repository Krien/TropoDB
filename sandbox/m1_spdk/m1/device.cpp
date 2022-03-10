/*
 * MIT License
Copyright (c) 2021 - current
Authors:  Animesh Trivedi
This code is part of the Storage System Course at VU Amsterdam
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */

#include "device.h"

extern "C" {
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


bool
probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
	 struct spdk_nvme_ctrlr_opts *opts)
{
	printf("Probing %s\n", trid->traddr);
	return true;
}

bool
attach_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
	  struct spdk_nvme_ctrlr *ctrlr, const struct spdk_nvme_ctrlr_opts *opts)
{

    struct device_manager *manager = (struct device_manager *)cb_ctx;
    const struct spdk_nvme_ctrlr_data *cdata = spdk_nvme_ctrlr_get_data(ctrlr);

    // Iterate over the namespaces
    int nsid;
    struct spdk_nvme_ns *ns;
    struct spdk_nvme_ns_data *nsdata;
    for (nsid = spdk_nvme_ctrlr_get_first_active_ns(ctrlr); nsid != 0;
	     nsid = spdk_nvme_ctrlr_get_next_active_ns(ctrlr, nsid)) {
        ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
        if (ns == NULL) {
            continue;
        }

        struct ns_data dat = {
            .lba_size_in_use = 0,
            .mdt = 0,
            .is_zns = false,
            .zasl = 0,
            .zone_size = 0
        };

        if (spdk_nvme_ns_get_csi(ns) == SPDK_NVME_CSI_ZNS) {
             nsdata = (spdk_nvme_ns_data*)spdk_nvme_ns_get_data(ns);
            dat.lba_size_in_use = 1 << nsdata->lbaf[nsdata->flbas.format].lbads;
	        union spdk_nvme_cap_register cap = spdk_nvme_ctrlr_get_regs_cap(ctrlr);
            dat.mdt = (uint64_t)1 << (12 + cap.bits.mpsmin + cdata->mdts);
            const struct spdk_nvme_zns_ns_data *nsdata_zns = spdk_nvme_zns_ns_get_data(ns);
            dat.is_zns = true;
            dat.zasl = spdk_nvme_zns_ctrlr_get_data(ctrlr)->zasl;
            dat.zone_size = nsdata_zns->lbafe[nsdata->flbas.format].zsze;
            printf("OK %du \n", dat.zone_size);
        }

        struct ns_entry *ns_ = (struct ns_entry *)calloc(1, sizeof(struct ns_entry));
        ns_->ctrlr = ctrlr;
        ns_->ns = ns;
        ns_->qpair = spdk_nvme_ctrlr_alloc_io_qpair(ctrlr, NULL, 0);
        ns_->data = dat;
        ns_->trid = (struct spdk_nvme_transport_id *) trid;
        snprintf(ns_->name, sizeof(ns_->name), "%s", cdata->sn);
        TAILQ_INSERT_TAIL(&g_namespaces, ns_, link);
    }


    //TAILQ_INSERT_TAIL(&manager->g_controllers, ns_, link);

	return true;
}

bool identify_devices(struct device_manager *manager)
{
    ERROR_ON_NULL(manager);
    //struct device_information *info = (struct device_information *)calloc(1, sizeof(struct device_information*));

	struct spdk_nvme_probe_ctx *probe_ctx;
	probe_ctx = spdk_nvme_probe_async(&manager->g_trid, NULL, probe_cb,
					  (spdk_nvme_attach_cb)attach_cb, NULL);
	if (!probe_ctx) {
		SPDK_ERRLOG("Create probe context failed\n");
		return false;
	}

    int rc = 0;
	while (true) {
		rc = spdk_nvme_probe_poll_async(probe_ctx);
		if (rc != -EAGAIN) {
			break;
		}
	}
    return true;
};



int 
count_and_show_all_nvme_devices(struct device_manager *manager) {
    ERROR_ON_NULL(manager);

	struct ns_entry			*ns_entry, *ns_entry_temp;
    int counter = 0;
    struct ns_data dat = {};

	TAILQ_FOREACH_SAFE(ns_entry, &g_namespaces, link, ns_entry_temp) {
        counter++;
        const struct spdk_nvme_ctrlr_data *cdata = spdk_nvme_ctrlr_get_data(ns_entry->ctrlr);
        dat = ns_entry->data;
        struct spdk_nvme_ns_data *nsdata = (spdk_nvme_ns_data*)spdk_nvme_ns_get_data(ns_entry->ns);
        printf("Displaying to %s,\n\t - NQN=%s\n\t - transport=%s - %u\n", ns_entry->trid->traddr, 
            ns_entry->trid->subnqn, ns_entry->trid->trstring, ns_entry->trid->trtype);
        printf("\t%s\n",ns_entry->name);
        printf("\tController:\n\t\t%s:%u\n", cdata->mn, cdata->sn);
        printf("\tNamespace:\n");
        printf("\t\tNamespace ID:%d\n", spdk_nvme_ns_get_id(ns_entry->ns));
        printf("\t\tlba_cap:%lu, lbas:%lu, guid:%lu\n",nsdata->ncap, nsdata->nsze, nsdata->nguid);
        for (int i = 0; i <= nsdata->nlbaf; i++) {
		    printf("\t\tLBA Format #%02d: Data Size: %5d  Metadata Size: %5d\n",
		           i, 1 << nsdata->lbaf[i].lbads, nsdata->lbaf[i].ms);
            if (dat.is_zns) {
                const struct spdk_nvme_zns_ns_data *nsdata_zns = spdk_nvme_zns_ns_get_data(ns_entry->ns);
                printf("\t\tLBA Format Extension #%02d: Zone Size (in LBAs): 0x%"PRIx64" Zone Descriptor Extension Size: %d bytes\n",
     			           i, nsdata_zns->lbafe[i].zsze, nsdata_zns->lbafe[i].zdes << 6);
                printf("\t\tMDT is %lu, ZASL is %lu, zone_size is %lu \n", dat.mdt, dat.zasl, dat.zone_size);
            }
        }
    }

    return counter;
}

int
ss_open_device(struct ns_entry** ns, const char *name) {
    struct ns_entry			*ns_entry;
    struct ns_data dat = {};
    TAILQ_FOREACH(ns_entry, &g_namespaces, link) {
        if (strncmp(ns_entry->name, name, strlen(name)) == 0) {
            *ns = ns_entry;
            return 0;
        } 
    }
    return -1;
}


int ss_nvme_device_io_with_mdts(struct device_manager *manager, uint64_t slba, uint16_t numbers, void *buffer, uint64_t buf_size,
                                uint64_t lba_size, uint64_t mdts_size, bool read){
    //FIXME:
    return -ENOSYS;
}

static void
read_complete(void *arg, const struct spdk_nvme_cpl *completion)
{
    bool *completed = (bool*)arg;
    *completed = true;
	if (spdk_nvme_cpl_is_error(completion)) {
		fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
		fprintf(stderr, "Read I/O failed, aborting run\n");
		exit(1);
	}
}

int
ss_nvme_device_read(struct ns_entry *entry, uint64_t slba, uint16_t numbers, void *buffer, uint64_t buf_size) {
    ERROR_ON_NULL(entry);
    struct ns_data *info = &entry->data; 
    ERROR_ON_NULL(info);
    uint32_t lba_size_in_use = info->lba_size_in_use;
    struct ns_entry *ns_entry = entry;

    int lbas = ((buf_size+lba_size_in_use-1) / lba_size_in_use);
    size_t sza;
    char* temp_buffer = (char*)spdk_nvme_ctrlr_map_cmb(ns_entry->ctrlr, &sza);
    bool cmbio = true;
    if (temp_buffer == NULL || sza < lbas*lba_size_in_use) {
			cmbio = false;
			temp_buffer = (char*)spdk_zmalloc(lbas*lba_size_in_use, lba_size_in_use, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
	}
    bool completed = false;
	int rc = spdk_nvme_ns_cmd_read(ns_entry->ns, ns_entry->qpair, temp_buffer,
				   slba, /* LBA start */
				   lbas, /* number of LBAs */
				   read_complete, &completed, 0);
    if (rc != 0)
        goto ret;
    while (!completed) {
		spdk_nvme_qpair_process_completions(ns_entry->qpair, 0);
	}
    memcpy(buffer, temp_buffer, buf_size);
    spdk_nvme_ctrlr_unmap_cmb(ns_entry->ctrlr);
    goto ret;
ret:
    spdk_free(temp_buffer);
    return rc;
}

static void
write_complete(void *arg, const struct spdk_nvme_cpl *completion)
{
    bool *completed = (bool*)arg;
    *completed = true;
	if (spdk_nvme_cpl_is_error(completion)) {
		fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
		fprintf(stderr, "Write I/O failed, aborting run\n");
		exit(1);
	}
}

int
ss_nvme_device_write(struct ns_entry *entry, uint64_t slba, uint16_t numbers, void *buffer,
uint64_t buf_size) {
    ERROR_ON_NULL(entry);
    struct ns_data *info = &entry->data; 
    ERROR_ON_NULL(info);
    uint32_t lba_size_in_use = info->lba_size_in_use;
    struct ns_entry *ns_entry = entry;

    int lbas = ((buf_size+lba_size_in_use-1) / lba_size_in_use);
    size_t sza;
    char* temp_buffer = (char*)spdk_nvme_ctrlr_map_cmb(ns_entry->ctrlr, &sza);
    bool cmbio = true;
    if (temp_buffer == NULL || sza < lbas*lba_size_in_use) {
			cmbio = false;
			temp_buffer = (char*)spdk_zmalloc(lbas*lba_size_in_use, lba_size_in_use, NULL, SPDK_ENV_SOCKET_ID_ANY,
                SPDK_MALLOC_DMA);
	}
    memcpy(temp_buffer, buffer, buf_size);
    bool completed = false;
	int rc = spdk_nvme_ns_cmd_write(ns_entry->ns, ns_entry->qpair, temp_buffer,
				   slba, /* LBA start */
				   lbas, /* number of LBAs */
				   write_complete, &completed, 0);
    if (rc != 0)
        return rc;
    while (!completed) {
		spdk_nvme_qpair_process_completions(ns_entry->qpair, 0);
	}
    spdk_nvme_ctrlr_unmap_cmb(ns_entry->ctrlr);
    return rc;
}


// this does not take slba because it will return that
int
ss_zns_device_zone_append(struct ns_entry *entry, uint64_t zslba, int numbers, void *buffer,
uint32_t buf_size, uint64_t *written_slba){
    ERROR_ON_NULL(entry);
    struct ns_data *info = &entry->data; 
    ERROR_ON_NULL(info);

    int lbas = ((buf_size+info->lba_size_in_use-1) / info->lba_size_in_use);
    static int zaddr = 0;
    int i = 0;
    char* temp_buffer;
    int rc;
    for (i=0; i < lbas;) {
        uint8_t step = lbas - i > info->zasl ? info->zasl : lbas - i;
        if (zaddr / info->zone_size < (zaddr+step) / info->zone_size) {
            step = ((zaddr+step) / info->zone_size) * info->zone_size - zaddr;
        }
        size_t sza;
        temp_buffer = (char*)spdk_nvme_ctrlr_map_cmb(entry->ctrlr, &sza);
        bool cmbio = true;
        if (temp_buffer == NULL || sza < step*info->lba_size_in_use) {
                cmbio = false;
                temp_buffer = (char*)spdk_zmalloc(step*info->lba_size_in_use, info->lba_size_in_use, 
                    NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
        }
        memcpy(temp_buffer, buffer+i*info->lba_size_in_use, step*info->lba_size_in_use);
        bool completed = false;
        rc = spdk_nvme_zns_zone_append(entry->ns, entry->qpair, temp_buffer,
                    (zaddr / info->zone_size)*info->zone_size, /* LBA start */
                    step, /* number of LBAs */
                    write_complete, &completed, 0);
        if (rc != 0)
            goto ret;
        while (!completed) {
            spdk_nvme_qpair_process_completions(entry->qpair, 0);
        }
        zaddr += step;
        spdk_nvme_ctrlr_unmap_cmb(entry->ctrlr);
        i += step;
    } 
    goto ret; 
ret:
    spdk_free(temp_buffer);
    return rc;
}

static void
reset_zone_complete(void *arg, const struct spdk_nvme_cpl *completion)
{
    bool *completed = (bool*)arg;
    *completed = true;
    if (spdk_nvme_cpl_is_error(completion)) {
		fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
		fprintf(stderr, "Reset zone I/O failed, aborting run\n");
		exit(1);
	}
}

int
ss_zns_device_zone_reset(struct ns_entry *entry, uint64_t slba) {
    printf("Resetting\n");
    ERROR_ON_NULL(entry);
    bool completed = false;
    int rc = spdk_nvme_zns_reset_zone(entry->ns, entry->qpair,
				     slba, /* starting LBA of the zone to reset */
				     true, /* don't reset all zones */
				     reset_zone_complete,
				     &completed);
    if (rc != 0)
        return rc;
    while (!completed) {
		spdk_nvme_qpair_process_completions(entry->qpair, 0);
	}
    return rc;
}


// see 5.15.2.2 Identify Controller data structure (CNS 01h)
uint64_t 
get_mdts_size(struct ns_entry *entry){
    ERROR_ON_NULL(entry);
    struct ns_data info = entry->data; 
    return info.mdt;
}

int
ss_get_lba_size(struct ns_entry *entry, uint32_t *lba_size) {
    ERROR_ON_NULL(entry);
    struct ns_data info = entry->data;
    *lba_size = info.lba_size_in_use;
    return 0;
}

int
ss_init(struct device_manager **manager) {
    *manager = (struct device_manager*)calloc(1, sizeof(struct device_manager));
    if (*manager == nullptr) {
        return 1;
    }
    struct spdk_env_opts opts;
    opts.name = "m1";

	spdk_env_opts_init(&opts);
    spdk_nvme_trid_populate_transport(&(*manager)->g_trid, SPDK_NVME_TRANSPORT_PCIE);
    if (spdk_env_init(&opts) < 0) {
        free(manager);
		fprintf(stderr, "Unable to initialize SPDK env\n");
		return 1;
	}

    bool identified = identify_devices(*manager);
    if (!identified) {
        return 1;
    }
    return 0;
}

int
ss_cleanup(struct device_manager *manager) {
    struct ns_entry *ns_entry, *ns_entry_temp;
    TAILQ_FOREACH_SAFE(ns_entry, &g_namespaces, link, ns_entry_temp) {
		TAILQ_REMOVE(&g_namespaces, ns_entry, link);
        spdk_nvme_ctrlr_free_io_qpair(ns_entry->qpair);
        spdk_nvme_detach_async(ns_entry->ctrlr, NULL);
		free(ns_entry);
	}
    free(manager);
    return 0;
}
}