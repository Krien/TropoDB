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


#ifndef STOSYS_PROJECT_DEVICE_H
#define STOSYS_PROJECT_DEVICE_H

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

extern "C" {
// we will use an ss_ extension to differentiate our struct definitions from the standard library
// In C++ we should use namespaces, but I am lazy

#define ERROR_ON_NULL(x) do {if((x) == nullptr) { \
    return 1; \
}} while(0)


struct device_manager {
    struct spdk_nvme_transport_id g_trid = {};
};

static TAILQ_HEAD(, ns_entry) g_namespaces = TAILQ_HEAD_INITIALIZER(g_namespaces);


struct ns_data {
    u_int32_t lba_size_in_use;
    u_int64_t mdt; 
    bool is_zns; 
    u_int64_t zasl;
    u_int64_t zone_size;
};

struct ns_entry {
	struct spdk_nvme_ctrlr	*ctrlr;
	struct spdk_nvme_ns	*ns;
    struct spdk_nvme_qpair	*qpair;
    struct spdk_nvme_transport_id *trid;
    struct ns_data data;
    char name[1024];
	TAILQ_ENTRY(ns_entry)	link;
};

struct ss_nvme_ns {
    char *ctrl_name;
    bool supports_zns;
    uint32_t nsid;
};

struct zone_to_test {
    uint64_t lba_size_in_use;
};


int
ss_init(struct device_manager **manager);

// these three function examples are given to you
int
count_and_show_all_nvme_devices(struct device_manager *manager);
int 
scan_and_identify_zns_devices(struct ss_nvme_ns *list);
int 
show_zns_zone_status(struct device_manager *manager, struct zone_to_test *ztest);

// Helper functions written by me
uint64_t 
ss_nmve_device_get_alligned_buf_size(struct device_manager *manager, uint16_t numbers,
     uint64_t buf_size); 
unsigned int
nvme_get_lba_size(int fd, uint32_t nsid);

// these follow nvme specification I added ss_ prefix to avoid namespace collision with other lbnvme functions
int 
ss_nvme_device_io_with_mdts(struct device_manager *manager, uint64_t slba,
                                uint16_t numbers, void *buffer, uint64_t buf_size,
                                uint64_t lba_size, uint64_t mdts_size, bool read);
int 
ss_nvme_device_read(struct ns_entry *entry, uint64_t slba, 
                                uint16_t numbers, void *buffer, uint64_t buf_size);
int 
ss_nvme_device_write(struct ns_entry *entry, uint64_t slba, 
                                uint16_t numbers, void *buffer, uint64_t buf_size);

// these are ZNS specific commands
int 
ss_zns_device_zone_reset(struct ns_entry *entry, uint64_t slba);
int 
ss_zns_device_zone_append(struct ns_entry *entry, uint64_t zslba, 
                                int numbers, void *buffer, uint32_t buf_size,
                                uint64_t *written_slba);

int
ss_open_device(struct ns_entry** ns, const char *name);

int
ss_get_lba_size(struct ns_entry* entry, uint32_t *lba_size);

void 
update_lba(uint64_t &write_lba, const uint32_t lba_size, const int count);
uint64_t
get_mdts_size(struct ns_entry *entry);

int
ss_cleanup(struct device_manager *manager);

}

#endif //STOSYS_PROJECT_DEVICE_H

