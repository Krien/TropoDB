#include "device.h"
#include "utils.h"

namespace ZnsDevice {
extern "C" {
int z_init(DeviceManager **manager, bool reset) {
  *manager = (DeviceManager *)calloc(1, sizeof(DeviceManager));
  RETURN_CODE_ON_NULL(*manager, 1);
  // Setup options
  struct spdk_env_opts opts;
  if (!reset) {
    opts.name = "znsdevice";
    spdk_env_opts_init(&opts);
  }
  // Setup SPDK
  (*manager)->g_trid = {};
  spdk_nvme_trid_populate_transport(&(*manager)->g_trid,
                                    SPDK_NVME_TRANSPORT_PCIE);
  if (spdk_env_init(reset ? NULL : &opts) < 0) {
    free(*manager);
    return 2;
  }
  // setup stub info
  (*manager)->info = {
      .lba_size = 0, .zone_size = 0, .mdts = 0, .zasl = 0, .lba_cap = 0};
  return 0;
}

int z_reinit(DeviceManager **manager) {
  RETURN_CODE_ON_NULL(manager, 1);
  RETURN_CODE_ON_NULL(*manager, 1);
  int rc = z_shutdown(*manager);
  if (rc != 0) {
    return rc;
  }
  *manager = (DeviceManager *)calloc(1, sizeof(DeviceManager));
  // Setup SPDK
  (*manager)->g_trid = {};
  spdk_nvme_trid_populate_transport(&(*manager)->g_trid,
                                    SPDK_NVME_TRANSPORT_PCIE);
  if (spdk_env_init(NULL) < 0) {
    free(*manager);
    return 2;
  }
  // setup stub info
  (*manager)->info = {
      .lba_size = 0, .zone_size = 0, .mdts = 0, .zasl = 0, .lba_cap = 0};
  return 0;
}

int z_shutdown(DeviceManager *manager) {
  RETURN_CODE_ON_NULL(manager, 1);
  int rc = 0;
  if (manager->ctrlr != NULL) {
    rc = z_close(manager) | rc;
  }
  free(manager);
  spdk_env_fini();
  return rc;
}

int z_probe(DeviceManager *manager, ProbeInformation **probe) {
  RETURN_CODE_ON_NULL(manager, 1);
  RETURN_CODE_ON_NULL(probe, 1);
  *probe = (ProbeInformation *)calloc(1, sizeof(ProbeInformation));
  for (int i = 0; i < 256; i++) {
    (*probe)->traddr = (char **)calloc(256, sizeof(char *));
    (*probe)->ctrlr =
        (struct spdk_nvme_ctrlr **)calloc(1, sizeof(spdk_nvme_ctrlr *));
  }
  (*probe)->zns = (bool *)calloc(256, sizeof(bool));
  (*probe)->mut = (pthread_mutex_t *)calloc(1, sizeof(pthread_mutex_t));
  if (pthread_mutex_init((*probe)->mut, NULL) != 0) {
    return 1;
  }
  int rc;
  rc = spdk_nvme_probe(&manager->g_trid, *probe,
                       (spdk_nvme_probe_cb)__probe_devices_probe_cb,
                       (spdk_nvme_attach_cb)__attach_devices__probe_cb, NULL);
  if (rc != 0) {
    spdk_env_fini();
    return 1;
  }
  pthread_mutex_lock((*probe)->mut);
  for (int i = 0; i < (*probe)->devices; i++) {
    rc = spdk_nvme_detach((*probe)->ctrlr[i]) | rc;
  }
  pthread_mutex_unlock((*probe)->mut);
  return rc;
}

int z_open(DeviceManager *manager, const char *traddr) {
  DeviceProber prober = {.manager = manager,
                         .traddr = traddr,
                         .traddr_len = (u_int8_t)strlen(traddr),
                         .found = false};
  // Find and open device
  int probe_ctx;
  probe_ctx = spdk_nvme_probe(&manager->g_trid, &prober,
                              (spdk_nvme_probe_cb)__probe_devices_cb,
                              (spdk_nvme_attach_cb)__attach_devices__cb,
                              (spdk_nvme_remove_cb)__remove_devices__cb);
  if (probe_ctx != 0) {
    spdk_env_fini();
    return 1;
  }
  if (!prober.found) {
    return 2;
  }
  return z_get_device_info(&manager->info, manager);
}

int z_close(DeviceManager *manager) {
  RETURN_CODE_ON_NULL(manager->ctrlr, 1);
  spdk_nvme_detach(manager->ctrlr);
  manager->ctrlr = nullptr;
  manager->ns = nullptr;
  manager->info = {
      .lba_size = 0, .zone_size = 0, .mdts = 0, .zasl = 0, .lba_cap = 0};
  return 0;
}

int z_get_device_info(DeviceInfo *info, DeviceManager *manager) {
  RETURN_CODE_ON_NULL(info, 1);
  RETURN_CODE_ON_NULL(manager, 1);
  RETURN_CODE_ON_NULL(manager->ctrlr, 1);
  RETURN_CODE_ON_NULL(manager->ns, 1);
  const struct spdk_nvme_ns_data *ns_data = spdk_nvme_ns_get_data(manager->ns);
  const struct spdk_nvme_zns_ns_data *ns_data_zns =
      spdk_nvme_zns_ns_get_data(manager->ns);
  const struct spdk_nvme_ctrlr_data *ctrlr_data =
      spdk_nvme_ctrlr_get_data(manager->ctrlr);
  const spdk_nvme_zns_ctrlr_data *ctrlr_data_zns =
      spdk_nvme_zns_ctrlr_get_data(manager->ctrlr);
  union spdk_nvme_cap_register cap =
      spdk_nvme_ctrlr_get_regs_cap(manager->ctrlr);
  info->lba_size = 1 << ns_data->lbaf[ns_data->flbas.format].lbads;
  info->zone_size = ns_data_zns->lbafe[ns_data->flbas.format].zsze;
  info->mdts = (uint64_t)1 << (12 + cap.bits.mpsmin + ctrlr_data->mdts);
  info->zasl = ctrlr_data_zns->zasl;
  info->zasl = info->zasl == 0
                   ? info->mdts
                   : (uint64_t)1 << (12 + cap.bits.mpsmin + info->zasl);
  info->lba_cap = ns_data->ncap;
  return 0;
}

bool __probe_devices_probe_cb(void *cb_ctx,
                              const struct spdk_nvme_transport_id *trid,
                              struct spdk_nvme_ctrlr_opts *opts) {
  (void)cb_ctx;
  (void)trid;
  (void)opts;
  return true;
}

void __attach_devices__probe_cb(void *cb_ctx,
                                const struct spdk_nvme_transport_id *trid,
                                struct spdk_nvme_ctrlr *ctrlr,
                                const struct spdk_nvme_ctrlr_opts *opts) {
  ProbeInformation *prober = (ProbeInformation *)cb_ctx;
  pthread_mutex_lock(prober->mut);
  if (prober->devices >= 255) {
    printf("At the moment no more than 256 devices are supported \n");
  } else {
    prober->traddr[prober->devices] =
        (char *)calloc(strlen(trid->traddr) + 1, sizeof(char));
    strncpy(prober->traddr[prober->devices], trid->traddr,
            strlen(trid->traddr));
    prober->ctrlr[prober->devices] = ctrlr;
    for (int nsid = spdk_nvme_ctrlr_get_first_active_ns(ctrlr); nsid != 0;
         nsid = spdk_nvme_ctrlr_get_next_active_ns(ctrlr, nsid)) {
      struct spdk_nvme_ns *ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
      prober->zns[prober->devices] =
          spdk_nvme_ns_get_csi(ns) == SPDK_NVME_CSI_ZNS;
    }
    prober->devices++;
  }
  pthread_mutex_unlock(prober->mut);
  (void)opts;
}

bool __probe_devices_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
                        struct spdk_nvme_ctrlr_opts *opts) {
  DeviceProber *prober = (DeviceProber *)cb_ctx;
  if (!prober->traddr) {
    return false;
  }
  if (strlen((const char *)trid->traddr) < prober->traddr_len) {
    return false;
  }
  if (strncmp((const char *)trid->traddr, prober->traddr, prober->traddr_len) !=
      0) {
    return false;
  }
  (void)opts;
  return true;
}

void __attach_devices__cb(void *cb_ctx,
                          const struct spdk_nvme_transport_id *trid,
                          struct spdk_nvme_ctrlr *ctrlr,
                          const struct spdk_nvme_ctrlr_opts *opts) {
  DeviceProber *prober = (DeviceProber *)cb_ctx;
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
  (void)trid;
  (void)opts;
  return;
}

void __remove_devices__cb(void *cb_ctx, struct spdk_nvme_ctrlr *ctrlr) {
  (void)cb_ctx;
  (void)ctrlr;
}

int z_create_qpair(DeviceManager *man, QPair **qpair) {
  RETURN_CODE_ON_NULL(man, 1);
  *qpair = (QPair *)calloc(1, sizeof(QPair));
  (*qpair)->qpair = spdk_nvme_ctrlr_alloc_io_qpair(man->ctrlr, NULL, 0);
  (*qpair)->man = man;
  RETURN_CODE_ON_NULL((*qpair)->qpair, 1);
  return 0;
}

int z_destroy_qpair(QPair *qpair) {
  RETURN_CODE_ON_NULL(qpair, 1);
  RETURN_CODE_ON_NULL(qpair->qpair, 1);
  spdk_nvme_ctrlr_free_io_qpair(qpair->qpair);
  qpair->man = NULL;
  free(qpair);
  return 0;
}

void *__reserve_dma(uint64_t size) {
  return (char *)spdk_zmalloc(size, 0, NULL, SPDK_ENV_SOCKET_ID_ANY,
                              SPDK_MALLOC_DMA);
}

void __operation_complete(void *arg, const struct spdk_nvme_cpl *completion) {
  Completion *completed = (Completion *)arg;
  completed->done = true;
  if (spdk_nvme_cpl_is_error(completion)) {
    completed->err = 1;
    return;
  }
}

void __append_complete(void *arg, const struct spdk_nvme_cpl *completion) {
  __operation_complete(arg, completion);
}

void __read_complete(void *arg, const struct spdk_nvme_cpl *completion) {
  __operation_complete(arg, completion);
}

void __reset_zone_complete(void *arg, const struct spdk_nvme_cpl *completion) {
  __operation_complete(arg, completion);
}

void __get_zone_head_complete(void *arg,
                              const struct spdk_nvme_cpl *completion) {
  __operation_complete(arg, completion);
}

int z_read(QPair *qpair, uint64_t slba, void *buffer, uint64_t size) {
  RETURN_CODE_ON_NULL(qpair, 1);
  RETURN_CODE_ON_NULL(buffer, 1);
  DeviceInfo info = qpair->man->info;
  int rc = 0;

  int lbas = (size + info.lba_size - 1) / info.lba_size;
  int lbas_processed = 0;
  int step_size = (info.mdts / info.lba_size);
  int current_step_size = step_size;
  int slba_start = slba;

  while (lbas_processed < lbas) {
    Completion completion = {.done = false, .err = 0};
    if ((slba + lbas_processed + step_size) / info.zone_size >
        (slba + lbas_processed) / info.zone_size) {
      current_step_size =
          ((slba + lbas_processed + step_size) / info.zone_size) *
              info.zone_size -
          lbas_processed - slba;
    } else {
      current_step_size = step_size;
    }
    current_step_size = lbas - lbas_processed > current_step_size
                            ? current_step_size
                            : lbas - lbas_processed;
    // printf("%d step %d  \n", slba_start, current_step_size);
    rc = spdk_nvme_ns_cmd_read(qpair->man->ns, qpair->qpair,
                               (char *)buffer + lbas_processed * info.lba_size,
                               slba_start,        /* LBA start */
                               current_step_size, /* number of LBAs */
                               __read_complete, &completion, 0);
    if (rc != 0) {
      return 1;
    }
    POLL_QPAIR(qpair->qpair, completion.done);
    if (completion.err != 0) {
      return completion.err;
    }
    lbas_processed += current_step_size;
    slba_start = slba + lbas_processed;
  }
  return rc;
}

void *z_calloc(QPair *qpair, int nr, int size) {
  uint32_t alligned_size = qpair->man->info.lba_size;
  uint32_t true_size = nr * size;
  if (true_size % alligned_size != 0) {
    return NULL;
  }
  void *temp_buffer = (char *)spdk_zmalloc(
      true_size, alligned_size, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
  (void)qpair;
  return temp_buffer;
}

void z_free(QPair *qpair, void *buffer) {
  spdk_free(buffer);
  (void)qpair;
  // free(buffer);
}

int z_append(QPair *qpair, uint64_t slba, void *buffer, uint64_t size) {
  RETURN_CODE_ON_NULL(qpair, 1);
  RETURN_CODE_ON_NULL(buffer, 1);

  DeviceInfo info = qpair->man->info;
  int rc = 0;

  int lbas = (size + info.lba_size - 1) / info.lba_size;
  int lbas_processed = 0;
  int step_size = (info.zasl / info.lba_size);
  int current_step_size = step_size;
  int slba_start = (slba / info.zone_size) * info.zone_size;

  while (lbas_processed < lbas) {
    Completion completion = {.done = false, .err = 0};
    if ((slba + lbas_processed + step_size) / info.zone_size >
        (slba + lbas_processed) / info.zone_size) {
      current_step_size =
          ((slba + lbas_processed + step_size) / info.zone_size) *
              info.zone_size -
          lbas_processed - slba;
    } else {
      current_step_size = step_size;
    }
    current_step_size = lbas - lbas_processed > current_step_size
                            ? current_step_size
                            : lbas - lbas_processed;
    rc = spdk_nvme_zns_zone_append(qpair->man->ns, qpair->qpair,
                                   (char *)buffer +
                                       lbas_processed * info.lba_size,
                                   slba_start,        /* LBA start */
                                   current_step_size, /* number of LBAs */
                                   __append_complete, &completion, 0);
    if (rc != 0) {
      break;
    }
    POLL_QPAIR(qpair->qpair, completion.done);
    if (completion.err != 0) {
      return completion.err;
    }
    lbas_processed += current_step_size;
    slba_start = ((slba + lbas_processed) / info.zone_size) * info.zone_size;
  }
  return rc;
}

int z_reset(QPair *qpair, uint64_t slba, bool all) {
  RETURN_CODE_ON_NULL(qpair, 1);
  Completion completion = {.done = false};
  int rc =
      spdk_nvme_zns_reset_zone(qpair->man->ns, qpair->qpair,
                               slba, /* starting LBA of the zone to reset */
                               all,  /* don't reset all zones */
                               __reset_zone_complete, &completion);
  if (rc != 0)
    return rc;
  POLL_QPAIR(qpair->qpair, completion.done);
  return rc;
}

int z_get_zone_head(QPair *qpair, uint64_t slba, uint64_t *head) {
  RETURN_CODE_ON_NULL(qpair, 1);
  RETURN_CODE_ON_NULL(qpair->man, 1);
  size_t report_bufsize = spdk_nvme_ns_get_max_io_xfer_size(qpair->man->ns);
  uint8_t *report_buf = (uint8_t *)calloc(1, report_bufsize);
  Completion completion = {.done = false, .err = 0};
  int rc = spdk_nvme_zns_report_zones(
      qpair->man->ns, qpair->qpair, report_buf, report_bufsize, slba,
      SPDK_NVME_ZRA_LIST_ALL, true, __get_zone_head_complete, &completion);
  if (rc != 0) {
    return rc;
  }
  POLL_QPAIR(qpair->qpair, completion.done);
  if (completion.err != 0) {
    return completion.err;
  }
  uint32_t zd_index = sizeof(struct spdk_nvme_zns_zone_report);
  struct spdk_nvme_zns_zone_desc *desc =
      (struct spdk_nvme_zns_zone_desc *)(report_buf + zd_index);
  *head = desc->wp;
  free(report_buf);
  return rc;
}

/* Function used for validating linking and build issues.*/
int __TEST_interface() {
  printf("TEST TEXT PRINTING\n");
  return 2022;
}
}
} // namespace ZnsDevice
