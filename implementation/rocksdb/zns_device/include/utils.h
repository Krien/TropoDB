#include "spdk/endian.h"
#include "spdk/env.h"
#include "spdk/log.h"
#include "spdk/nvme.h"
#include "spdk/nvme_intel.h"
#include "spdk/nvme_ocssd.h"
#include "spdk/nvme_zns.h"
#include "spdk/nvmf_spec.h"
#include "spdk/pci_ids.h"
#include "spdk/stdinc.h"
#include "spdk/string.h"
#include "spdk/util.h"
#include "spdk/uuid.h"
#include "spdk/vmd.h"

extern "C" {
#define RETURN_CODE_ON_NULL(x, err)                                            \
  do {                                                                         \
    if ((x) == nullptr) {                                                      \
      return (err);                                                            \
    }                                                                          \
  } while (0)

#define POLL_QPAIR(qpair, target)                                              \
  do {                                                                         \
    while (!(target)) {                                                        \
      spdk_nvme_qpair_process_completions((qpair), 0);                         \
    }                                                                          \
  } while (0)
}
