#include "device.h"
#include "utils.h"
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

extern "C" {

int min_zns(int l1, int l2) { return l1 < l2 ? l1 : l2; }

int print_help_util() {
  fprintf(
      stdout,
      "znscli [options]\n"
      "options:\n"
      " probe   get trid from all devices and ZNS indicators\n"
      " info    get device information (sizes etc.)\n"
      "   -t <trid>   REQUIRED - ZNS trid of the device to request information "
      "from\n"
      " zones   get write heads from each zone and print them\n"
      "   -t <trid>   REQUIRED - ZNS trid of the device to request "
      "zoneinformation from\n"
      " reset   reset zones on a ZNS device\n"
      "   -t <trid>   REQUIRED - ZNS trid of the device to request information "
      "from\n"
      "   -l <slba>   REQUIRED - slba of zone to reset\n"
      "   -a          OPTIONAL - reset the whole device instead of one zone\n"
      " write   append data to zones on a ZNS device\n"
      "   -t <trid>   REQUIRED - ZNS trid of the device to request information "
      "from\n"
      "   -l <slba>   REQUIRED - slba of zone to append to\n"
      "   -s <size>   REQUIRED - bytes to write in multiple of lba_size\n"
      "   -d <data>   REQUIRED - data to write to the device\n"
      " read    read bytes from a ZNS device\n"
      "   -t <trid>   REQUIRED - ZNS trid of the device to request information "
      "from\n"
      "   -l <slba>   REQUIRED - slba of zone to read from (does not need to "
      "be alligned to a zone)\n"
      "   -s <size>   REQUIRED - bytes to read in multiple of lba_size\n"
      "DISCLAIMER:\n"
      " This tool is not tested for security concerns (buffer overflows etc.), "
      "use at own risk!\n"
      " This tool is meant to debug ZNS device, not for actual production "
      "use.\n"
      " The tool will also only work properly with NVMe ZNS devices only\n");
  return 0;
}

int parse_reset_zns(int argc, char **argv, ZnsDevice::DeviceManager **manager) {
  int op;
  char *trid = (char *)calloc(0x100, sizeof(char *));
  bool trid_set = false;
  bool reset_all = false;
  int64_t slba = -1;
  while ((op = getopt(argc, argv, "t:l:a::")) != -1) {
    switch (op) {
    case 'l':
      slba = (int64_t)spdk_strtol(optarg, 10);
      if (slba < 0) {
        fprintf(stderr, "Invalid slba\n");
        return slba;
      }
      break;
    case 't':
      trid_set = true;
      snprintf(trid, 0x100, "%s", optarg);
      break;
    case 'a':
      reset_all = true;
      break;
    default:
      return 1;
    }
  }
  if (!trid_set || slba < 0) {
    fprintf(stderr, "Not all required arguments are set for reset\n");
    print_help_util();
    return 1;
  }
  int rc = ZnsDevice::z_open(*manager, trid);
  if (rc != 0) {
    fprintf(stderr, "Invalid trid %s\n", trid);
    print_help_util();
    return 1;
  }
  ZnsDevice::DeviceInfo info = (*manager)->info;
  if (info.lba_cap < (uint64_t)slba || (uint64_t)slba % info.zone_size != 0) {
    fprintf(stderr, "Invalid slba \n");
    return 1;
  }
  ZnsDevice::QPair **qpair =
      (ZnsDevice::QPair **)calloc(1, sizeof(ZnsDevice::QPair *));
  rc = ZnsDevice::z_create_qpair(*manager, qpair);
  if (rc != 0) {
    free(qpair);
    ZnsDevice::z_shutdown(*manager);
    return rc;
  }
  if (reset_all) {
    fprintf(stdout, "Resetting complete device %s \n", trid);
  } else {
    fprintf(stdout, "Resetting device %s at %lu \n", trid, (uint64_t)slba);
  }
  rc = ZnsDevice::z_reset(*qpair, (uint64_t)slba, reset_all);
  free(qpair);
  ZnsDevice::z_shutdown(*manager);
  return rc;
}

int parse_read_zns(int argc, char **argv, ZnsDevice::DeviceManager **manager) {
  int op;
  char *trid = (char *)calloc(0x100, sizeof(char *));
  bool trid_set = false;
  bool reset_all = false;
  int64_t lba = -1;
  int64_t size = -1;
  while ((op = getopt(argc, argv, "t:l:s:")) != -1) {
    switch (op) {
    case 'l':
      lba = (int64_t)spdk_strtol(optarg, 10);
      if (lba < 0) {
        fprintf(stderr, "Invalid lba %s\n", optarg);
        print_help_util();
        return lba;
      }
      break;
    case 't':
      trid_set = true;
      snprintf(trid, 0x100, "%s", optarg);
      break;
    case 's':
      size = (int64_t)spdk_strtol(optarg, 10);
      if (size < 0) {
        fprintf(stderr, "Invalid size %s\n", optarg);
        print_help_util();
        return size;
      }
      break;
    default:
      return 1;
    }
  }
  if (!trid_set || lba < 0 || size < 0) {
    fprintf(stderr, "Not all required arguments are set for a read\n");
    print_help_util();
    return 1;
  }
  int rc = ZnsDevice::z_open(*manager, trid);
  if (rc != 0) {
    fprintf(stderr, "Invalid trid %s\n", trid);
    print_help_util();
    return 1;
  }
  ZnsDevice::DeviceInfo info = (*manager)->info;
  if (info.lba_cap < (uint64_t)lba || (uint64_t)size % info.lba_size != 0) {
    fprintf(stderr,
            "Invalid slba or size \n"
            " requested lba:%lu <-CHECK-> lba capacity: %lu\n"
            " requested size:%lu <-CHECK-> lba size %lu",
            (uint64_t)lba, info.lba_cap, (uint64_t)size, info.lba_size);
    return 1;
  }
  size = min_zns(size, (info.lba_cap - lba) * info.lba_size);
  ZnsDevice::QPair **qpair =
      (ZnsDevice::QPair **)calloc(1, sizeof(ZnsDevice::QPair *));
  rc = ZnsDevice::z_create_qpair(*manager, qpair);
  if (rc != 0) {
    fprintf(stderr, "Error creating qpair\n");
    free(qpair);
    ZnsDevice::z_shutdown(*manager);
    return rc;
  }
  char *data = (char *)ZnsDevice::z_calloc(*qpair, size, sizeof(char *));
  if (!data) {
    fprintf(stderr, "Error allocating with SPDKs malloc\n");
    free(qpair);
    ZnsDevice::z_shutdown(*manager);
    return rc;
  }
  rc = ZnsDevice::z_read(*qpair, lba, data, size);
  if (rc != 0) {
    fprintf(stderr, "Error reading %d\n", rc);
    free(qpair);
    rc = ZnsDevice::z_shutdown(*manager);
    return rc;
  }
  fprintf(stdout, "Read data from device %s, location %lu, size %lu:\n", trid,
          lba, size);
  // prevent stopping on a \0.
  for (int i = 0; i < size; i++) {
    fprintf(stdout, "%c", data[i]);
  }
  fprintf(stdout, "\n");
  free(qpair);
  rc = ZnsDevice::z_shutdown(*manager);
  return rc;
}

int parse_write_zns(int argc, char **argv, ZnsDevice::DeviceManager **manager) {
  int op;
  char *trid = (char *)calloc(0x100, sizeof(char));
  bool trid_set = false;
  bool reset_all = false;
  int64_t lba = -1;
  int64_t size = -1;
  int64_t data_size = -1;
  char *data;
  while ((op = getopt(argc, argv, "t:l:s:d:")) != -1) {
    switch (op) {
    case 'l':
      lba = (int64_t)spdk_strtol(optarg, 10);
      if (lba < 0) {
        fprintf(stderr, "Invalid lba %s\n", optarg);
        print_help_util();
        return lba;
      }
      break;
    case 't':
      trid_set = true;
      snprintf(trid, 0x100, "%s", optarg);
      break;
    case 's':
      size = (int64_t)spdk_strtol(optarg, 10);
      if (size < 0) {
        fprintf(stderr, "Invalid size %s\n", optarg);
        print_help_util();
        return size;
      }
      break;
    case 'd':
      data_size = strlen(optarg) + 1;
      data = (char *)calloc(data_size, sizeof(char));
      snprintf(data, data_size, "%s", optarg);
      break;
    default:
      return 1;
    }
  }
  if (!trid_set || lba < 0 || size < 0 || !data) {
    fprintf(stderr, "Not all required arguments are set for a write\n");
    print_help_util();
    return 1;
  }
  int rc = ZnsDevice::z_open(*manager, trid);
  if (rc != 0) {
    fprintf(stderr, "Invalid trid %s\n", trid);
    print_help_util();
    free(data);
    return 1;
  }
  ZnsDevice::DeviceInfo info = (*manager)->info;
  if (info.lba_cap < (uint64_t)lba || (uint64_t)size % info.lba_size != 0 ||
      (uint64_t)lba % info.zone_size != 0) {
    fprintf(stderr,
            "Invalid slba or size \n"
            " requested lba:%lu <-CHECK-> lba capacity: %lu, zone size: %lu\n"
            " requested size:%lu <-CHECK-> lba size %lu",
            (uint64_t)lba, info.lba_cap, info.zone_size, (uint64_t)size,
            info.lba_size);
    print_help_util();
    free(data);
    return 1;
  }
  size = min_zns(size, (info.lba_cap - lba) * info.lba_size);
  ZnsDevice::QPair **qpair =
      (ZnsDevice::QPair **)calloc(1, sizeof(ZnsDevice::QPair *));
  rc = ZnsDevice::z_create_qpair(*manager, qpair);
  if (rc != 0) {
    fprintf(stderr, "Error creating qpair %d\n", rc);
    free(qpair);
    ZnsDevice::z_shutdown(*manager);
    free(data);
    return rc;
  }
  char *data_spdk = (char *)ZnsDevice::z_calloc(*qpair, size, sizeof(char *));
  if (!data_spdk) {
    fprintf(stderr, "Error allocating with SPDKs malloc\n");
    free(qpair);
    ZnsDevice::z_shutdown(*manager);
    free(data);
    return rc;
  }
  snprintf(data_spdk, min_zns(data_size, size), "%s", data);
  rc = ZnsDevice::z_append(*qpair, lba, data_spdk, size);
  if (rc != 0) {
    fprintf(stderr, "Error appending %d\n", rc);
    free(qpair);
    ZnsDevice::z_shutdown(*manager);
    free(data);
    return rc;
  }
  fprintf(stdout, "write data to device %s, location %lu, size %lu\n", trid,
          lba, size);
  free(qpair);
  rc = ZnsDevice::z_shutdown(*manager);
  free(data);
  return rc;
}

int parse_probe_zns(int argc, char **argv, ZnsDevice::DeviceManager **manager) {
  ZnsDevice::ProbeInformation **prober = (ZnsDevice::ProbeInformation **)calloc(
      1, sizeof(ZnsDevice::ProbeInformation *));
  printf("Looking for devices:\n");
  int rc = ZnsDevice::z_probe(*manager, prober);
  if (rc != 0) {
    printf("Fatal error during probing %d\n Are you sure you are running as "
           "root?\n",
           rc);
    free(prober);
    return 1;
  }
  for (int i = 0; i < (*prober)->devices; i++) {
    const char *is_zns = (*prober)->zns[i] ? "true" : "false";
    printf("Device found\n\tname:%s\n\tZNS device:%s\n", (*prober)->traddr[i],
           is_zns);
    free((*prober)->traddr[i]);
  }
  (*prober)->devices = 0;
  // dangerous! we must be absolutely sure that no other process is using this
  // anymore.
  free((*prober)->mut);
  free((*prober)->ctrlr);
  free((*prober)->zns);
  free(*prober);
  free(prober);
  return 0;
}

int parse_info_zns(int argc, char **argv, ZnsDevice::DeviceManager **manager) {
  int op;
  char *trid = (char *)calloc(0x100, sizeof(char));
  bool trid_set = false;
  while ((op = getopt(argc, argv, "t:")) != -1) {
    switch (op) {
    case 't':
      trid_set = true;
      snprintf(trid, 0x100, "%s", optarg);
      break;
    default:
      return 1;
    }
  }
  if (!trid_set) {
    return 1;
  }
  int rc = ZnsDevice::z_open(*manager, trid);
  if (rc != 0) {
    fprintf(stderr, "Invalid trid %s\n", trid);
    return 1;
  }
  ZnsDevice::DeviceInfo info = (*manager)->info;
  fprintf(stdout,
          "Getting information from device %s:\n"
          "\t*lba size (in bytes)    :%lu\n"
          "\t*zone capacity (in lbas):%lu\n"
          "\t*amount of zones        :%lu\n"
          "\t*total amount of lbas   :%lu\n"
          "\t*mdts (in bytes)        :%lu\n"
          "\t*zasl (in bytes)        :%lu\n",
          trid, info.lba_size, info.zone_size, info.lba_cap / info.zone_size,
          info.lba_cap, info.mdts, info.zasl);
  return rc;
}

int parse_zones_zns(int argc, char **argv, ZnsDevice::DeviceManager **manager) {
  int op;
  char *trid = (char *)calloc(0x100, sizeof(char));
  bool trid_set = false;
  while ((op = getopt(argc, argv, "t:")) != -1) {
    switch (op) {
    case 't':
      trid_set = true;
      snprintf(trid, 0x100, "%s", optarg);
      break;
    default:
      return 1;
    }
  }
  if (!trid_set) {
    return 1;
  }
  int rc = ZnsDevice::z_open(*manager, trid);
  if (rc != 0) {
    fprintf(stderr, "Invalid trid %s\n", trid);
    print_help_util();
    return 1;
  }
  ZnsDevice::QPair **qpair =
      (ZnsDevice::QPair **)calloc(1, sizeof(ZnsDevice::QPair *));
  rc = ZnsDevice::z_create_qpair(*manager, qpair);
  if (rc != 0) {
    fprintf(stderr, "Error allocating with SPDKs malloc %d\n", rc);
    free(qpair);
    ZnsDevice::z_shutdown(*manager);
    return rc;
  }
  ZnsDevice::DeviceInfo info = (*manager)->info;
  printf("Printing zone writeheads for device %s:\n", trid);
  uint64_t zone_head;
  for (uint64_t i = 0; i < info.lba_cap; i += info.zone_size) {
    rc = ZnsDevice::z_get_zone_head(*qpair, i, &zone_head);
    if (rc != 0) {
      fprintf(stderr, "Error during getting zonehead %d\n", rc);
      return 1;
    }
    printf("\tslba:%6lu - wp:%6lu - %lu/%lu\n", i, zone_head, zone_head - i,
           info.zone_size);
  }
  return rc;
}

int parse_help_zns(int argc, char **argv, ZnsDevice::DeviceManager **manager) {
  return print_help_util();
}

int parse_args_zns(int argc, char **argv, ZnsDevice::DeviceManager **manager) {
  if (!argc) {
    return 1;
  }
// checks if first word matches a command, if it is try to parse the rest and
// return the rc.
#define TRY_PARSE_ARGS_ZNS_COMMAND(FUNCTIONNAME, argc, argv, manager)          \
  do {                                                                         \
    if (strncmp((argv)[0], #FUNCTIONNAME,                                      \
                min_zns(strlen(argv[0]), strlen(#FUNCTIONNAME))) == 0) {       \
      return parse_##FUNCTIONNAME##_zns(argc, argv, manager);                  \
    }                                                                          \
  } while (0)
  TRY_PARSE_ARGS_ZNS_COMMAND(reset, argc, argv, manager);
  TRY_PARSE_ARGS_ZNS_COMMAND(read, argc, argv, manager);
  TRY_PARSE_ARGS_ZNS_COMMAND(write, argc, argv, manager);
  TRY_PARSE_ARGS_ZNS_COMMAND(info, argc, argv, manager);
  TRY_PARSE_ARGS_ZNS_COMMAND(probe, argc, argv, manager);
  TRY_PARSE_ARGS_ZNS_COMMAND(zones, argc, argv, manager);
  TRY_PARSE_ARGS_ZNS_COMMAND(help, argc, argv, manager);
  fprintf(stderr, "Command not recognised...\n");
  return print_help_util();
}

int main(int argc, char **argv) {
  int rc;
  if (argc < 2) {
    printf("Not enough args provided\n");
    print_help_util();
    return 1;
  }

  ZnsDevice::DeviceManager **manager =
      (ZnsDevice::DeviceManager **)calloc(1, sizeof(ZnsDevice::DeviceManager));
  rc = ZnsDevice::z_init(manager);
  if (rc != 0) {
    return rc;
  }

  rc = parse_args_zns(argc - 1, &argv[1], manager);
  if (rc != 0) {
    printf("error during operation \n");
    free(manager);
    return rc;
  }
  free(manager);
  return rc;
}
}
