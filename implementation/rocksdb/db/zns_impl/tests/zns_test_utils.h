#include "db/zns_impl/table/zns_sstable_manager.h"
#include "test_util/testharness.h"
#include "db/zns_impl/tropodb_config.h"

namespace ROCKSDB_NAMESPACE {
struct Device {
  ZnsDevice::DeviceManager** device_manager = nullptr;
  ZnsDevice::QPair** qpair = nullptr;
  QPairFactory* qpair_factory = nullptr;
  ZNSSSTableManager* ss_manager = nullptr;
};

static void SetupDev(Device* device, int first_zone, int last_zone,
                     bool reinit) {
  device->device_manager = new ZnsDevice::DeviceManager*;
  int rc = reinit ? ZnsDevice::z_reinit(device->device_manager)
                  : ZnsDevice::z_init(device->device_manager);
  ASSERT_EQ(rc, 0);
  rc = ZnsDevice::z_open(*(device->device_manager), "0000:00:04.0");
  ASSERT_EQ(rc, 0);
  device->qpair = (ZnsDevice::QPair**)calloc(1, sizeof(ZnsDevice::QPair*));
  rc = ZnsDevice::z_create_qpair(*(device->device_manager), (device->qpair));
  ASSERT_EQ(rc, 0);
  rc = ZnsDevice::z_reset(*device->qpair, 0, true);
  ASSERT_EQ(rc, 0);
  device->qpair_factory = new QPairFactory(*(device->device_manager));
  device->qpair_factory->Ref();
  ASSERT_EQ(device->qpair_factory->Getref(), 1);
  uint64_t zsize = (*device->device_manager)->info.zone_size;
  std::pair<uint64_t, uint64_t> ranges[ZnsConfig::level_count] = {
      std::make_pair(zsize * first_zone, zsize * last_zone),
      std::make_pair(zsize * last_zone, zsize * (last_zone + 5)),
      std::make_pair(zsize * (last_zone + 5), zsize * (last_zone + 10)),
      std::make_pair(zsize * (last_zone + 10), zsize * (last_zone + 15)),
      std::make_pair(zsize * (last_zone + 15), zsize * (last_zone + 20)),
      std::make_pair(zsize * (last_zone + 20), zsize * (last_zone + 25)),
      std::make_pair(zsize * (last_zone + 25), zsize * (last_zone + 30))};
  device->ss_manager = new ZNSSSTableManager(
      device->qpair_factory, (*device->device_manager)->info, ranges);
  ASSERT_EQ(device->qpair_factory->Getref(), 2 + 7);
  device->ss_manager->Ref();
  ASSERT_EQ(device->ss_manager->Getref(), 1);
}

static void TearDownDev(Device* device) {
  ASSERT_EQ(device->ss_manager->Getref(), 1);
  device->ss_manager->Unref();
  ASSERT_EQ(device->qpair_factory->Getref(), 1);
  device->qpair_factory->Unref();
  int rc = ZnsDevice::z_destroy_qpair(*device->qpair);
  ASSERT_EQ(rc, 0);
  rc = ZnsDevice::z_shutdown(*device->device_manager);
  if (device->qpair != nullptr) delete device->qpair;
}

static void ValidateMeta(Device* device, int first_zone, int last_zone) {
  ZnsDevice::DeviceInfo info = (*device->device_manager)->info;
  L0ZnsSSTable* sstable = device->ss_manager->GetL0SSTableLog();
  ASSERT_EQ(ZnsSSTableManagerInternal::GetMinZoneHead(sstable),
            info.zone_size * first_zone);
  ASSERT_EQ(ZnsSSTableManagerInternal::GetMaxZoneHead(sstable),
            info.zone_size * last_zone);
  ASSERT_EQ(ZnsSSTableManagerInternal::GetZoneSize(sstable), info.zone_size);
  ASSERT_EQ(ZnsSSTableManagerInternal::GetLbaSize(sstable), info.lba_size);
}

}  // namespace ROCKSDB_NAMESPACE
