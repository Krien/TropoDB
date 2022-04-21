#include "db/zns_impl/io/qpair_factory.h"

#include "db/zns_impl/io/device_wrapper.h"

namespace ROCKSDB_NAMESPACE {
QPairFactory::QPairFactory(ZnsDevice::DeviceManager* device_manager)
    : qpair_count_(0), device_manager_(device_manager) {}
QPairFactory::~QPairFactory() {
  //printf("Deleting QPairFactory.\n");
  assert(qpair_count_ == 0);
}

int QPairFactory::register_qpair(ZnsDevice::QPair** qpair) {
  int rc = ZnsDevice::z_create_qpair(device_manager_, qpair);
  if (rc != 0) {
    qpair_count_++;
  }
  return rc;
}

int QPairFactory::unregister_qpair(ZnsDevice::QPair* qpair) {
  int rc = ZnsDevice::z_destroy_qpair(qpair);
  qpair_count_--;
  return rc;
}
}  // namespace ROCKSDB_NAMESPACE
