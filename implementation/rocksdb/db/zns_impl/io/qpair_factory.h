#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef QPAIR_FACTORY_H
#define QPAIR_FACTORY_H

#include "db/zns_impl/io/device_wrapper.h"
#include "db/zns_impl/ref_counter.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
// TODO make thread safe (mutexes?)
class QPairFactory : public RefCounter {
 public:
  QPairFactory(ZnsDevice::DeviceManager* device_manager);
  ~QPairFactory();
  // No copying or implicits
  QPairFactory(const QPairFactory&) = delete;
  QPairFactory& operator=(const QPairFactory&) = delete;
  int register_qpair(ZnsDevice::QPair** qpair);
  int unregister_qpair(ZnsDevice::QPair* qpair);

 private:
  uint8_t qpair_count_;
  ZnsDevice::DeviceManager* device_manager_;
};
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
