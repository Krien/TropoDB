// Prevents loading the device dependency multiple times.
// bit like a precompiled header.
#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_DEVICE
#define ZNS_DEVICE
#include <szd/cpp/datastructures/szd_buffer.h>
#include <szd/cpp/szd_channel.h>
#include <szd/cpp/szd_channel_factory.h>
#include <szd/szd.h>

#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
extern Status FromStatus(SZD::SZDStatus s);
extern Status FromStatusMsg(SZD::SZDStatusDetailed s);
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
