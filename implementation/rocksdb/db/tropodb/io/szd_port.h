// Prevents loading the device dependency multiple times.
// bit like a precompiled header.
#pragma once
#ifdef TROPODB_PLUGIN_ENABLED
#ifndef SZD_PORT_H
#define SZD_PORT_H
#include <szd/datastructures/szd_buffer.hpp>
#include <szd/datastructures/szd_circular_log.hpp>
#include <szd/datastructures/szd_fragmented_log.hpp>
#include <szd/datastructures/szd_log.hpp>
#include <szd/datastructures/szd_once_log.hpp>
#include <szd/szd_channel.hpp>
#include <szd/szd_channel_factory.hpp>
#include <szd/szd_device.hpp>

#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
extern Status FromStatus(SZD::SZDStatus s);
extern Status FromStatusMsg(SZD::SZDStatusDetailed s);
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
