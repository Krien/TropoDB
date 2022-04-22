#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_CONFIG_H
#define ZNS_CONFIG_H

#include <stddef.h>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
// Changing any line here requires rebuilding all ZNS source files.
namespace ZnsConfig {
const static size_t level_count = 7;
static_assert(level_count > 1);
const static size_t manifest_zones = 2;
const static size_t zones_foreach_wal = 3;
const static size_t wal_count = 5;
const static size_t ss_distribution[level_count] = {1, 2, 3, 5, 8, 13, 21};
static_assert(sizeof(ss_distribution) == level_count * sizeof(size_t));
const static size_t min_ss_zone_count = 5;
static_assert(min_ss_zone_count > 1);
const static double ss_compact_treshold[level_count]{0.75, 0.85, 0.85, 0.85,
                                                     0.85, 0.85, 0.85};
static_assert(sizeof(ss_compact_treshold) == level_count * sizeof(double));
}  // namespace ZnsConfig
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
