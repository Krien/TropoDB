#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_CONFIG_H
#define ZNS_CONFIG_H

#include <stddef.h>
#include <stdint.h>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
// Changing any line here requires rebuilding all ZNS source files.
namespace ZnsConfig {
const static size_t level_count =
    7; /**< Amount of LSM-tree levels L0 up to LN */
const static size_t manifest_zones =
    2; /**< Amount of zones to reserve for metadata*/
const static size_t zones_foreach_wal = 3; /**< Amount of zones for each WAL*/
const static size_t wal_count = 5; /**< Maximum amount of concurrent WALS*/
const static size_t ss_distribution[level_count] = {
    1, 2,  3, 5,
    8, 13, 21}; /**< each level i gets \f$\frac{Xi}{\sum_{i=0}^{N}
                   x}\f$  of the remaining zones*/
const static size_t min_ss_zone_count =
    5; /**< Minimum amount of zones for each LSM-tree level*/
const static double ss_compact_treshold[level_count]{
    0.75, 0.85, 0.85, 0.85,
    0.85, 0.85, 0.85}; /**< Fraction of lbas that need to be filled to trigger
                          compaction for a level.*/
const static uint64_t min_zone = 0;   /**< Minimum zone to use for database.*/
const static uint64_t max_zone = 100; /**< Maximum zone to use for database*/

// Configs are asking for trouble... As they say in security, never trust user
// input! Even/especially your own.
static_assert(level_count > 1);
static_assert(sizeof(ss_distribution) == level_count * sizeof(size_t));
static_assert(sizeof(ss_compact_treshold) == level_count * sizeof(double));
static_assert(min_ss_zone_count > 1);
static_assert(min_zone < max_zone);
static_assert(max_zone > manifest_zones + zones_foreach_wal * wal_count +
                             min_ss_zone_count * level_count);

}  // namespace ZnsConfig
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
