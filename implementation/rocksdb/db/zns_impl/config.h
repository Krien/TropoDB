#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_CONFIG_H
#define ZNS_CONFIG_H

#include <stddef.h>
#include <stdint.h>

#include <limits>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
// Changing any line here requires rebuilding all ZNS DB source files.
// Reasons for statics is static_asserts and as they can be directly used during
// compilation.
namespace ZnsConfig {
constexpr static uint8_t level_count =
    4; /**< Amount of LSM-tree levels L0 up to LN */
constexpr static size_t manifest_zones =
    2; /**< Amount of zones to reserve for metadata*/
constexpr static size_t zones_foreach_wal =
    3;                                 /**< Amount of zones for each WAL*/
constexpr static size_t wal_count = 5; /**< Maximum amount of concurrent WALS*/
constexpr static size_t ss_distribution[level_count] = {
    1, 2, 3, 5}; /**< each level i gets \f$\frac{Xi}{\sum_{i=0}^{N}
                  x}\f$  of the remaining zones*/
constexpr static size_t min_ss_zone_count =
    5; /**< Minimum amount of zones for each LSM-tree level*/
constexpr static double ss_compact_treshold[level_count]{
    0.50, 0.45, 0.45, 0.85}; /**< Fraction of lbas that need to be filled to
                          trigger compaction for a level.*/
constexpr static uint64_t max_bytes_sstable_ =
    16 * 1024;  // Based on LevelDBs max_file_size. Be carefull, this will
                // scale up depending on lba_size!
constexpr static uint64_t min_zone = 0; /**< Minimum zone to use for database.*/
constexpr static uint64_t max_zone =
    100; /**< Maximum zone to use for database*/

// Configs are asking for trouble... As they say in security, never trust user
// input! Even/especially your own.
static_assert(level_count > 1 &&
              level_count <
                  std::numeric_limits<uint8_t>::max() -
                      1);  // max - 1 because we sometimes poll at next level.
                           // We do not want numeric overflows...
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
