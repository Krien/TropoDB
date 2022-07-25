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
    6; /**< Amount of LSM-tree levels L0 up to LN */
constexpr static size_t manifest_zones =
    4; /**< Amount of zones to reserve for metadata*/
constexpr static size_t zones_foreach_wal =
    4;                                 /**< Amount of zones for each WAL*/
constexpr bool use_write_buffering = true;
constexpr static size_t wal_count = 40; /**< Maximum amount of concurrent WALS*/
constexpr static size_t wal_concurrency =
    1; /**< Maximum number of concurrent WAL writerts */
constexpr static size_t ss_distribution[level_count] = {
    1, 2, 3, 5,1,1}; /**< each level i gets \f$\frac{Xi}{\sum_{i=0}^{N}
                  x}\f$  of the remaining zones*/
constexpr static size_t min_ss_zone_count =
    5; /**< Minimum amount of zones for each LSM-tree level*/
constexpr static int L0_slow_down = 60;
constexpr static double ss_compact_treshold[level_count]{
    4, 8.*1024.*1024.*1024., 32.*1024.*1024.*1024., 128.*1024.*1024.*1024., 512.*1024.*1024.*1024., 2048.*1024.*1024.*1024.}; /**< Number of SSTables before we want compaction
                              (hint can be preempted). */
constexpr static double ss_compact_treshold_force[level_count]{
    0.85, 0.55, 0.55, 0.55, 0.55, 0.55}; /**< Fraction of lbas that might require
                                compaction to prevent out of space. HIGHER
                                prio than ss_compact_treshold*/
constexpr static uint64_t max_bytes_sstable_ =
 (uint64_t)(2097152. * 2. *
               0.95);  // please set to a multitude of approximately n zones
constexpr static uint64_t min_zone = 0; /**< Minimum zone to use for database.*/
constexpr static uint64_t max_zone =
    0x0; /**< Maximum zone to use for database. Set to 0 for full region*/
constexpr static size_t max_channels =
    0x100; /**< Used to ensure that there is no channel leak. */

constexpr static bool use_sstable_encoding =
    true; /**< If rle should be used for SSTables. */
constexpr static uint32_t max_sstable_encoding = 16; /**< RLE max size. */
constexpr static const char* deadbeef =
    "\xaf\xeb\xad\xde"; /**< Used for placeholder strings*/

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
static_assert(((max_zone == min_zone) && min_zone == 0) || min_zone < max_zone);
static_assert(max_zone == 0 || max_zone > manifest_zones +
                                              zones_foreach_wal * wal_count +
                                              min_ss_zone_count * level_count);
static_assert(!use_sstable_encoding || max_sstable_encoding > 0);

}  // namespace ZnsConfig
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
