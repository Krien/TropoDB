#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_CONFIG_H
#define ZNS_CONFIG_H

#include <stddef.h>
#include <stdint.h>

#include <limits>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// Features. In general do not touch. This is for experiments.
#define WAL_BUFFERED   // Enables WAL buffering to be compiled
#define WAL_UNORDERED  // Enables asynchronous I/O. If setting to 0, force
                       // wal_iodepth to 1!
#define DIRECT_COMMIT  // Commits are either done block by block or in ZASL.
#define WAL_MANAGER_MANAGES_CHANNELS  // Do not unset, this will break almost
                                      // WAL features.
//#define USE_COMMITTER // Use ZNScommiter for L0. Legacy. do not touch

// Changing any line here requires rebuilding all ZNS DB source files.
// Reasons for statics is static_asserts and as they can be directly used during
// compilation.
namespace ZnsConfig {
// WAL options
constexpr static uint8_t level_count =
    6; /**< Amount of LSM-tree levels L0 up to LN */
constexpr static size_t manifest_zones =
    4; /**< Amount of zones to reserve for metadata*/
constexpr static size_t zones_foreach_wal =
    4; /**< Amount of zones for each WAL*/
#ifdef WAL_BUFFERED
constexpr bool wal_allow_buffering =
    true; /**< If writes are allowed to be buffered when written to the WAL.
             Increases performance at the cost of persistence.*/
#endif
constexpr static size_t wal_count = 40; /**< Amount of WALs on one zone region*/
constexpr static uint8_t wal_iodepth =
    4; /**< Determines the outstanding queue depth for each WAL. */
constexpr static bool wal_preserve_dma =
    true; /**< Some DMA memory is claimed for WALs, even WALs are not busy.
             Prevents reallocations. */
constexpr static size_t wal_concurrency =
    1; /**< Legacy, do not touch. Determines concurrenct qpairs for WAL which is
          no longer supported*/

// L0 and LN options
constexpr static size_t L0_zones =
    100; /**< amount of zones to reserve for each L0 circular log */
constexpr static uint8_t lower_concurrency  =
    4; /**< Number of L0 circular logs. Increases parallelism. */
constexpr static size_t wal_manager_zone_count = wal_count / lower_concurrency;
constexpr static int L0_slow_down =
    80; /**< Amount of SSTables in L0 before client puts will be stalled. Can
           stabilise latency. Setting this too high can cause some clients to
           wait for minutes during heavy background I/O.*/
static constexpr uint8_t number_of_concurrent_L0_readers =
    4;  // Maximum number of concurrent reader threads reading from L0.
static constexpr uint8_t number_of_concurrent_LN_readers =
    4;  // Maximum number of concurrent reader threads reading from LN.
constexpr static size_t min_ss_zone_count =
    5; /**< Minimum amount of zones for L0 and LN each*/
constexpr static double ss_compact_treshold[level_count]{
    8.,
    16. * 1024. * 1024. * 1024.,
    16. * 4. * 1024. * 1024. * 1024.,
    16. * 16. * 1024. * 1024. * 1024.,
    16. * 64. * 1024. * 1024. * 1024.,
    16. * 256. * 1024. * 1024. * 1024.}; /**< Size of each level before we want
compaction. L0 is in number of SSTables, L1 and up in bytes. */
constexpr static double ss_compact_treshold_force[level_count]{
    0.85, 0.95, 0.95, 0.95,
    0.95, 0.95}; /**< A level can containd live and dead tables and this can be
                    more than the treshold. However, at some point in time it is
                    full and compaction is FORCED. This treshold prevents out of
                    space issues. E.g. L0 may not be filled more than 85%.*/
constexpr static double ss_compact_modifier[level_count]{
    64, 32, 16, 8,
    4, 1}; /**< Modifier for each compaction treshold. When size reaches above
             the compaction treshold, certain compactions might be more
             important than others. For example, setting a high modifier for 2
             causes compactions in 2 to be done first. 0 and 1 are ignored as
             they are handled in a separate thread */
constexpr static uint64_t max_bytes_sstable_l0 =
    1024U * 1024U * 512; /**< Maximum size of SSTables in L0. Determines the
                amount of tables generated on a flush. Rounded to round number
                of lbas by TropoDB. */
constexpr static uint64_t max_bytes_sstable_ = (uint64_t)(
    1073741824. * 2. * 0.95); /**< Maximum size of SSTables in LN. LN tables
                                 reserve entire zones, therefore, please set
                                 to a multitude of approximately n zones */
constexpr static uint64_t max_lbas_compaction_l0 = 2097152 * 12; /**< Maximum
amount of LBAS that can be considered for L0 to LN compaction. Prevents OOM.*/

// Compaction
constexpr static bool compaction_allow_prefetching =
    true; /**< If LN tables can be prefetched during compaction. This uses one
             thread more and some RAM, but can reduce compaction time. */
constexpr static uint8_t compaction_maximum_prefetches =
    6; /**< How many SSTables can be prefetched at most. Be careful, setting
          this too high can cause OOM.*/
constexpr static bool compaction_allow_deferring_writes =
    true; /**< Allows deferring SSTable writes during compaction to a separate
             thread. This can allow compaction to continue without waiting on
             writes to finish.*/
constexpr static uint8_t compaction_maximum_deferred_writes =
    6; /**< How many SSTables can be deferred at most. Be careful, setting
this too high can cause OOM.*/

// Containerisation
constexpr static uint64_t min_zone = 0; /**< Minimum zone to use for database.*/
constexpr static uint64_t max_zone =
    0x0; /**< Maximum zone to use for database. Set to 0 for full region*/

// MISC
constexpr static size_t max_channels =
    0x100; /**< Maximum amount of channels that can be live. Used to ensure
              that there is no channel leak. */
constexpr static bool use_sstable_encoding =
    true; /**< If RLE should be used for SSTables. */
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
static_assert(manifest_zones > 1);
static_assert(zones_foreach_wal > 2);
static_assert(wal_count > 2);
#ifndef WAL_UNORDERED
static_assert(wal_iodepth == 1);
#endif
static_assert(L0_slow_down > 0);
static_assert(number_of_concurrent_L0_readers > 0);
static_assert(number_of_concurrent_LN_readers > 0);
static_assert(min_ss_zone_count > 1);
static_assert(sizeof(ss_compact_treshold) == level_count * sizeof(double));
static_assert(sizeof(ss_compact_treshold_force) ==
              level_count * sizeof(double));
static_assert(sizeof(ss_compact_modifier) == level_count * sizeof(double));
static_assert(((max_zone == min_zone) && min_zone == 0) || min_zone < max_zone);
static_assert(max_zone == 0 || max_zone > manifest_zones +
                                              zones_foreach_wal * wal_count +
                                              min_ss_zone_count * level_count);
static_assert(max_bytes_sstable_l0 > 0);
static_assert(max_bytes_sstable_ > 0);
static_assert(max_lbas_compaction_l0 > 0);
static_assert(
    (!compaction_allow_prefetching && compaction_maximum_prefetches == 0) ||
    (compaction_allow_prefetching && compaction_maximum_prefetches > 0));
static_assert((!compaction_allow_deferring_writes &&
               compaction_maximum_deferred_writes == 0) ||
              (compaction_allow_deferring_writes &&
               compaction_maximum_deferred_writes > 0));
static_assert(max_lbas_compaction_l0 > 0);
static_assert(max_channels > 0);
static_assert(!use_sstable_encoding || max_sstable_encoding > 0);

}  // namespace ZnsConfig
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
