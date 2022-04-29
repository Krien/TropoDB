// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread-safe (provides internal synchronization)

#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_SSTABLE_CACHE_H
#define ZNS_SSTABLE_CACHE_H

#include <memory>

#include "db/dbformat.h"
#include "db/zns_impl/table/zns_sstable.h"
#include "db/zns_impl/table/zns_sstable_manager.h"
#include "rocksdb/cache.h"
#include "rocksdb/iterator.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
class ZnsTableCache {
 public:
  ZnsTableCache(const Options& options, const InternalKeyComparator& icmp,
                const size_t entries, ZNSSSTableManager* ssmanager);
  ZnsTableCache(const ZnsTableCache&) = delete;
  ZnsTableCache& operator=(const ZnsTableCache&) = delete;
  ~ZnsTableCache();

  Iterator* NewIterator(const ReadOptions& options, const SSZoneMetaData& meta,
                        const size_t level, ZnsSSTable** tableptr = nullptr);

  Status Get(const ReadOptions& options, const SSZoneMetaData& meta,
             size_t level, const Slice& key, std::string* value,
             EntryStatus* status);

  void Evict(const uint64_t ss_number);

 private:
  Status FindSSZone(const SSZoneMetaData& meta, const size_t level,
                    Cache::Handle** handle);

  const InternalKeyComparator icmp_;
  const Options& options_;
  std::shared_ptr<Cache> cache_;
  ZNSSSTableManager* ssmanager_;
};

}  // namespace ROCKSDB_NAMESPACE

#endif
#endif