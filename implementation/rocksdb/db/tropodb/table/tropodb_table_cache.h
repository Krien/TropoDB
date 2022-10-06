// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread-safe (provides internal synchronization)

#pragma once
#ifdef TROPODB_PLUGIN_ENABLED
#ifndef ZNS_SSTABLE_CACHE_H
#define ZNS_SSTABLE_CACHE_H

#include <memory>

#include "db/dbformat.h"
#include "db/tropodb/table/tropodb_sstable.h"
#include "db/tropodb/table/tropodb_sstable_manager.h"
#include "rocksdb/cache.h"
#include "rocksdb/iterator.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
class TropoTableCache {
 public:
  TropoTableCache(const Options& options, const InternalKeyComparator& icmp,
                const size_t entries, TropoSSTableManager* ssmanager);
  TropoTableCache(const TropoTableCache&) = delete;
  TropoTableCache& operator=(const TropoTableCache&) = delete;
  ~TropoTableCache();

  Iterator* NewIterator(const ReadOptions& options, const SSZoneMetaData& meta,
                        const uint8_t level, TropoSSTable** tableptr = nullptr);

  Status Get(const ReadOptions& options, const SSZoneMetaData& meta,
             const uint8_t level, const Slice& key, std::string* value,
             EntryStatus* status);

  void Evict(const uint64_t ss_number);

 private:
  Status FindSSZone(const SSZoneMetaData& meta, const uint8_t level,
                    Cache::Handle** handle);

  const InternalKeyComparator icmp_;
  const Options& options_;
  std::shared_ptr<Cache> cache_;
  TropoSSTableManager* ssmanager_;
};

}  // namespace ROCKSDB_NAMESPACE

#endif
#endif
