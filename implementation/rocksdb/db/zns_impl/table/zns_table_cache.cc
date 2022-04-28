// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread-safe (provides internal synchronization)

#include "db/zns_impl/table/zns_table_cache.h"

#include "db/zns_impl/table/zns_sstable_manager.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

static void DeleteEntry(const Slice& key, void* value) {
  Iterator* it = reinterpret_cast<Iterator*>(value);
  delete it;
}

// static void UnrefEntry(void* arg1, void* arg2) {
//   Cache* cache = reinterpret_cast<Cache*>(arg1);
//   Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
//   cache->Release(h);
// }

ZnsTableCache::ZnsTableCache(const Options& options,
                             const InternalKeyComparator& icmp, int entries,
                             ZNSSSTableManager* ssmanager)
    : icmp_(icmp), options_(options), ssmanager_(ssmanager) {
  LRUCacheOptions opts;
  opts.capacity = entries;
  cache_ = NewLRUCache(opts);
}

ZnsTableCache::~ZnsTableCache() { cache_.reset(); }

Status ZnsTableCache::FindSSZone(SSZoneMetaData* meta, size_t level,
                                 Cache::Handle** handle) {
  Status s;
  char buf[sizeof(meta->number)];
  EncodeFixed64(buf, meta->number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if (*handle == nullptr) {
    Iterator* it = ssmanager_->NewIterator(level, meta, icmp_);
    s = cache_->Insert(key, it, 1, &DeleteEntry, handle);
  }
  return s;
}

Iterator* ZnsTableCache::NewIterator(const ReadOptions& options,
                                     SSZoneMetaData* meta, size_t level,
                                     ZnsSSTable** tableptr) {
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
  Status s = FindSSZone(meta, level, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Iterator* it = reinterpret_cast<Iterator*>(cache_->Value(handle));
  return it;
}

Status ZnsTableCache::Get(const ReadOptions& options, SSZoneMetaData* meta,
                          size_t level, const Slice& key, std::string* value,
                          EntryStatus* status) {
  Cache::Handle* handle = nullptr;
  Status s = FindSSZone(meta, level, &handle);
  if (s.ok()) {
    Iterator* it = reinterpret_cast<Iterator*>(cache_->Value(handle));
    it->Seek(key);
    if (it->Valid()) {
      *value = it->value().ToString();
      *status =
          it->value().size() > 0 ? EntryStatus::found : EntryStatus::deleted;
    } else {
      *status = EntryStatus::notfound;
    }
    cache_->Release(handle);
  }
  return s;
}

void ZnsTableCache::Evict(uint64_t ss_number) {
  char buf[sizeof(ss_number)];
  EncodeFixed64(buf, ss_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}
}  // namespace ROCKSDB_NAMESPACE