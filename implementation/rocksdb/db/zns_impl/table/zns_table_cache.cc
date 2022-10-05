// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread-safe (provides internal synchronization)

#include "db/zns_impl/table/zns_table_cache.h"

#include "db/zns_impl/table/zns_sstable_manager.h"
#include "db/zns_impl/utils/tropodb_logger.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

// Necessary at the moment...
// TODO: we do not want to lock on a iterator preferably.
// This is at the moment necessary because of a design flaw.
struct LockedIterator {
  port::Mutex mutex_;
  Iterator* it;
};

static void DeleteEntry(const Slice& key, void* value) {
  LockedIterator* it = reinterpret_cast<LockedIterator*>(value);
  delete it->it;
  delete it;
}

// static void UnrefEntry(void* arg1, void* arg2) {
//   Cache* cache = reinterpret_cast<Cache*>(arg1);
//   Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
//   cache->Release(h);
// }

ZnsTableCache::ZnsTableCache(const Options& options,
                             const InternalKeyComparator& icmp,
                             const size_t entries, ZNSSSTableManager* ssmanager)
    : icmp_(icmp), options_(options), ssmanager_(ssmanager) {
  LRUCacheOptions opts;
  opts.capacity = entries;
  cache_ = NewLRUCache(opts);
}

ZnsTableCache::~ZnsTableCache() { cache_.reset(); }

Status ZnsTableCache::FindSSZone(const SSZoneMetaData& meta,
                                 const uint8_t level, Cache::Handle** handle) {
  Status s;
  char buf[sizeof(meta.number)];
  EncodeFixed64(buf, meta.number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if (*handle == nullptr) {
    Iterator* it =
        ssmanager_->NewIterator(level, meta, icmp_.user_comparator());
    LockedIterator* lit = new LockedIterator;
    lit->it = it;
    s = cache_->Insert(key, lit, 1, &DeleteEntry, handle);
  }
  return s;
}

Iterator* ZnsTableCache::NewIterator(const ReadOptions& options,
                                     const SSZoneMetaData& meta,
                                     const uint8_t level,
                                     ZnsSSTable** tableptr) {
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
  Status s = FindSSZone(meta, level, &handle);
  if (!s.ok()) {
    TROPODB_ERROR("Error getting iterator\n");
    return NewErrorIterator(s);
  }

  Iterator* it = reinterpret_cast<Iterator*>(cache_->Value(handle));

  return it;
}

Status ZnsTableCache::Get(const ReadOptions& options,
                          const SSZoneMetaData& meta, const uint8_t level,
                          const Slice& key, std::string* value,
                          EntryStatus* status) {
  Cache::Handle* handle = nullptr;
  Status s = FindSSZone(meta, level, &handle);
  if (s.ok()) {
    LockedIterator* lit =
        reinterpret_cast<LockedIterator*>(cache_->Value(handle));
    lit->mutex_.Lock();
    Iterator* it = lit->it;
    it->Seek(key);
    if (it->Valid()) {
      ParsedInternalKey parsed_key;
      if (!ParseInternalKey(it->key(), &parsed_key, false).ok()) {
        *status = EntryStatus::notfound;
        TROPODB_ERROR(
            "corrupt key in table cache, for level %u and table %lu, str %s\n",
            level, meta.number, it->key().ToString().data());
      } else if (parsed_key.type == kTypeDeletion) {
        *status = EntryStatus::deleted;
        value->clear();
      } else {
        *status = EntryStatus::found;
        *value = it->value().ToString();
      }
    } else {
      *status = EntryStatus::notfound;
    }
    lit->mutex_.Unlock();
    cache_->Release(handle);
  }
  return s;
}

void ZnsTableCache::Evict(const uint64_t ss_number) {
  char buf[sizeof(ss_number)];
  EncodeFixed64(buf, ss_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}
}  // namespace ROCKSDB_NAMESPACE
