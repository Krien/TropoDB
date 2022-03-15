// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <set>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "lsm_zns/db.h"
#include "lsm_zns/env.h"
#include "lsm_zns/status.h"
#include "port/port.h"


namespace leveldb {

const int kNumNonTableCacheFiles = 10;

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(raw_options){}

DBImpl::~DBImpl() {
}

Status DBImpl::NewDB() {
  return Status::OK();
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,
                               const Slice* end) {
}

Status DBImpl::TEST_CompactMemTable() {
  return Status::OK();
}

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  return nullptr;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  return nullptr;
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  return -1;
}

Status DBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value) {
  return Status::OK();
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  return nullptr;
}

void DBImpl::RecordReadSample(Slice key) {
}

const Snapshot* DBImpl::GetSnapshot() {
  return nullptr;
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {

}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return Status::OK();
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return Status::OK();
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
    return Status::OK();
}


bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  return false;
}

void DBImpl::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  return Status::OK();
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  return Status::OK();

}

DB::~DB() = default;

Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
   return Status::OK();
}

Snapshot::~Snapshot() = default;

Status DestroyDB(const std::string& dbname, const Options& options) {
  return Status::OK();
}

}  // namespace leveldb
