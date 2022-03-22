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
#include <iostream>

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
      options_(raw_options),
      name_(dbname){}

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
  char* val = (char*)ZnsDevice::z_calloc(*this->qpair_, 4096, sizeof(char));
  int rc = ZnsDevice::z_read(*this->qpair_, 0, val, 4096);
  int walker = 0;
  while(walker < 4096 && val[walker] != ':') {
    walker++;
  }
  if (walker == 4096) {
    return Status::NotFound("Invalid block");
  }
  if (strncmp(val, key.data(), walker) == 0) {
    *value = std::string(val+walker+1);
    return Status::OK();
  }
  return Status::NotFound("Key not found");
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
  char* valpt = (char*)ZnsDevice::z_calloc(*this->qpair_, 4096, sizeof(char));
  snprintf(valpt, 4096, "%s:%s", key.data(), val.data());
  int rc = ZnsDevice::z_append(*this->qpair_, 0, valpt, 4096);
  return rc == 0 ? Status::OK() : Status::IOError("Error appending to zone");
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

Status DBImpl::InitDB() {
  ZnsDevice::DeviceManager** device_manager = (ZnsDevice::DeviceManager**)calloc(1, sizeof(ZnsDevice::DeviceManager));
  int rc = ZnsDevice::z_init(device_manager);
  this->device_manager_ = device_manager;
  if (rc != 0) {
    return Status::IOError("Error opening SPDK");
  }
  rc = ZnsDevice::z_open(*device_manager, this->name_.c_str());
  if (rc != 0) {
    return Status::IOError("Error opening ZNS device");
  }
  this->qpair_ = (ZnsDevice::QPair**)calloc(1, sizeof(ZnsDevice::QPair*));
  rc = ZnsDevice::z_create_qpair(*device_manager, this->qpair_);
  if (rc != 0) {
    return Status::IOError("Error creating QPair");
  }
  rc = ZnsDevice::z_reset(*this->qpair_, 0, true);
  return rc == 0 ? Status::OK() : Status::IOError("Error resetting device");
}

Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
  DBImpl* impl = new DBImpl(options, dbname);
  *dbptr = impl;
  return impl->InitDB();
}

Snapshot::~Snapshot() = default;

Status DestroyDB(const std::string& dbname, const Options& options) {
  return Status::OK();
}

}  // namespace leveldb
