// Copyright (c) 2013 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"

int main_test(int argc, char** argv) {
  std::string dbname_ = "0000:00:04.0";
  rocksdb::Options options_;
  options_.create_if_missing = true;
  options_.compression = rocksdb::kNoCompression;
  options_.use_zns_impl = true;
  rocksdb::DB* db_;
  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  rocksdb::Status s = rocksdb::DB::Open(options_, dbname_, &db_);
  assert(s.ok());
  // read false
  rocksdb::ReadOptions ro = rocksdb::ReadOptions();
  const rocksdb::Slice key_test = rocksdb::Slice("hello");
  std::string value_test;
  s = db_->Get(ro, key_test, &value_test);
  std::cout << value_test << "\n";
  assert(!s.ok());
  // put one pair
  rocksdb::WriteOptions wo = rocksdb::WriteOptions();
  const rocksdb::Slice test_value = rocksdb::Slice("lsmzns !");
  s = db_->Put(wo, key_test, test_value);
  assert(s.ok());
  // read one correct pair
  s = db_->Get(ro, key_test, &value_test);
  assert(s.ok());
  assert(value_test == "lsmzns !");
  printf("Time for coffee \n");
  return 0;
}

int main(int argc, char** argv) { return main_test(argc, argv); }
