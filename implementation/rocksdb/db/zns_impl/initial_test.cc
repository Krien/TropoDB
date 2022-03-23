// Copyright (c) 2013 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "db_impl_zns.h"

namespace ROCKSDB_NAMESPACE {
int main_test(int argc, char** argv) {
    std::string dbname_ = "0000:00:04.0";
    Options options_;
    options_.create_if_missing = true;
    options_.compression = kNoCompression;
    DB* db_; 
    std::vector<ColumnFamilyDescriptor> column_families;
    std::vector<ColumnFamilyHandle*> handles;
    Status s = DBImplZNS::Open(options_, dbname_, column_families, &handles, &db_, false, false);
    assert(s.ok());
    // read false
    ReadOptions ro = ReadOptions();
    const Slice key_test = Slice("hello");
    std::string value_test;
    s = db_->Get(ro, key_test, &value_test);
    assert(!s.ok());
    // put one pair
    WriteOptions wo = WriteOptions();
    const Slice test_value = Slice("lsmzns !");
    s = db_->Put(wo, key_test, test_value);
    assert(s.ok());
    // read one correct pair
    s = db_->Get(ro, key_test, &value_test);
    assert(s.ok());
    assert(value_test == "lsmzns !");
    printf("Time for coffee \n");
    return 0;
}
}

int main(int argc, char** argv) {
    return rocksdb::main_test(argc, argv);
}
