// Copyright (c) 2013 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "db/db_impl.h"

namespace leveldb {
int main_test(int argc, char** argv) {
    std::string dbname_ = "0000:00:04.0";
    Options options_;
    options_.create_if_missing = true;
    options_.compression = kNoCompression;
    DB* db_; 
    Status s = DB::Open(options_, dbname_, &db_);
    assert(s.ok());
    // read false
    leveldb::ReadOptions ro = leveldb::ReadOptions();
    const leveldb::Slice key_test = leveldb::Slice("hello");
    std::string value_test;
    s = db_->Get(ro, key_test, &value_test);
    assert(!s.ok());
    // put one pair
    leveldb::WriteOptions wo = leveldb::WriteOptions();
    const leveldb::Slice test_value = leveldb::Slice("lsmzns!");
    s = db_->Put(wo, key_test, test_value);
    assert(s.ok());
    // read one correct pair
    s = db_->Get(ro, key_test, &value_test);
    assert(s.ok());
    assert(value_test == "lsmzns!");
    return 0;
}
}

int main(int argc, char** argv) {
    return leveldb::main_test(argc, argv);
}
