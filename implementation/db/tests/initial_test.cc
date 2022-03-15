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
    std::string dbname_ = "hello world";
    Options options_;
    options_.create_if_missing = true;
    options_.compression = kNoCompression;
    DB* db_; 
    Status s = DB::Open(options_, dbname_, &db_);
    std::cout << "OK? " << s.ok() << "\n"; 
    return 0;
}
}

int main(int argc, char** argv) {
    return leveldb::main_test(argc, argv);
}