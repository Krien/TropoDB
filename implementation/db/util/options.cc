// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "lsm_zns/options.h"

#include "lsm_zns/comparator.h"
//#include "lsm_zns/env.h"

namespace leveldb {

Options::Options() : comparator(BytewiseComparator()), env(nullptr) {}

}  // namespace leveldb
