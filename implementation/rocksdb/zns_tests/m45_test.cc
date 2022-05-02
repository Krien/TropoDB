/*
 * MIT License
Copyright (c) 2021 - current
Authors:  Animesh Trivedi
This code is part of the Storage System Course at VU Amsterdam
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */

#include <getopt.h>
#include <unistd.h>

#ifdef NDEBUG
#undef NDEBUG
#endif
#include <cassert>
#include <cstdio>
#include <iostream>
#include <memory>

#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/file_system.h"

using namespace std;

struct MyRocksContext {
  std::string uri;
  rocksdb::Options options;
  rocksdb::DB *db;
  rocksdb::ConfigOptions config_options;
  std::shared_ptr<rocksdb::Env> env_guard;
};

static std::string genrate_random_string(const int len) {
  std::string str;
  static const char alphanum_char[] =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz@!#$%^&*("
      ")";
  str.reserve(len);
  for (int i = 0; i < len; ++i) {
    str += alphanum_char[rand() % (sizeof(alphanum_char))];
  }
  return str;
}

static int fill_up_map(std::map<std::string, std::string> &testmap, int entries,
                       int ksize, int vsize) {
  int count = 0;
  while (testmap.size() != (size_t)entries) {
    // the problem is that with small key sizes, we might run out of unique keys
    // to insert, hence we append the count at the end to make them unique and
    // then dynamically adjust the value size to control the total bytes of data
    // inserted in the database
    assert(count < entries);
    std::string key = genrate_random_string(ksize);
    key = key.append(to_string(count));  // this makes it unique
    auto x =
        testmap.insert({key, genrate_random_string((ksize + vsize) - key.size())
                                 .append(to_string(count))});
    // this would fail if there were not unique keys
    assert(x.second);
    (void)x;
    count++;
  }
  std::cout << "a testmap is filled with " << entries << " of max_size "
            << (ksize + vsize) << " each \n";
  return 0;
}

static int allocate_myrocks_context(struct MyRocksContext *&ctx) {
  ctx = new MyRocksContext[1];
  assert(ctx != nullptr);
  ctx->config_options.env = rocksdb::Env::Default();
  ctx->db = nullptr;
  return 0;
}

static void destroy_myrocks_context(struct MyRocksContext *&ctx) {
  assert(ctx != nullptr);
  if (ctx->db != nullptr) {
    rocksdb::Status s = ctx->db->Close();
    assert(s.ok());
    delete ctx->db;
  }
  // free ctx
  delete[] ctx;
}

static void print_myrocks_context(struct MyRocksContext *ctx) {
  assert(ctx != nullptr);
  std::cout << " uri: " << ctx->uri
            << " fs_attached: " << ctx->db->GetFileSystem()->Name() << " \n";
}

int open_zns_rocksdb(struct MyRocksContext *context, std::string db_path) {
  rocksdb::Status s;
  context->options.use_zns_impl = true;
  s = rocksdb::DB::Open(context->options, db_path, &context->db);
  if (!s.ok()) {
    return -1;
  }
  return 0;
}

// posix takes:  posix://.*"
// s2fs-rocksdb takes takes: s2fs:.*://.*
int open_rocksdb(struct MyRocksContext *context, const std::string delimiter) {
  std::string db_path = context->uri.substr(
      context->uri.find(delimiter) + delimiter.length(), context->uri.size());
  std::string uri_ext = context->uri.substr(0, context->uri.find(delimiter));
  rocksdb::Status s;
  cout << "Opening database at " << context->uri << " with uri " << uri_ext
       << " and the db_path as " << db_path << std::endl;
  if (uri_ext.compare("zns") == 0) {
    int rc = open_zns_rocksdb(context, db_path);
    if (rc != 0) {
      return rc;
    }
    cout << "## Database opened at " << context->uri << " db name is "
         << context->db->GetName() << "\n";
    return rc;
  }
  s = rocksdb::Env::CreateFromUri(context->config_options, "", context->uri,
                                  &(context->options.env), &context->env_guard);
  if (!s.ok()) {
    fprintf(stderr, "Create Env from uri failed, status %s \n",
            s.ToString().c_str());
    return -1;
  }
  std::cout << "Environment from URI " << context->uri << " for FS "
            << context->options.env->GetFileSystem()->Name() << " \n";
  s = rocksdb::DB::Open(context->options, db_path, &context->db);
  if (!s.ok()) {
    fprintf(stderr, "DB opening failed at %s due to %s \n\n",
            context->uri.c_str(), s.ToString().c_str());
    return -1;
  }
  cout << "## Database opened at " << context->uri << " db name is "
       << context->db->GetName() << " , attached FS is --> "
       << context->db->GetFileSystem()->Name() << "<-- \n";
  return 0;
}

static int delete_rocksdb(struct MyRocksContext *context,
                          const std::string delimiter) {
  std::string db_path = context->uri.substr(
      context->uri.find(delimiter) + delimiter.length(), context->uri.size());
  std::string uri_ext = context->uri.substr(0, context->uri.find(delimiter));
  rocksdb::Status s;
  cout << "Opening database at " << context->uri << " with uri " << uri_ext
       << " and the db_path as " << db_path << std::endl;
  if (uri_ext.compare("zns") == 0) {
    context->options.use_zns_impl = true;
    s = rocksdb::DestroyDB(db_path, context->options);
    return s.ok() ? 0 : -1;
  }
  s = rocksdb::Env::CreateFromUri(context->config_options, "", context->uri,
                                  &(context->options.env), &context->env_guard);
  if (!s.ok()) {
    fprintf(stderr, "Create Env from uri failed, status %s \n",
            s.ToString().c_str());
    return -1;
  }
  std::cout << "Environment from URI " << context->uri << " for FS "
            << context->options.env->GetFileSystem()->Name() << " \n";
  s = rocksdb::DestroyDB(db_path, context->options);
  if (!s.ok()) {
    fprintf(stderr, "DB deleting failed at %s due to %s \n\n",
            context->uri.c_str(), s.ToString().c_str());
    return -1;
  }
  cout << "Database deleted at " << context->uri << endl;
  return 0;
}

static void show_help() {
  cout << "options: -p val -t val -r -s val -e val -d -h\n";
  cout << " -p test_db_uri, example: -p posix:///tmp/testdb or -p "
          "s2fs:nvme0n1:///tmp/atr1/ , delimited is :// (3 char) \n";
  cout
      << " -t is test/shadow DB which will be populated in parallel with the "
         "same keys as the testdb and is used when using roverify in ro mode\n";
  cout << "-r says do a readonly verification from the shadowdb, it is assumed "
          "that the shadow db is primed before by running this program once\n";
  cout << " -k integer: max size of the key\n ";
  cout << " -v integer: max size of the value\n ";
  cout << " -e integer: total number of entries to test for \n";
  cout << " -l integer: total number of entries to delete after e\n";
  cout << "-D delete the databases at the test and shadow locations\n";
  cout << "-S do a single DB test on the test_db with -p, no comparison \n";
  cout << "-d show debugging information\n";
  cout << "-h show this help\n";
}

// if the changes are not being picked up then you need to delete and reinstall
// the file rm librocksdb.a; cp ../../../storage/rocksdb/librocksdb.a .
int main(int argc, char **argv) {
  srand((unsigned)time(nullptr) * getpid());
  printf("============================================================== \n");
  std::cout << "Welcome to milestone 4/5, which is about integration into "
               "RocksDB (also: congratulations for bearing with us so far!)\n";
  printf("============================================================== \n");
  const std::string delimiter = "://";
  int ksize = 10, vsize = 90, entires = 1, deletes = 0;
  std::map<std::string, std::string> testdata;
  rocksdb::Status s;
  int c, ret;
  bool roverify = false, deleteall = false, debug = false, single = false;

  MyRocksContext *ctx_test = nullptr, *ctx_shadow = nullptr;
  ret = allocate_myrocks_context(ctx_test);
  assert(ret == 0 && ctx_test != nullptr);
  ret = allocate_myrocks_context(ctx_shadow);
  assert(ret == 0 && ctx_shadow != nullptr);

  ctx_test->uri = "zns://0000:00:04.0";
  // ctx_test->uri = "posix:///tmp/shadow2db";
  // ctx_shadow->uri = "zenfs://dev:nvme1n1";
  ctx_shadow->uri = "posix:///tmp/shadowdb";
  ctx_test->options.create_if_missing = true;
  ctx_shadow->options.create_if_missing = true;

  while ((c = getopt(argc, argv, "p:t:rv:k:e:l:dDhS")) != -1) {
    switch (c) {
      case 'h':
        show_help();
        exit(0);
      case 'p':
        ctx_test->uri = optarg;
        break;
      case 't':  // t for test/shadow db
        ctx_shadow->uri = optarg;
        break;
      case 'v':
        vsize = atoi(optarg);
        break;
      case 'k':
        ksize = atoi(optarg);
        break;
      case 'e':
        entires = atoi(optarg);
        break;
      case 'l':
        deletes = atoi(optarg);
        break;
      case 'D':
        deleteall = true;
        break;
      case 'S':
        single = true;
        break;
      case 'd':
        debug = true;
        break;
      case 'r':
        roverify = true;
        // what we dont want - in case of verification that if the db does not
        // exist do not create
        ctx_shadow->options.create_if_missing = false;
        break;
      default:
        fprintf(stderr,
                "Unknown option `-%c'. Supported arguments are -p (fs_path "
                "with uri) and enable debugging -d \n",
                optopt);
        show_help();
        exit(-1);
    }
  }
  printf("============================================================== \n");
  std::cout << "test_uri " << ctx_test->uri << " , shadow uri "
            << ctx_shadow->uri << "\n";
  std::cout << "entries " << entires << ", each entry ksize " << ksize
            << " bytes, vsize " << vsize
            << " bytes, readonly verify : " << roverify
            << " deleteall : " << deleteall << " single : " << single << "\n";
  printf("============================================================== \n");
  if (deleteall && roverify) {
    std::cout << "Delete all and roverify both are set, I cannot do that \n";
    return -EINVAL;
  }
  if (single && roverify) {
    std::cout << "Single and roverify both are set, I cannot do that \n";
    return -EINVAL;
  }
  if (deletes > 0 && roverify) {
    std::cout
        << "Multiple deletes and roverify both are set, I cannot do that \n";
    return -EINVAL;
  }
  if (deletes > entires) {
    std::cout << "More data will be deleted than is add, I cannot do that \n";
    return -EINVAL;
  }

  if (deleteall) {
    ret = delete_rocksdb(ctx_test, delimiter);
    if (0 != ret) {
      return -1;
    }
    if (!single) {
      ret = delete_rocksdb(ctx_shadow, delimiter);
      if (0 != ret) {
        return -1;
      }
    }
    // once we have cleaned up - we need to set a new db
    ctx_shadow->options.create_if_missing = true;
    ctx_test->options.create_if_missing = true;
  }

  if (!single) {
    ret = open_rocksdb(ctx_shadow, delimiter);
    if (ret != 0) {
      return -1;
    }
    print_myrocks_context(ctx_shadow);
    assert(ret == 0);
    assert(ctx_shadow->db != nullptr);
  }
  ret = open_rocksdb(ctx_test, delimiter);
  assert(ret == 0);
  assert(ctx_test->db != nullptr);
  rocksdb::WriteOptions wo = rocksdb::WriteOptions();

  if (!roverify) {
    std::cout << "Preparing the map to insert values for " << entires
              << " entries, max size " << (ksize + vsize) << "\n";
    // init the dataset
    ret = fill_up_map(testdata, entires, ksize, vsize);
    assert(ret == 0);
    // if not just ro verify is set then that means we need to insert values
    // we have data, lets write it out to the test DB and then just compare and
    // read with the test and shadow dbs
    for (auto it = testdata.begin(); it != testdata.end(); ++it) {
      if (debug) {
        cout << "inserting: " << it->first << " : " << it->second << endl;
      }
      s = ctx_test->db->Put(wo, (*it).first, (*it).second);
      assert(s.ok());
      if (!single) {
        // if we are not running in the single mode, then insert in the
        // shadow-db too
        s = ctx_shadow->db->Put(wo, (*it).first, (*it).second);
        assert(s.ok());
      }
    }
    std::cout << "All values inserted, number of entries " << entires
              << ", expected size stores would be "
              << ((ksize + vsize) * entires) << " bytes \n";

    int i = deletes;
    for (auto it = testdata.begin(); it != testdata.end(); ++it) {
      s = ctx_test->db->Delete(wo, (*it).first);
      assert(s.ok());
      if (!single) {
        s = ctx_shadow->db->Delete(wo, (*it).first);
        assert(s.ok());
      }
      i--;
      if (i == 0) {
        break;
      }
    }
  }
  // shadow DB must be a posix db so we know it works
  // lets get an iterator
  std::cout << "Starting data reading from the shadow db at " << ctx_shadow->uri
            << " | single " << single << "\n";
  if (!single) {
    rocksdb::ReadOptions ro = rocksdb::ReadOptions();
    rocksdb::Iterator *it = ctx_shadow->db->NewIterator(ro);
    std::string test_value;
    uint64_t ent = 0;
    uint64_t ent_total = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      ent_total++;
      if (debug) {
        cout << "reading: " << it->key().ToString() << " : "
             << it->value().ToString() << endl;
      }
      s = ctx_test->db->Get(ro, it->key().ToString(), &test_value);
      if (!s.ok()) {
        cout << "reading of test DB failed with " << s.ToString()
             << " for the key (read from the shadowdb): "
             << it->key().ToString();
        auto x = testdata.find(it->key().ToString());
        if (x != testdata.end())
          cout << " NEW!";
        else
          cout << " OLD!";
        cout << "\n";
        continue;
      }
      assert(s.ok());
      // now we have shadow value and test value - they must be the same
      if (test_value != it->value().ToString()) {
        std::cout << "wrong val " << it->value().ToString() << " should be "
                  << test_value << "\n";
      } else {
        ent++;
      }
      test_value = "";
    }
    cout << "********************************************** \n";
    std::cout << "OK: all " << ent << " out of " << ent_total
              << " values matched successfully \n ";
    cout << "********************************************** \n";
    assert(it->status().ok());  // Check for any errors found during the scan
    delete it;
  } else {
    // we just try to read what we have inserted
    rocksdb::ReadOptions ro = rocksdb::ReadOptions();
    std::string test_value;
    uint64_t ent = 0;
    int i = deletes;
    for (auto it = testdata.begin(); it != testdata.end(); ++it) {
      if (i > 0) {
        i--;
        continue;
      }
      s = ctx_test->db->Get(ro, it->first, &test_value);
      assert(s.ok());
      if (test_value != it->second) {
        cout << "Error: the values did not match, expecting " << it->second
             << " | found = " << test_value << " , continuing...\n";
      } else {
        ent++;
      }
    }
    cout << "********************************************** \n";
    std::cout << "OK: " << ent << " out of " << testdata.size() - deletes
              << " values matched successfully in the SINGLE mode \n";
    cout << "********************************************** \n";
  }

  if (deletes > 0) {
    int i = deletes;
    rocksdb::ReadOptions ro = rocksdb::ReadOptions();
    std::string test_value;
    for (auto it = testdata.begin(); it != testdata.end(); ++it) {
      s = ctx_test->db->Get(ro, it->first, &test_value);
      if (s.ok()) {
        std::cout << test_value.length() << "\n";
        assert(!s.ok() && test_value.length() == 0);
      }
      if (!single) {
        s = ctx_shadow->db->Get(ro, it->first, &test_value);
        assert(!s.ok() && test_value.length() == 0);
      }
      i--;
      if (i == 0) {
        break;
      }
    }
    cout << "********************************************** \n";
    std::cout << "OK: all " << deletes
              << " deleted entries succesfully not found \n";
    if (single) {
      std::cout << " in the SINGLE mode \n";
    }
    cout << "********************************************** \n";
  }

  // will close the db - for now shadow context is created unconditionally, but
  // can be moved in the single mode
  destroy_myrocks_context(ctx_shadow);
  destroy_myrocks_context(ctx_test);
  cout << "database(s) closed, test is done OK " << endl;
  return 0;
}
