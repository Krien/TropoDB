#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_MEMTABLE_H
#define ZNS_MEMTABLE_H

#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "db/zns_impl/ref_counter.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/internal_iterator.h"

namespace ROCKSDB_NAMESPACE {
class ZNSMemTable : public RefCounter {
 public:
  ZNSMemTable(const DBOptions& options, const InternalKeyComparator& ikc);
  ~ZNSMemTable();
  Status Write(const WriteOptions& options, WriteBatch* updates);
  bool Get(const ReadOptions& options, const LookupKey& key, std::string* value,
           Status* s);
  bool ShouldScheduleFlush();
  InternalIterator* NewIterator();
  // not thread safe, I think.
  inline uint64_t GetInternalSize() {
    return this->mem_->GetMemTable()->get_data_size();
  }

 private:
  ColumnFamilyMemTables* mem_;
  WriteBufferManager* wb_;
  size_t write_buffer_size_;
};
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
