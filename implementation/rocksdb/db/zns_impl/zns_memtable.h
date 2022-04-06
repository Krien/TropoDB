#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_MEMTABLE_H
#define ZNS_MEMTABLE_H

#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/internal_iterator.h"

namespace ROCKSDB_NAMESPACE {
class ZNSMemTable {
 public:
  ZNSMemTable(const DBOptions& options, const InternalKeyComparator& ikc);
  ~ZNSMemTable();
  Status Write(const WriteOptions& options, WriteBatch* updates);
  Status Get(const ReadOptions& options, const Slice& key, std::string* value);
  bool ShouldScheduleFlush();
  InternalIterator* NewIterator();
  inline void Ref() { mem_->GetMemTable()->Ref(); }
  inline void Unref() { mem_->GetMemTable()->Unref(); }

 private:
  ColumnFamilyMemTables* mem_;
  WriteBufferManager* wb_;
  size_t write_buffer_size_;
};
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
