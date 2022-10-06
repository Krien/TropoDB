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
  ZNSMemTable(const DBOptions& options, const InternalKeyComparator& ikc,
              const size_t buffer_size);
  ~ZNSMemTable();
  Status Write(const WriteOptions& options, WriteBatch* updates);
  bool Get(const ReadOptions& options, const LookupKey& key, std::string* value,
           Status* s, SequenceNumber* seq = nullptr);
  bool ShouldScheduleFlush();
  InternalIterator* NewIterator();
  // not thread safe
  inline uint64_t GetInternalSize() {
    return this->mem_->GetMemTable()->get_data_size();
  }

 private:
  // Meta
  Options options_;
  const ImmutableOptions ioptions_;
  const size_t write_buffer_size_;
  WriteBufferManager wb_;
  // Actual in memory table
  Arena arena_;
  ColumnFamilyMemTables* mem_;
};
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
