#include "db/zns_impl/memtable/zns_memtable.h"

#include "db/column_family.h"
#include "db/memtable.h"
#include "db/merge_context.h"
#include "options/cf_options.h"
#include "table/internal_iterator.h"

namespace ROCKSDB_NAMESPACE {
ZNSMemTable::ZNSMemTable(const DBOptions& db_options,
                         const InternalKeyComparator& ikc)
    : options_(db_options, ColumnFamilyOptions()),
      ioptions_(options_),
      write_buffer_size_(options_.write_buffer_size),
      wb_(options_.db_write_buffer_size),
      arena_() {
  mem_ = new ColumnFamilyMemTablesDefault(
      new MemTable(ikc, ioptions_, MutableCFOptions(options_), &wb_,
                   kMaxSequenceNumber, 0 /* column_family_id */));
  mem_->GetMemTable()->Ref();
}

ZNSMemTable::~ZNSMemTable() {
  // printf("Deleting memtable.\n");
  mem_->GetMemTable()->Unref();
  delete mem_->GetMemTable();
  delete mem_;
}

Status ZNSMemTable::Write(const WriteOptions& options, WriteBatch* updates) {
  Status s =
      WriteBatchInternal::InsertInto(updates, this->mem_, nullptr, nullptr);
  return s;
}

bool ZNSMemTable::Get(const ReadOptions& options, const LookupKey& lkey,
                      std::string* value, Status* s) {
  ReadOptions roptions;
  SequenceNumber max_covering_tombstone_seq = 0;
  MergeContext merge_context;
  return this->mem_->GetMemTable()->Get(lkey, value, /*timestamp=*/nullptr, s,
                                        &merge_context,
                                        &max_covering_tombstone_seq, roptions);
}

bool ZNSMemTable::ShouldScheduleFlush() {
  size_t current_size = mem_->GetMemTable()->ApproximateMemoryUsage();
  size_t allowed_size = write_buffer_size_;
  return current_size > allowed_size;
}

InternalIterator* ZNSMemTable::NewIterator() {
  return mem_->GetMemTable()->NewIterator(ReadOptions(), &arena_);
}
}  // namespace ROCKSDB_NAMESPACE
