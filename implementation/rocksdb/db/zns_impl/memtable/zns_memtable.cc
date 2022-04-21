#include "db/zns_impl/memtable/zns_memtable.h"

#include "db/column_family.h"
#include "db/memtable.h"
#include "db/merge_context.h"
#include "options/cf_options.h"
#include "table/internal_iterator.h"

namespace ROCKSDB_NAMESPACE {
ZNSMemTable::ZNSMemTable(const DBOptions& db_options,
                         const InternalKeyComparator& ikc) {
  Options options(db_options, ColumnFamilyOptions());
  ImmutableOptions ioptions(options);
  wb_ = new WriteBufferManager(options.db_write_buffer_size);
  mem_ = new ColumnFamilyMemTablesDefault(
      new MemTable(ikc, ioptions, MutableCFOptions(options), this->wb_,
                   kMaxSequenceNumber, 0 /* column_family_id */));
  mem_->GetMemTable()->Ref();
  write_buffer_size_ = options.write_buffer_size;  // options.write_buffer_size;
}

ZNSMemTable::~ZNSMemTable() {
  // printf("Deleting memtable.\n");
  mem_->GetMemTable()->Unref();
  delete mem_;
  delete wb_;
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
  Arena arena;
  return mem_->GetMemTable()->NewIterator(ReadOptions(), &arena);
}
}  // namespace ROCKSDB_NAMESPACE
