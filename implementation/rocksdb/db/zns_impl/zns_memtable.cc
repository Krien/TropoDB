#include "db/zns_impl/zns_memtable.h"

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
  this->wb_ = new WriteBufferManager(options.db_write_buffer_size);
  this->mem_ = new ColumnFamilyMemTablesDefault(
      new MemTable(ikc, ioptions, MutableCFOptions(options), this->wb_,
                   kMaxSequenceNumber, 0 /* column_family_id */));
  this->write_buffer_size_ = 4096 * 8;  // options.write_buffer_size;
}

ZNSMemTable::~ZNSMemTable() {}

Status ZNSMemTable::Write(const WriteOptions& options, WriteBatch* updates) {
  assert(this->meNewIteratorm_ != nullptr);
  WriteBatchInternal::SetSequence(updates, 100);
  Status s =
      WriteBatchInternal::InsertInto(updates, this->mem_, nullptr, nullptr);
  return s;
}

Status ZNSMemTable::Get(const ReadOptions& options, const Slice& key,
                        std::string* value) {
  ReadOptions roptions;
  SequenceNumber max_covering_tombstone_seq = 0;
  LookupKey lkey(key, kMaxSequenceNumber);
  MergeContext merge_context;
  Status s;
  bool res = this->mem_->GetMemTable()->Get(
      lkey, value, /*timestamp=*/nullptr, &s, &merge_context,
      &max_covering_tombstone_seq, roptions);
  if (s.ok() && res) {
    return s;
  }
  return Status::NotFound("Key not found in memtable");
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