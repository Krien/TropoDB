#pragma once
#ifdef ZNS_PLUGIN_ENABLED
#ifndef ZNS_SSTABLE_H
#define ZNS_SSTABLE_H

#include "db/zns_impl/device_wrapper.h"
#include "db/zns_impl/qpair_factory.h"
#include "db/zns_impl/ref_counter.h"
#include "db/zns_impl/zns_memtable.h"
#include "db/zns_impl/zns_zonemetadata.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/iterator.h"

#include <iostream>

namespace ROCKSDB_NAMESPACE {
enum class EntryStatus { found, deleted, notfound };

class ZNSSSTableManager;

class ZnsSSTable {
 public:
  ZnsSSTable(QPairFactory* qpair_factory, const ZnsDevice::DeviceInfo& info,
             const uint64_t min_zone_head, uint64_t max_zone_head);
  virtual ~ZnsSSTable();
  virtual Status ReadSSTable(Slice* sstable, SSZoneMetaData* meta) = 0;
  virtual Status Get(const Slice& key, std::string* value, SSZoneMetaData* meta,
                     EntryStatus* entry) = 0;
  virtual bool EnoughSpaceAvailable(Slice slice) = 0;
  virtual Status InvalidateSSZone(SSZoneMetaData* meta) = 0;
  virtual Status WriteSSTable(Slice content, SSZoneMetaData* meta) = 0;
  virtual Iterator* NewIterator(SSZoneMetaData* meta) = 0;

 protected:
  void PutKVPair(std::string* dst, const Slice& key, const Slice& value);

  // data
  uint64_t zone_head_;
  uint64_t write_head_;
  uint64_t zone_tail_;
  uint64_t write_tail_;
  uint64_t min_zone_head_;
  uint64_t max_zone_head_;
  uint64_t zone_size_;
  uint64_t lba_size_;
  // references
  QPairFactory* qpair_factory_;
  ZnsDevice::QPair** qpair_;
};

// Like a Oroborous, an entire circle without holes.
class L0ZnsSSTable : public ZnsSSTable {
 public:
  L0ZnsSSTable(QPairFactory* qpair_factory, const ZnsDevice::DeviceInfo& info,
               const uint64_t min_zone_head, uint64_t max_zone_head);
  ~L0ZnsSSTable();
  Status FlushMemTable(ZNSMemTable* mem, SSZoneMetaData* meta);
  Status ReadSSTable(Slice* sstable, SSZoneMetaData* meta) override;
  Status Get(const Slice& key, std::string* value, SSZoneMetaData* meta,
             EntryStatus* entry) override;
  bool EnoughSpaceAvailable(Slice slice) override;
  Status InvalidateSSZone(SSZoneMetaData* meta);
  Status WriteSSTable(Slice content, SSZoneMetaData* meta) override;
  Iterator* NewIterator(SSZoneMetaData* meta) override;


 private:
  friend class ZnsSSTableManagerInternal;
  Status GenerateSSTableString(std::string* dst, ZNSMemTable* mem,
                               SSZoneMetaData* meta);
  Status SetWriteAddress(Slice slice);
  Status ConsumeTail(uint64_t begin_lba, uint64_t end_lba);
  bool ValidateReadAddress(SSZoneMetaData* meta);
  static void ParseNext(char** src, Slice* key, Slice* value);
  uint64_t pseudo_write_head_;
  port::Mutex mutex_;
};

class L0ZnsSSTableIterator : public Iterator {
  public:
    L0ZnsSSTableIterator(char* data, size_t count, 
    void(*nextf)(char** src, Slice*key, Slice*value))
      : data_(data), walker_(data), nextf_(nextf), index_(0), count_(count), current_val_("deadbeef"),
      current_key_("deadbeef") {
    }
    bool Valid() const override { return index_ <= count_;}
    void Seek(const Slice& target) override {
      walker_ = data_;
      index_ = 0;
      while (Valid()) {
        index_++;
        nextf_(&walker_, &current_key_, &current_val_);
        if (target.compare(current_key_) == 0) {
          break;
        }
      }
    }
    void SeekForPrev(const Slice& target) override {
      Seek(target);
      Prev();
    }
    void SeekToFirst() override { 
      walker_ = data_;
      nextf_(&walker_, &current_key_, &current_val_);
      index_ = 1; 
    }
    void SeekToLast() override {
      while (index_ < count_) {
        index_++;
        nextf_(&walker_, &current_key_, &current_val_);
      }
    }
    void Next() override {
      assert(Valid());
      nextf_(&walker_, &current_key_, &current_val_);
      index_++;
    }
    void Prev() override {
      size_t target = index_-1;
      walker_ = data_;
      index_ = 0; 
      while (index_ < target) {
        index_++;
        nextf_(&walker_, &current_key_, &current_val_);
      }
      // set to invalid next iteration.
      if (index_ == 1) {
          index_ = count_+1;
      }
    }
    Slice key() const override {
      assert(Valid());
      return current_key_;
    }
    Slice value() const override {
      assert(Valid());
      return current_val_;
    }
    Status status() const override {
      return Status::OK();
    }
  private:
    char* data_;
    char* walker_;
    void(*nextf_)(char** src, Slice*key, Slice*value);
    size_t index_;
    size_t count_;
    Slice current_val_;
    Slice current_key_;
};

// Can have holes.
// class LnZnsSSTable : public ZnsSSTable {

// };

/**
 * @brief To be used for debugging private variables of ZNSSSTableManager only.
 */
class ZnsSSTableManagerInternal {
 public:
  static inline uint64_t GetZoneHead(L0ZnsSSTable* sstable) {
    return sstable->zone_head_;
  }
  static inline uint64_t GetWriteHead(L0ZnsSSTable* sstable) {
    return sstable->write_head_;
  }
  static inline uint64_t GetMinZoneHead(L0ZnsSSTable* sstable) {
    return sstable->min_zone_head_;
  }
  static inline uint64_t GetMaxZoneHead(L0ZnsSSTable* sstable) {
    return sstable->max_zone_head_;
  }
  static inline uint64_t GetZoneSize(L0ZnsSSTable* sstable) {
    return sstable->zone_size_;
  }
  static inline uint64_t GetLbaSize(L0ZnsSSTable* sstable) {
    return sstable->lba_size_;
  }

  static Status ConsumeTail(L0ZnsSSTable* sstable, uint64_t begin_lba,
                            uint64_t end_lba) {
    return sstable->ConsumeTail(begin_lba, end_lba);
  }
};
}  // namespace ROCKSDB_NAMESPACE
#endif
#endif
