
#include "db/tropodb/table/tropodb_sstable_manager.h"

#include "db/tropodb/tests/zns_test_utils.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {
class SSTableTest : public testing::Test {};

static void WasteLba(Device* device) {
  DBOptions options;
  WriteOptions woptions;
  InternalKeyComparator icmp = InternalKeyComparator(BytewiseComparator());
  SSZoneMetaData meta;
  ZNSMemTable* mem = new ZNSMemTable(options, icmp);
  mem->Ref();
  WriteBatch batch = WriteBatch();
  batch.Put(Slice("123"), Slice("456"));
  mem->Write(woptions, &batch);
  device->ss_manager->FlushMemTable(mem, &meta);
  mem->Unref();
}

TEST_F(SSTableTest, TESTFILL) {
  Device dev;
  DBOptions options;
  WriteOptions woptions;
  InternalKeyComparator icmp = InternalKeyComparator(BytewiseComparator());
  Status s;
  uint64_t begin = 3;
  uint64_t end = 5;
  // Initial
  SetupDev(&dev, begin, end, false);
  ZnsDevice::DeviceInfo info = (*dev.device_manager)->info;
  ValidateMeta(&dev, begin, end);
  L0ZnsSSTable* sstable = dev.ss_manager->GetL0SSTableLog();
  ASSERT_EQ(ZnsSSTableManagerInternal::GetZoneHead(sstable),
            info.zone_size * begin);
  ASSERT_EQ(ZnsSSTableManagerInternal::GetWriteHead(sstable),
            info.zone_size * begin);
  // 1 pair
  ZNSMemTable* mem = new ZNSMemTable(options, icmp);
  mem->Ref();
  ASSERT_EQ(mem->GetInternalSize(), 0);
  WriteBatch batch;
  batch.Put(Slice("123"), Slice("456"));
  mem->Write(woptions, &batch);
  SSZoneMetaData meta;
  dev.ss_manager->FlushMemTable(mem, &meta);
  std::string value;
  EntryStatus stata;
  s = dev.ss_manager->Get(0, Slice("123"), &value, &meta, &stata);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(Slice(value) == Slice("456"));
  ASSERT_EQ(meta.lba, info.zone_size * begin);
  ASSERT_EQ(meta.numbers, 1);
  ASSERT_EQ(meta.lba_count, 1);
  ASSERT_TRUE(meta.smallest.Encode() == meta.largest.Encode());
  // 1000 pairs (enough to cause multiple lbas for 4kb lbasize)
  mem->Unref();
  mem = new ZNSMemTable(options, icmp);
  mem->Ref();
  batch = WriteBatch();
  for (int i = 0; i < 1000; i++) {
    batch.Put(Slice(std::to_string(i)), Slice(std::to_string(i % 20)));
  }
  mem->Write(woptions, &batch);
  ASSERT_TRUE(mem->GetInternalSize() > info.lba_size);
  dev.ss_manager->FlushMemTable(mem, &meta);
  uint64_t lbas = (mem->GetInternalSize() + info.lba_size - 1) / info.lba_size;
  ASSERT_EQ(meta.lba, info.zone_size * begin + 1);
  ASSERT_EQ(meta.numbers, 1000);
  ASSERT_TRUE(meta.lba_count <= lbas);
  ASSERT_TRUE(meta.smallest.user_key() ==
              InternalKey(Slice("0"), 0, kTypeValue).user_key());
  ASSERT_TRUE(meta.largest.user_key() ==
              InternalKey(Slice("999"), 0, kTypeValue).user_key());
  for (int i = 0; i < 1000; i++) {
    value.clear();
    s = dev.ss_manager->Get(0, Slice(std::to_string(i)), &value, &meta, &stata);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(stata == EntryStatus::found);
    ASSERT_TRUE(Slice(std::to_string(i % 20)) == Slice(value));
  }
  // Try to stop just before a zone
  for (uint64_t i = meta.lba + lbas; i < (begin + 1) * info.zone_size - 1;
       i++) {
    WasteLba(&dev);
  }
  // Now do a multiple zone SSTable...
  mem->Unref();
  mem = new ZNSMemTable(options, icmp);
  mem->Ref();
  batch = WriteBatch();
  for (int i = 0; i < 1000; i++) {
    batch.Put(Slice(std::to_string(i)), Slice(std::to_string(i)));
  }
  mem->Write(woptions, &batch);
  ASSERT_TRUE(mem->GetInternalSize() > info.lba_size);
  SSZoneMetaData meta_border;
  s = dev.ss_manager->FlushMemTable(mem, &meta_border);
  ASSERT_TRUE(s.ok());
  lbas = (mem->GetInternalSize() + info.lba_size - 1) / info.lba_size;
  mem->Unref();
  ASSERT_TRUE(meta_border.lba <= info.zone_size * (begin + 1) - 1);
  ASSERT_EQ(meta_border.numbers, 1000);
  ASSERT_TRUE(meta_border.lba_count <= lbas);
  ASSERT_TRUE(meta_border.smallest.user_key() ==
              InternalKey(Slice("0"), 0, kTypeValue).user_key());
  ASSERT_TRUE(meta_border.largest.user_key() ==
              InternalKey(Slice("999"), 0, kTypeValue).user_key());
  for (int i = 0; i < 1000; i++) {
    value.clear();
    s = dev.ss_manager->Get(0, Slice(std::to_string(i)), &value, &meta_border,
                            &stata);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(stata == EntryStatus::found);
    ASSERT_TRUE(Slice(std::to_string(i)) == Slice(value));
  }
  // fill
  for (uint64_t i = ZnsSSTableManagerInternal::GetWriteHead(sstable);
       i < ZnsSSTableManagerInternal::GetMaxZoneHead(sstable); i++) {
    WasteLba(&dev);
  }
  // check what happens when no space.
  {
    mem = new ZNSMemTable(options, icmp);
    mem->Ref();
    batch = WriteBatch();
    batch.Put(Slice("break"), Slice("down"));
    mem->Write(woptions, &batch);
    s = dev.ss_manager->FlushMemTable(mem, &meta);
    ASSERT_FALSE(s.ok());
    // try to eat parts of the tail
    s = ZnsSSTableManagerInternal::ConsumeTail(sstable, begin * info.zone_size,
                                               begin * info.zone_size + 3);
    ASSERT_TRUE(s.ok());
    s = ZnsSSTableManagerInternal::ConsumeTail(
        sstable, begin * info.zone_size + 3, (begin + 1) * info.zone_size);
    ASSERT_TRUE(s.ok());
    s = ZnsSSTableManagerInternal::ConsumeTail(
        sstable, (begin + 1) * info.zone_size, info.zone_size * end);
    ASSERT_TRUE(s.ok());
  }
  TearDownDev(&dev);
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
