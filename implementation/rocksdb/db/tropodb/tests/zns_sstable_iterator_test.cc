#include "db/tropodb/table/iterators/merging_iterator.h"
#include "db/tropodb/table/tropodb_sstable_manager.h"
#include "db/tropodb/tests/zns_test_utils.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {
class SSTableTest : public testing::Test {};

static std::vector<Slice> in_order_search(InternalKeyComparator ucmp, int begin,
                                          int end) {
  std::vector<Slice> slices;
  for (int i = begin; i < end; i++) {
    std::string str = std::to_string(i);
    char* c = new char[str.length() + 1];
    memcpy(c, str.c_str(), str.length() + 1);
    Slice s = Slice(c, str.length() + 1);
    slices.push_back(s);
  }

  const Comparator* icmp = ucmp.user_comparator();
  std::sort(slices.begin(), slices.end(),
            [icmp](const Slice& s1, const Slice& s2) -> bool {
              return icmp->Compare(s1, s2) < 0;
            });
  return slices;
}

// no idea what on earth memtable is doing with Slices that length decreases,
// but this workaround is needed.
void safe_check(Slice l, Slice r) {
  ASSERT_TRUE(l.size() <= r.size());
  ASSERT_TRUE(memcmp(l.data(), r.data(), l.size()) == 0);
}

TEST_F(SSTableTest, ITER) {
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
  TropoMemtable* mem = new TropoMemtable(options, icmp);
  SSZoneMetaData meta;
  // write 1000 pairs (enough to cause multiple lbas for 4kb lbasize)
  mem->Ref();
  WriteBatch batch = WriteBatch();
  for (int i = 0; i < 1000; i++) {
    batch.Put(Slice(std::to_string(i)), Slice(std::to_string(i)));
  }
  mem->Write(woptions, &batch);
  ASSERT_TRUE(mem->GetInternalSize() > info.lba_size);
  dev.ss_manager->FlushMemTable(mem, &meta);
  // Iterator test
  Iterator* it = dev.ss_manager->NewIterator(0, &meta);
  it->SeekToFirst();
  ASSERT_TRUE(it->Valid());
  ASSERT_TRUE(it->value() == Slice("0"));
  std::vector<Slice> slices_ordered = in_order_search(icmp, 0, 1000);
  for (int i = 1; i < 200; i++) {
    ASSERT_TRUE(it->Valid());
    it->Next();
    safe_check(it->key(), slices_ordered[i]);
    safe_check(it->value(), slices_ordered[i]);
  }
  it->Seek(Slice("300"));
  ASSERT_TRUE(it->value() == Slice("300"));
  it->Seek(Slice("257"));
  ASSERT_TRUE(it->value() == Slice("257"));
  it->Prev();
  safe_check(it->value(), slices_ordered[175]);
  it->Next();
  safe_check(it->value(), slices_ordered[176]);
  it->Prev();
  it->Prev();
  it->Prev();
  it->Next();
  safe_check(it->value(), slices_ordered[174]);
  for (int i = 0; i < 826; i++) {
    ASSERT_TRUE(it->Valid());
    it->Next();
  }
  ASSERT_TRUE(!it->Valid());
  it->Prev();
  safe_check(it->value(), slices_ordered[999]);
  for (int i = 0; i < 999; i++) {
    ASSERT_TRUE(it->Valid());
    it->Prev();
  }
  ASSERT_TRUE(!it->Valid());
  it->SeekToFirst();
  safe_check(it->value(), slices_ordered[0]);
  it->SeekToLast();
  safe_check(it->value(), slices_ordered[999]);
  mem->Unref();

  // merge iterators
  mem = new TropoMemtable(options, icmp);
  mem->Ref();
  batch = WriteBatch();
  for (int i = 0; i < 1000; i += 2) {
    batch.Put(Slice(std::to_string(i)), Slice(std::to_string(i)));
  }
  mem->Write(woptions, &batch);
  SSZoneMetaData meta_m1;
  dev.ss_manager->FlushMemTable(mem, &meta_m1);
  mem->Unref();
  mem = new TropoMemtable(options, icmp);
  mem->Ref();
  batch = WriteBatch();
  for (int i = 1; i < 1000; i += 2) {
    batch.Put(Slice(std::to_string(i)), Slice(std::to_string(i)));
  }
  mem->Write(woptions, &batch);
  SSZoneMetaData meta_m2;
  dev.ss_manager->FlushMemTable(mem, &meta_m2);
  mem->Unref();
  Iterator* m1 = dev.ss_manager->NewIterator(0, &meta_m1);
  Iterator* m2 = dev.ss_manager->NewIterator(0, &meta_m2);
  Iterator** m12 = new Iterator* [2] { m1, m2 };
  Iterator* merged = NewMergingIterator(icmp.user_comparator(), m12, 2);
  merged->SeekToFirst();
  ASSERT_TRUE(merged->Valid());
  for (int i = 1; i <= 1000; i++) {
    safe_check(merged->key(), slices_ordered[i - 1]);
    merged->Next();
  }
  ASSERT_TRUE(!merged->Valid());
  TearDownDev(&dev);
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
