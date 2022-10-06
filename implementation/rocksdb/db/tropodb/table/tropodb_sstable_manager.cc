#include "db/tropodb/table/tropodb_sstable_manager.h"

#include <iomanip>
#include <iostream>

#include "db/tropodb/tropodb_config.h"
#include "db/tropodb/io/szd_port.h"
#include "db/tropodb/memtable/tropodb_memtable.h"
#include "db/tropodb/table/iterators/sstable_ln_iterator.h"
#include "db/tropodb/table/tropodb_l0_sstable.h"
#include "db/tropodb/table/tropodb_ln_sstable.h"
#include "db/tropodb/table/tropodb_sstable.h"
#include "db/tropodb/table/tropodb_zonemetadata.h"
#include "db/tropodb/utils/tropodb_logger.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "table/internal_iterator.h"

namespace ROCKSDB_NAMESPACE {
TropoSSTableManager::TropoSSTableManager(SZD::SZDChannelFactory* channel_factory,
                                     const SZD::DeviceInfo& info,
                                     const RangeArray& ranges)
    : zone_cap_(info.zone_cap),
      lba_size_(info.lba_size),
      ranges_(ranges),
      channel_factory_(channel_factory) {
  assert(channel_factory_ != nullptr);
  channel_factory_->Ref();
  // Create tables
  for (size_t i = 0; i < TropoDBConfig::lower_concurrency; i++) {
    sstable_level_[i] = new TropoL0SSTable(channel_factory_, info,
                                         ranges[i].first, ranges[i].second);
  }
  sstable_level_[TropoDBConfig::lower_concurrency] = new TropoLNSSTable(
      channel_factory_, info, ranges[TropoDBConfig::lower_concurrency].first,
      ranges[TropoDBConfig::lower_concurrency].second);

  // Move from zone regions to block ranges
  for (size_t i = 0; i < TropoDBConfig::lower_concurrency; i++) {
    ranges_[i] = std::make_pair(ranges_[i].first * info.zone_cap,
                                ranges_[i].second * info.zone_cap);
  }
  ranges_[TropoDBConfig::lower_concurrency] = std::make_pair(
      ranges_[TropoDBConfig::lower_concurrency].first * info.zone_cap,
      ranges_[TropoDBConfig::lower_concurrency].second * info.zone_cap);
}

TropoSSTableManager::~TropoSSTableManager() {
  TROPO_LOG_DEBUG("Deleting SSTable manager\n");
  for (size_t i = 0; i < 1 + TropoDBConfig::lower_concurrency; i++) {
    if (sstable_level_[i] != nullptr) delete sstable_level_[i];
  }
  channel_factory_->Unref();
  channel_factory_ = nullptr;
}

Status TropoSSTableManager::Get(const uint8_t level,
                              const InternalKeyComparator& icmp,
                              const Slice& key_ptr, std::string* value_ptr,
                              const SSZoneMetaData& meta,
                              EntryStatus* status) const {
  assert(level < TropoDBConfig::level_count);
  if (level == 0) {
    return sstable_level_[meta.L0.log_number]->Get(icmp, key_ptr, value_ptr,
                                                   meta, status);
  } else {
    return sstable_level_[TropoDBConfig::lower_concurrency]->Get(
        icmp, key_ptr, value_ptr, meta, status);
  }
}

Status TropoSSTableManager::ReadSSTable(const uint8_t level, Slice* sstable,
                                      const SSZoneMetaData& meta) const {
  assert(level < TropoDBConfig::level_count);
  if (level == 0) {
    return sstable_level_[meta.L0.log_number]->ReadSSTable(sstable, meta);
  } else {
    return sstable_level_[TropoDBConfig::lower_concurrency]->ReadSSTable(sstable,
                                                                     meta);
  }
}

Iterator* TropoSSTableManager::GetLNIterator(const Slice& file_value,
                                           const Comparator* cmp) {
  std::pair<SSZoneMetaData, uint8_t> decoded_iterator =
      LNZoneIterator::DecodeLNIterator(file_value);
  return NewIterator(decoded_iterator.second, decoded_iterator.first, cmp);
}

Iterator* TropoSSTableManager::NewIterator(const uint8_t level,
                                         const SSZoneMetaData& meta,
                                         const Comparator* cmp) const {
  assert(level < TropoDBConfig::level_count);
  if (level == 0) {
    return sstable_level_[meta.L0.log_number]->NewIterator(meta, cmp);
  } else {
    return sstable_level_[TropoDBConfig::lower_concurrency]->NewIterator(meta, cmp);
  }
}

Status TropoSSTableManager::RecoverL0() {
  Status s = Status::OK();
  // Recover L0
  for (size_t i = 0; i < TropoDBConfig::lower_concurrency; i++) {
    s = static_cast<TropoL0SSTable*>(sstable_level_[i])->Recover();
    if (!s.ok()) {
      TROPO_LOG_ERROR("ERROR: SSTable recovery: Can not recover L0-%lu\n", i);
      return s;
    }
  }
  return s;
}

Status TropoSSTableManager::RecoverLN(const std::string& recovery_data) {
  if (recovery_data == "") {
    return Status::OK();
  }
  Status s =
      static_cast<TropoLNSSTable*>(sstable_level_[TropoDBConfig::lower_concurrency])
          ->Recover(recovery_data);
  if (!s.ok()) {
    TROPO_LOG_ERROR("ERROR: SSTable recovery: Can not recover LN\n");
  }
  return s;
}

Status TropoSSTableManager::Recover(const std::string& recovery_data) {
  Status s = RecoverL0();
  // Lazy failing: if L0 fails, so does LN
  s = s.ok() ? RecoverLN(recovery_data) : s;
  return s;
}

std::string TropoSSTableManager::GetRecoveryData() {
  TropoLNSSTable* table =
      static_cast<TropoLNSSTable*>(sstable_level_[TropoDBConfig::lower_concurrency]);
  return table->Encode();
}

TropoSSTableBuilder* TropoSSTableManager::NewTropoSSTableBuilder(
    const uint8_t level, SSZoneMetaData* meta) const {
  assert(level < TropoDBConfig::level_count);
  if (level == 0) {
    return sstable_level_[meta->L0.log_number]->NewBuilder(meta);
  } else if (level == 1) {
    return sstable_level_[TropoDBConfig::lower_concurrency]->NewBuilder(meta);
  } else {
    return static_cast<TropoLNSSTable*>(
               sstable_level_[TropoDBConfig::lower_concurrency])
        ->NewLNBuilder(meta);
  }
}

Status TropoSSTableManager::CopySSTable(const uint8_t level1,
                                      const uint8_t level2,
                                      const SSZoneMetaData& meta,
                                      SSZoneMetaData* new_meta) const {
  Status s = Status::OK();
  // Lazy copy. We do not have to rewrite, all tables are already in LN.
  if (level1 != 0) {
    *new_meta = SSZoneMetaData::copy(meta);
    return s;
  } else {
    // Read and copy to LN
    Slice original;
    s = ReadSSTable(level1, &original, meta);
    if (!s.ok() || original.size() == 0) {
      TROPO_LOG_ERROR("ERROR: SSTable in L0 can not be read or is empty");
      // Potential leaks need to be solved
      if (original.data()) {
        delete[] original.data();
      }
      return s;
    }
    *new_meta = SSZoneMetaData::copy(meta);

    s = sstable_level_[level2]->WriteSSTable(original, new_meta);
    delete[] original.data();
    return s;
  }
}

double TropoSSTableManager::GetFractionFilled(const uint8_t level) const {
  assert(level < TropoDBConfig::level_count);
  uint64_t space_available = 0;
  uint64_t total_space = 0;
  if (level == 0) {
    for (size_t i = 0; i < TropoDBConfig::lower_concurrency; i++) {
      space_available += sstable_level_[i]->SpaceAvailable() / lba_size_;
      total_space += ranges_[i].second - ranges_[i].first;
    }
  } else {
    space_available =
        sstable_level_[TropoDBConfig::lower_concurrency]->SpaceAvailable() /
        lba_size_;
    total_space = ranges_[TropoDBConfig::lower_concurrency].second -
                  ranges_[TropoDBConfig::lower_concurrency].first;
  }
  return (double)(total_space - space_available) / (double)total_space;
}

bool TropoSSTableManager::EnoughSpaceAvailable(const uint8_t level,
                                             const Slice& slice) const {
  assert(level < TropoDBConfig::level_count);
  if (level == 0) {
    // TODO: Not used for L0 so not tested
    for (size_t i = 0; i < TropoDBConfig::lower_concurrency; i++) {
      if (!sstable_level_[i]->EnoughSpaceAvailable(slice)) {
        return false;
      }
    }
    return true;
  } else {
    return sstable_level_[TropoDBConfig::lower_concurrency]->EnoughSpaceAvailable(
        slice);
  }
}

TropoL0SSTable* TropoSSTableManager::GetL0SSTableLog(
    uint8_t parallel_number) const {
  assert(parallel_number < TropoDBConfig::lower_concurrency);
  return static_cast<TropoL0SSTable*>(sstable_level_[parallel_number]);
}

Status TropoSSTableManager::FlushMemTable(TropoMemtable* mem,
                                        std::vector<SSZoneMetaData>& metas,
                                        uint8_t parallel_number) const {
  assert(parallel_number < TropoDBConfig::lower_concurrency);
  return GetL0SSTableLog(parallel_number)
      ->FlushMemTable(mem, metas, parallel_number);
}

Status TropoSSTableManager::DeleteL0Table(
    const std::vector<SSZoneMetaData*>& metas_to_delete,
    std::vector<SSZoneMetaData*>& remaining_metas) const {
  Status s = Status::OK();
  // Nothing to distribute
  if (TropoDBConfig::lower_concurrency == 1) {
    s = static_cast<TropoL0SSTable*>(sstable_level_[0])
            ->TryInvalidateSSZones(metas_to_delete, remaining_metas);
    if (!s.ok()) {
      TROPO_LOG_ERROR("ERROR: Resetting SSTables from L0-0 log\n");
    }
    return s;
  }
  // Delete for each individual L0 log, distribute and diligate
  for (size_t i = 0; i < TropoDBConfig::lower_concurrency; i++) {
    std::vector<SSZoneMetaData*> metas_for_log;
    for (auto& m : metas_to_delete) {
      if (m->L0.log_number == i) {
        metas_for_log.push_back(m);
      }
    }
    // Nothing to do
    if (metas_for_log.size() == 0) {
      continue;
    }
    s = static_cast<TropoL0SSTable*>(sstable_level_[i])
            ->TryInvalidateSSZones(metas_for_log, remaining_metas);
    if (!s.ok()) {
      TROPO_LOG_ERROR("ERROR: Resetting SSTables from L0 log %lu\n", i);
      return s;
    }
  }
  return s;
}

double TropoSSTableManager::GetFractionFilledL0(
    const uint8_t parallel_number) const {
  assert(level < TropoDBConfig::level_count);
  uint64_t space_available =
      sstable_level_[parallel_number]->SpaceAvailable() / lba_size_;
  uint64_t total_space =
      ranges_[parallel_number].second - ranges_[parallel_number].first;

  return (double)(total_space - space_available) / (double)total_space;
}

uint64_t TropoSSTableManager::SpaceRemainingInBytesL0(
    uint8_t parallel_number) const {
  assert(parallel_number < TropoDBConfig::lower_concurrency);
  return sstable_level_[parallel_number]->SpaceAvailable();
}

// TODO: investigate
uint64_t TropoSSTableManager::SpaceRemainingL0(uint8_t parallel_number) const {
  assert(level < TropoDBConfig::level_count);
  return SpaceRemainingInBytesL0(parallel_number) / lba_size_;
}

Status TropoSSTableManager::DeleteLNTable(const uint8_t level,
                                        const SSZoneMetaData& meta) const {
  if (level == 0) {
    TROPO_LOG_ERROR("Error: %s : Invalid level for LN delete\n", __func__);
    return Status::InvalidArgument();
  }
  return sstable_level_[TropoDBConfig::lower_concurrency]->InvalidateSSZone(meta);
}

uint64_t TropoSSTableManager::SpaceRemainingInBytesLN() const {
  return sstable_level_[TropoDBConfig::lower_concurrency]->SpaceAvailable();
}

uint64_t TropoSSTableManager::SpaceRemainingLN() const {
  return SpaceRemainingInBytesLN() / lba_size_;
}

uint64_t TropoSSTableManager::GetBytesInLevel(
    const std::vector<SSZoneMetaData*>& metas) {
  // Bytes is equal to all used lbas and the lba size
  uint64_t total = 0;
  for (const auto& meta : metas) {
    total += meta->lba_count * lba_size_;
  }
  return total;
}

std::vector<TropoDiagnostics> TropoSSTableManager::IODiagnostics() {
  std::vector<TropoDiagnostics> diags;
  // L0 diagnostics
  for (size_t i = 0; i < TropoDBConfig::lower_concurrency; i++) {
    TropoDiagnostics diag = sstable_level_[i]->GetDiagnostics();
    diag.name_ = "L0-" + std::to_string(i);
    diags.push_back(diag);
  }
  // LN diagnostics
  {
    TropoDiagnostics diag =
        sstable_level_[TropoDBConfig::lower_concurrency]->GetDiagnostics();
    diag.name_ = "LN";
    diags.push_back(diag);
  }
  return diags;
}

std::string TropoSSTableManager::LayoutDivisionString() {
  std::ostringstream div;
  for (size_t i = 0; i < TropoDBConfig::lower_concurrency; i++) {
    div << std::left << ("L0-" + std::to_string(i)) << std::setw(13) << ""
        << std::right << std::setw(25) << (ranges_[i].first / zone_cap_)
        << std::setw(25) << (ranges_[i].second / zone_cap_) << "\n";
  }
  div << std::left << "LN" << std::setw(13) << "" << std::right << std::setw(25)
      << (ranges_[TropoDBConfig::lower_concurrency].first / zone_cap_)
      << std::setw(25)
      << (ranges_[TropoDBConfig::lower_concurrency].second / zone_cap_) << "\n";
  return div.str();
}

size_t TropoSSTableManager::FindSSTableIndex(
    const Comparator* cmp, const std::vector<SSZoneMetaData*>& ss,
    const Slice& key) {
  // binary search for index
  size_t left = 0;
  size_t right = ss.size();
  while (left < right) {
    size_t mid = (left + right) / 2;
    const SSZoneMetaData* m = ss[mid];
    if (cmp->Compare(m->largest.user_key(), ExtractUserKey(key)) < 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  return right;
}

std::optional<TropoSSTableManager*> TropoSSTableManager::NewZNSSTableManager(
    SZD::SZDChannelFactory* channel_factory, const SZD::DeviceInfo& info,
    const uint64_t min_zone, const uint64_t max_zone) {
  uint64_t num_zones = max_zone - min_zone;
  RangeArray ranges;
  // Validate
  if (min_zone > max_zone ||
      num_zones < TropoDBConfig::level_count * TropoDBConfig::min_ss_zone_count ||
      channel_factory == nullptr) {
    TROPO_LOG_ERROR(
        "ERROR: Creating SSTable division: not enough zones assigned "
        "%lu\\%lu\n",
        num_zones, TropoDBConfig::level_count * TropoDBConfig::min_ss_zone_count);
    return {};
  }
  // Distribute for L0
  uint64_t zone_head = min_zone;
  uint64_t zone_step = TropoDBConfig::L0_zones;
  zone_step = zone_step < TropoDBConfig::min_ss_zone_count
                  ? TropoDBConfig::min_ss_zone_count
                  : zone_step;
  zone_step /= TropoDBConfig::lower_concurrency;
  for (size_t i = 0; i < TropoDBConfig::lower_concurrency; i++) {
    ranges[i] = std::make_pair(zone_head, zone_head + zone_step);
    zone_head += zone_step;
  }
  // LN will get the remainder
  zone_step = max_zone - zone_head;
  ranges[TropoDBConfig::lower_concurrency] =
      std::make_pair(zone_head, zone_head + zone_step);
  // Verify that no rounding errors occurred
  zone_head += zone_step;
  if (zone_head != max_zone) {
    TROPO_LOG_ERROR(
        "ERROR: Creating SSTable division: Rounding error %lu != %lu \n",
        zone_head, max_zone);
    return {};
  }
  // Now create
  return new TropoSSTableManager(channel_factory, info, ranges);
}

}  // namespace ROCKSDB_NAMESPACE
