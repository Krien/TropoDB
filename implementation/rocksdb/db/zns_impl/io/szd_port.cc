#include "db/zns_impl/io/szd_port.h"

#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
Status FromStatus(SZD::SZDStatus s) {
  if (s == SZD::SZDStatus::Success) {
    return Status::OK();
  }
  if (s == SZD::SZDStatus::InvalidArguments) {
    return Status::InvalidArgument();
  }
  if (s == SZD::SZDStatus::IOError) {
    return Status::IOError();
  }
  return Status::Corruption("Unknown error");
}

Status FromStatusMsg(SZD::SZDStatusDetailed s) {
  if (s.sc == SZD::SZDStatus::Success) {
    return Status::OK();
  } else if (s.sc == SZD::SZDStatus::InvalidArguments) {
    return Status::InvalidArgument(s.msg);
  } else if (s.sc == SZD::SZDStatus::IOError) {
    return Status::IOError(s.msg);
  } else {
    return Status::Corruption(s.msg);
  }
}

}  // namespace ROCKSDB_NAMESPACE