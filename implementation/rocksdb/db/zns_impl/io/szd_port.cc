#include "db/zns_impl/io/szd_port.h"

#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
Status FromStatus(SZD::SZDStatus s) {
  switch (s) {
    case SZD::SZDStatus::Success:
      return Status::OK();
    case SZD::SZDStatus::InvalidArguments:
      return Status::InvalidArgument();
    case SZD::SZDStatus::IOError:
      return Status::IOError();
    default:
      return Status::Corruption("Unknown error");
  }
}

Status FromStatusMsg(SZD::SZDStatusDetailed s) {
  switch (s.sc) {
    case SZD::SZDStatus::Success:
      return Status::OK();
    case SZD::SZDStatus::InvalidArguments:
      return Status::InvalidArgument(s.msg);
    case SZD::SZDStatus::IOError:
      return Status::IOError(s.msg);
    default:
      return Status::Corruption(s.msg);
  }
}

}  // namespace ROCKSDB_NAMESPACE
