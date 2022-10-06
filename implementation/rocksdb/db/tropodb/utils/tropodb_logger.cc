/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
// ^ This code is inspired by the SPDK logger

#include "db/tropodb/utils/tropodb_logger.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

namespace ROCKSDB_NAMESPACE {
// Globals
TropoDBLogLevel g_TropoDB_log_level = TropoDBLogLevel::TROPO_DISABLED;

void SetTropoDBLogLevel(const TropoDBLogLevel log_level) {
  g_TropoDB_log_level = log_level;
}

TropoDBLogLevel GetTropoDBLogLevel() { return g_TropoDB_log_level; }

static void TropoDBVlog(const TropoDBLogLevel level, const char *format,
                        va_list ap) {
  if (level < g_TropoDB_log_level) {
    return;
  }
  vprintf(format, ap);
}

void TropoDBLog(const TropoDBLogLevel level, const char *format, ...) {
  va_list ap;
  va_start(ap, format);
  TropoDBVlog(level, format, ap);
  va_end(ap);
}
}  // namespace ROCKSDB_NAMESPACE
