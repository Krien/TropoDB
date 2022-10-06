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
// ^ This code is based on the SPDK logger
#pragma once
#ifdef TROPODB_PLUGIN_ENABLED
#ifndef ZNS_LOGGER_H
#define ZNS_LOGGER_H

#include <rocksdb/rocksdb_namespace.h>

namespace ROCKSDB_NAMESPACE {

enum TropoDBLogLevel : unsigned char {
	TROPO_DEBUG_LEVEL = 0,
	TROPO_INFO_LEVEL  = 1,
	TROPO_PERF_LEVEL  = 2,
	TROPO_ERROR_LEVEL = 3,
	TROPO_DISABLED    = 4
};

extern void SetTropoDBLogLevel(const TropoDBLogLevel log_level);
extern TropoDBLogLevel GetTropoDBLogLevel();

#define TROPODB_INFO(...) \
	TropoDBLog(TropoDBLogLevel::TROPO_INFO_LEVEL, __VA_ARGS__)
#define TROPODB_PERF(...) \
	TropoDBLog(TropoDBLogLevel::TROPO_PERF_LEVEL, __VA_ARGS__)
#define TROPODB_ERROR(...) \
	TropoDBLog(TropoDBLogLevel::TROPO_ERROR_LEVEL, __VA_ARGS__)
#ifdef TROPICAL_DEBUG
#define TROPODB_DEBUG(...) \
	TropoDBLog(TropoDBLogLevel::TROPO_DEBUG_LEVEL, __VA_ARGS__)
#else 
#define TROPODB_DEBUG(...) do {} while (0)
#endif

extern void TropoDBLog(const TropoDBLogLevel level, const char *format, ...) __attribute__((__format__(__printf__, 2, 3)));
}
#endif
#endif
