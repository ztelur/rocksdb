// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <stdint.h>
#include <string>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// How much perf stats to collect. Affects perf_context and iostats_context.
enum PerfLevel : unsigned char {
  kUninitialized = 0,             // unknown setting // 什么也不监控，一般不实用这个选项
  kDisable = 1,                   // disable perf stats  关闭profling功能
  kEnableCount = 2,               // enable only count stats 仅开启count计数的profling 功能
  kEnableTimeExceptForMutex = 3,  // Other than count stats, also enable time 仅开启time profiling功能，我们耗时方面分析就直接开启这个级别。
                                  // stats except for mutexes
  // Other than time, also measure CPU time counters. Still don't measure
  // time (neither wall time nor CPU time) for mutexes.
  kEnableTimeAndCPUTimeExceptForMutex = 4, // 仅开启cpu 耗时的统计
  kEnableTime = 5,  // enable count and time stats // 开启所有stats的统计，包括耗时和计数
  kOutOfBounds = 6  // N.B. Must always be the last value!
};

// set the perf stats level for current thread
void SetPerfLevel(PerfLevel level);

// get current perf stats level for current thread
PerfLevel GetPerfLevel();

}  // namespace ROCKSDB_NAMESPACE
