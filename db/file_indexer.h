//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <cstdint>
#include <functional>
#include <limits>
#include <vector>
#include "memory/arena.h"
#include "port/port.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

class Comparator;
struct FileMetaData;
struct FdWithKeyRange;
struct FileLevel;

// The file tree structure in Version is prebuilt and the range of each file
// is known. On Version::Get(), it uses binary search to find a potential file
// and then check if a target key can be found in the file by comparing the key
// to each file's smallest and largest key. The results of these comparisons
// can be reused beyond checking if a key falls into a file's range.
// With some pre-calculated knowledge, each key comparison that has been done
// can serve as a hint to narrow down further searches: if a key compared to
// be smaller than a file's smallest or largest, that comparison can be used
// to find out the right bound of next binary search. Similarly, if a key
// compared to be larger than a file's smallest or largest, it can be utilized
// to find out the left bound of next binary search.
// With these hints: it can greatly reduce the range of binary search,
// especially for bottom levels, given that one file most likely overlaps with
// only N files from level below (where N is max_bytes_for_level_multiplier).
// So on level L, we will only look at ~N files instead of N^L files on the
// naive approach.
/**
 * 这里RocksDB使用了一个技巧用来加快二分查找的速度，每次更新sst的时候，
 * RocksDB都会调用FileIndexer::UpdateIndex来更新这样的一个结构,这个结构就是FileIndexer，
 * 它主要是用来保存每一个level和level+1的key范围的关联信息，这样当我们在level查找的时候，
 * 如果没有查找到信息，那么我们将会迅速得到下一个level需要查找的文件范围.
 */
class FileIndexer {
 public:
  explicit FileIndexer(const Comparator* ucmp);

  size_t NumLevelIndex() const;

  size_t LevelIndexSize(size_t level) const;

  // Return a file index range in the next level to search for a key based on
  // smallest and largest key comparison for the current file specified by
  // level and file_index. When *left_index < *right_index, both index should
  // be valid and fit in the vector size.
  void GetNextLevelIndex(const size_t level, const size_t file_index,
                         const int cmp_smallest, const int cmp_largest,
                         int32_t* left_bound, int32_t* right_bound) const;

  void UpdateIndex(Arena* arena, const size_t num_levels,
                   std::vector<FileMetaData*>* const files);

  enum {
    // MSVC version 1800 still does not have constexpr for ::max()
    kLevelMaxIndex = ROCKSDB_NAMESPACE::port::kMaxInt32
  };

 private:
  size_t num_levels_;
  const Comparator* ucmp_;

  struct IndexUnit {
    IndexUnit()
      : smallest_lb(0), largest_lb(0), smallest_rb(-1), largest_rb(-1) {}
    //
    //
    //
    // During file search, a key is compared against smallest and largest
    // from a FileMetaData. It can have 3 possible outcomes:
    // (1) key is smaller than smallest, implying it is also smaller than
    //     larger. Precalculated index based on "smallest < smallest" can
    //     be used to provide right bound. 说明其最小值小于该indexer的最小值的indexer会是有帮助的，来提供右值
    // (2) key is in between smallest and largest.
    //     说明最小值大于该indexer最大值的indexer 可能用来提供left 边界
    //     Precalculated index based on "smallest > greatest" can be used to
    //     provide left bound.
    //     说明最大值小于该indexer最小值的indexer 可以用来提供 right 边界
    //     Precalculated index based on "largest < smallest" can be used to
    //     provide right bound.
    // (3) key is larger than largest, implying it is also larger than smallest.
    //     Precalculated index based on "largest > largest" can be used to
    //     provide left bound.
    //     说明最大值大于该indexer 最大值的的indexer可能用来提供left边界
    //
    // As a result, we will need to do:
    // 所以，
    // Compare smallest (<=) and largest keys from upper level file with
    // smallest key from lower level to get a right bound.
    // Compare smallest (>=) and largest keys from upper level file with
    // largest key from lower level to get a left bound.
    //
    // Example:
    //    level 1:              [50 - 60]
    //    level 2:        [1 - 40], [45 - 55], [58 - 80]
    // A key 35, compared to be less than 50, 3rd file on level 2 can be
    // skipped according to rule (1). LB = 0, RB = 1.
    // A key 53, sits in the middle 50 and 60. 1st file on level 2 can be
    // skipped according to rule (2)-a, but the 3rd file cannot be skipped
    // because 60 is greater than 58. LB = 1, RB = 2.
    // A key 70, compared to be larger than 60. 1st and 2nd file can be skipped
    // according to rule (3). LB = 2, RB = 2.
    //
    // Point to a left most file in a lower level that may contain a key,
    // which compares greater than smallest of a FileMetaData (upper level)
    int32_t smallest_lb;
    // Point to a left most file in a lower level that may contain a key,
    // which compares greater than largest of a FileMetaData (upper level)
    int32_t largest_lb;
    // Point to a right most file in a lower level that may contain a key,
    // which compares smaller than smallest of a FileMetaData (upper level)
    int32_t smallest_rb;
    // Point to a right most file in a lower level that may contain a key,
    // which compares smaller than largest of a FileMetaData (upper level)
    int32_t largest_rb;
  };

  // Data structure to store IndexUnits in a whole level
  struct IndexLevel {
    size_t num_index;
    IndexUnit* index_units;

    IndexLevel() : num_index(0), index_units(nullptr) {}
  };

  void CalculateLB(
      const std::vector<FileMetaData*>& upper_files,
      const std::vector<FileMetaData*>& lower_files, IndexLevel* index_level,
      std::function<int(const FileMetaData*, const FileMetaData*)> cmp_op,
      std::function<void(IndexUnit*, int32_t)> set_index);

  void CalculateRB(
      const std::vector<FileMetaData*>& upper_files,
      const std::vector<FileMetaData*>& lower_files, IndexLevel* index_level,
      std::function<int(const FileMetaData*, const FileMetaData*)> cmp_op,
      std::function<void(IndexUnit*, int32_t)> set_index);

  autovector<IndexLevel> next_level_index_;
  int32_t* level_rb_;
};

}  // namespace ROCKSDB_NAMESPACE
