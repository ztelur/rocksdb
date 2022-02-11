//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <stdint.h>
#include "file/writable_file_writer.h"
#include "rocksdb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {
namespace log {

Writer::Writer(std::unique_ptr<WritableFileWriter>&& dest, uint64_t log_number,
               bool recycle_log_files, bool manual_flush)
    : dest_(std::move(dest)),
      block_offset_(0),
      log_number_(log_number),
      recycle_log_files_(recycle_log_files),
      manual_flush_(manual_flush) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc_[i] = crc32c::Value(&t, 1);
  }
}

Writer::~Writer() {
  if (dest_) {
    WriteBuffer().PermitUncheckedError();
  }
}

IOStatus Writer::WriteBuffer() { return dest_->Flush(); }

IOStatus Writer::Close() {
  IOStatus s;
  if (dest_) {
    s = dest_->Close();
    dest_.reset();
  }
  return s;
}
// 将 wal entry 写入到文件中
IOStatus Writer::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();
  size_t left = slice.size();

  // Header size varies depending on whether we are recycling or not.
  // 要根据是否重复使用 log files 来判断不同的 header size
  const int header_size =
      recycle_log_files_ ? kRecyclableHeaderSize : kHeaderSize;

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  IOStatus s;
  bool begin = true;
  do {
    // 计算 block 剩下的大小
    const int64_t leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);
    // 如果小于 header_size, 则需要切换到一个新的 block 上。
    if (leftover < header_size) {
      // Switch to a new block
      // 如果此时还有剩余的，则进行数据填充。因为一定是小于 header_size 的，所以直接填充
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize and
        // kRecyclableHeaderSize being <= 11)
        // 写入填充的字段
        assert(header_size <= 11);
        s = dest_->Append(Slice("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
                                static_cast<size_t>(leftover)));
        if (!s.ok()) {
          break;
        }
      }
      // 重置
      block_offset_ = 0;
    }

    // Invariant: we never leave < header_size bytes in a block.
    assert(static_cast<int64_t>(kBlockSize - block_offset_) >= header_size);
    // 计算除了写入 header_size 外，还剩下多少空间
    const size_t avail = kBlockSize - block_offset_ - header_size;
    // 判断是否足够写入空间
    const size_t fragment_length = (left < avail) ? left : avail;
    // 计算 wal record type
    RecordType type;
    const bool end = (left == fragment_length);
    // 计算类型，是full，还是first，还是last，还是 midle
    if (begin && end) {
      // 应该是 FULL 类型
      type = recycle_log_files_ ? kRecyclableFullType : kFullType;
    } else if (begin) {
      // 只有 begin 没有 end 则是 FIRST 类型
      type = recycle_log_files_ ? kRecyclableFirstType : kFirstType;
    } else if (end) {
      // 只有 end 则是 Last
      type = recycle_log_files_ ? kRecyclableLastType : kLastType;
    } else {
      // 否则就是中间
      type = recycle_log_files_ ? kRecyclableMiddleType : kMiddleType;
    }
    // 进行写入
    s = EmitPhysicalRecord(type, ptr, fragment_length);
    ptr += fragment_length;
    // 减少 left 剩下的未写入的数据
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);

  if (s.ok()) {
    // 如果是true，则并不每次写入都flush，而是以来上层的 WriteBuffer
    if (!manual_flush_) {
      s = dest_->Flush();
    }
  }

  return s;
}

bool Writer::TEST_BufferIsEmpty() { return dest_->TEST_BufferIsEmpty(); }

IOStatus Writer::EmitPhysicalRecord(RecordType t, const char* ptr, size_t n) {
  assert(n <= 0xffff);  // Must fit in two bytes

  size_t header_size;
  char buf[kRecyclableHeaderSize];
  // CRC 4B SIZE 2B Type 1B Payload
  // Format the header 构造 record header
  buf[4] = static_cast<char>(n & 0xff);
  buf[5] = static_cast<char>(n >> 8);
  buf[6] = static_cast<char>(t);
  // 把类型的 crc 缓存起来，不用一直计算
  uint32_t crc = type_crc_[t];
  if (t < kRecyclableFullType) {
    // Legacy record format
    assert(block_offset_ + kHeaderSize + n <= kBlockSize);
    header_size = kHeaderSize;
  } else {
    // Recyclable record format
    assert(block_offset_ + kRecyclableHeaderSize + n <= kBlockSize);
    header_size = kRecyclableHeaderSize;

    // Only encode low 32-bits of the 64-bit log number.  This means
    // we will fail to detect an old record if we recycled a log from
    // ~4 billion logs ago, but that is effectively impossible, and
    // even if it were we'dbe far more likely to see a false positive
    // on the 32-bit CRC.
    EncodeFixed32(buf + 7, static_cast<uint32_t>(log_number_));
    crc = crc32c::Extend(crc, buf + 7, 4);
  }

  // Compute the crc of the record type and the payload.
  uint32_t payload_crc = crc32c::Value(ptr, n);
  // 计算 crc
  crc = crc32c::Crc32cCombine(crc, payload_crc, n);
  crc = crc32c::Mask(crc);  // Adjust for storage
  TEST_SYNC_POINT_CALLBACK("LogWriter::EmitPhysicalRecord:BeforeEncodeChecksum",
                           &crc);
  // 写入 crc 到 buf
  EncodeFixed32(buf, crc);

  // Write the header and the payload
  // 写入 header
  IOStatus s = dest_->Append(Slice(buf, header_size));
  if (s.ok()) {
    // 写入 payload
    s = dest_->Append(Slice(ptr, n), payload_crc);
  }
  block_offset_ += header_size + n;
  return s;
}

}  // namespace log
}  // namespace ROCKSDB_NAMESPACE
