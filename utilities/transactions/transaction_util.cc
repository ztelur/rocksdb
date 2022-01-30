//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "utilities/transactions/transaction_util.h"

#include <cinttypes>
#include <string>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "util/cast_util.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
// 检查是否冲突
Status TransactionUtil::CheckKeyForConflicts(
    DBImpl* db_impl, ColumnFamilyHandle* column_family, const std::string& key,
    SequenceNumber snap_seq, const std::string* const read_ts, bool cache_only,
    ReadCallback* snap_checker, SequenceNumber min_uncommitted) {
  Status result;

  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  // 从 cfhandler 中 拿到 cf data
  auto cfd = cfh->cfd();
  // 从version 系统中 取一个local_version GetThreadLocalSuperVersion
  SuperVersion* sv = db_impl->GetAndRefSuperVersion(cfd);

  if (sv == nullptr) {
    result = Status::InvalidArgument("Could not access column family " +
                                     cfh->GetName());
  }

  if (result.ok()) {
    SequenceNumber earliest_seq =
        db_impl->GetEarliestMemTableSequenceNumber(sv, true);
    // 进行 CheckKey 进行检查
    result = CheckKey(db_impl, sv, earliest_seq, snap_seq, key, read_ts,
                      cache_only, snap_checker, min_uncommitted);
    // 归还
    db_impl->ReturnAndCleanupSuperVersion(cfd, sv);
  }

  return result;
}
// 悲观事务模式下，检查 key 是否冲突
Status TransactionUtil::CheckKey(DBImpl* db_impl, SuperVersion* sv,
                                 SequenceNumber earliest_seq,
                                 SequenceNumber snap_seq,
                                 const std::string& key,
                                 const std::string* const read_ts,
                                 bool cache_only, ReadCallback* snap_checker,
                                 SequenceNumber min_uncommitted) {
  // When `min_uncommitted` is provided, keys are not always committed
  // in sequence number order, and `snap_checker` is used to check whether
  // specific sequence number is in the database is visible to the transaction.
  // So `snap_checker` must be provided.
  assert(min_uncommitted == kMaxSequenceNumber || snap_checker != nullptr);

  Status result;
  bool need_to_read_sst = false;

  // Since it would be too slow to check the SST files, we will only use
  // the memtables to check whether there have been any recent writes
  // to this key after it was accessed in this transaction.  But if the
  // Memtables do not contain a long enough history, we must fail the
  // transaction.
  // 因为检查 sst file 太慢了，所以 可以用 memtable 来检查一下从事务开始，是否有write
  // 但是，如果 memtable 没有包含足够的数据，我们就只有fail 事务
  if (earliest_seq == kMaxSequenceNumber) {
    // The age of this memtable is unknown.  Cannot rely on it to check
    // for recent writes.  This error shouldn't happen often in practice as
    // the Memtable should have a valid earliest sequence number except in some
    // corner cases (such as error cases during recovery).
    need_to_read_sst = true;

    if (cache_only) {
      result = Status::TryAgain(
          "Transaction could not check for conflicts as the MemTable does not "
          "contain a long enough history to check write at SequenceNumber: ",
          ToString(snap_seq));
    }
  } else if (snap_seq < earliest_seq || min_uncommitted <= earliest_seq) {
    // Use <= for min_uncommitted since earliest_seq is actually the largest sec
    // before this memtable was created
    need_to_read_sst = true;

    if (cache_only) {
      // The age of this memtable is too new to use to check for recent
      // writes.
      char msg[300];
      snprintf(msg, sizeof(msg),
               "Transaction could not check for conflicts for operation at "
               "SequenceNumber %" PRIu64
               " as the MemTable only contains changes newer than "
               "SequenceNumber %" PRIu64
               ".  Increasing the value of the "
               "max_write_buffer_size_to_maintain option could reduce the "
               "frequency "
               "of this error.",
               snap_seq, earliest_seq);
      result = Status::TryAgain(msg);
    }
  }

  if (result.ok()) {
    SequenceNumber seq = kMaxSequenceNumber;
    std::string timestamp;
    bool found_record_for_key = false;

    // When min_uncommitted == kMaxSequenceNumber, writes are committed in
    // sequence number order, so only keys larger than `snap_seq` can cause
    // conflict.
    // When min_uncommitted != kMaxSequenceNumber, keys lower than
    // min_uncommitted will not triggered conflicts, while keys larger than
    // min_uncommitted might create conflicts, so we need  to read them out
    // from the DB, and call callback to snap_checker to determine. So only
    // keys lower than min_uncommitted can be skipped.
    // 当 min_uncommitter = kMaxSequenceNumber 时，写操作会导致冲突，所以一般都是大于 snap_seq
    // 否则，
    SequenceNumber lower_bound_seq =
        (min_uncommitted == kMaxSequenceNumber) ? snap_seq : min_uncommitted;
    // 调用 GetLatestSequenceForKey
    Status s = db_impl->GetLatestSequenceForKey(
        sv, key, !need_to_read_sst, lower_bound_seq, &seq,
        !read_ts ? nullptr : &timestamp, &found_record_for_key,
        /*is_blob_index=*/nullptr);

    if (!(s.ok() || s.IsNotFound() || s.IsMergeInProgress())) {
      // 如果没有找到
      result = s;
    } else if (found_record_for_key) {
      // 如果找到了 key 对应的记录
      // 判断冲突，如果 snap_checker == null 则只要取到的 seq > snap_seq 就是表示冲突
      // 否则 交给 snap_checker 去判断
      bool write_conflict = snap_checker == nullptr
                                ? snap_seq < seq
                                : !snap_checker->IsVisible(seq);
      // Perform conflict checking based on timestamp if applicable.
      // 如果不冲突，在根据 timestamp 进行冲突检测
      if (!write_conflict && read_ts != nullptr) {
        ColumnFamilyData* cfd = sv->cfd;
        assert(cfd);
        const Comparator* const ucmp = cfd->user_comparator();
        assert(ucmp);
        assert(read_ts->size() == ucmp->timestamp_size());
        assert(read_ts->size() == timestamp.size());
        // Write conflict if *ts < timestamp.
        write_conflict = ucmp->CompareTimestamp(*read_ts, timestamp) < 0;
      }
      if (write_conflict) {
        result = Status::Busy();
      }
    }
  }

  return result;
}
/**
 * 乐观事务模式下commit时会进行校验，使用该函数进行检查
 * 前面 TryLock 过程中获取到的 LockTracker 信息 进行seq num的比较，和 PessimisticTransaction 事务中的
 * ValidSnapshot 做的事情一样，检测 tracker 中要提交的key 的 seq 是否比当前db 中最新的
 * 已经commit 的 seq小，用来确定这个事务执行 中间是否有外部事务 对当前key 的更新(完成 commit的操作)
 * @param db_impl
 * @param tracker
 * @param cache_only
 * @return
 */
Status TransactionUtil::CheckKeysForConflicts(DBImpl* db_impl,
                                              const LockTracker& tracker,
                                              bool cache_only) {
  Status result;

  std::unique_ptr<LockTracker::ColumnFamilyIterator> cf_it(
      tracker.GetColumnFamilyIterator());
  assert(cf_it != nullptr);
  while (cf_it->HasNext()) {
    ColumnFamilyId cf = cf_it->Next();

    SuperVersion* sv = db_impl->GetAndRefSuperVersion(cf);
    if (sv == nullptr) {
      result = Status::InvalidArgument("Could not access column family " +
                                       ToString(cf));
      break;
    }

    SequenceNumber earliest_seq =
        db_impl->GetEarliestMemTableSequenceNumber(sv, true);

    // For each of the keys in this transaction, check to see if someone has
    // written to this key since the start of the transaction.
    std::unique_ptr<LockTracker::KeyIterator> key_it(
        tracker.GetKeyIterator(cf));
    assert(key_it != nullptr);
    while (key_it->HasNext()) {
      const std::string& key = key_it->Next();
      PointLockStatus status = tracker.GetPointLockStatus(cf, key);
      const SequenceNumber key_seq = status.seq;

      // TODO: support timestamp-based conflict checking.
      // CheckKeysForConflicts() is currently used only by optimistic
      // transactions.
      result = CheckKey(db_impl, sv, earliest_seq, key_seq, key,
                        /*read_ts=*/nullptr, cache_only);
      if (!result.ok()) {
        break;
      }
    }

    db_impl->ReturnAndCleanupSuperVersion(cf, sv);

    if (!result.ok()) {
      break;
    }
  }

  return result;
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
