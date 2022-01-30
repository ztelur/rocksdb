//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "utilities/transactions/optimistic_transaction.h"

#include <string>

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "util/cast_util.h"
#include "util/string_util.h"
#include "utilities/transactions/lock/point/point_lock_tracker.h"
#include "utilities/transactions/optimistic_transaction.h"
#include "utilities/transactions/optimistic_transaction_db_impl.h"
#include "utilities/transactions/transaction_util.h"

namespace ROCKSDB_NAMESPACE {

struct WriteOptions;

OptimisticTransaction::OptimisticTransaction(
    OptimisticTransactionDB* txn_db, const WriteOptions& write_options,
    const OptimisticTransactionOptions& txn_options)
    : TransactionBaseImpl(txn_db->GetBaseDB(), write_options,
                          PointLockTrackerFactory::Get()),
      txn_db_(txn_db) {
  Initialize(txn_options);
}

void OptimisticTransaction::Initialize(
    const OptimisticTransactionOptions& txn_options) {
  if (txn_options.set_snapshot) {
    SetSnapshot();
  }
}

void OptimisticTransaction::Reinitialize(
    OptimisticTransactionDB* txn_db, const WriteOptions& write_options,
    const OptimisticTransactionOptions& txn_options) {
  TransactionBaseImpl::Reinitialize(txn_db->GetBaseDB(), write_options);
  Initialize(txn_options);
}

OptimisticTransaction::~OptimisticTransaction() {}

void OptimisticTransaction::Clear() { TransactionBaseImpl::Clear(); }

Status OptimisticTransaction::Prepare() {
  return Status::InvalidArgument(
      "Two phase commit not supported for optimistic transactions.");
}
// 乐观模式下的提交操作
Status OptimisticTransaction::Commit() {
  auto txn_db_impl = static_cast_with_check<OptimisticTransactionDBImpl,
                                            OptimisticTransactionDB>(txn_db_);
  assert(txn_db_impl);
  // 不同的提交校验策略
  switch (txn_db_impl->GetValidatePolicy()) {
    case OccValidationPolicy::kValidateParallel:
      return CommitWithParallelValidate();
    case OccValidationPolicy::kValidateSerial:
      return CommitWithSerialValidate();
    default:
      assert(0);
  }
  // unreachable, just void compiler complain
  return Status::OK();
}
// 乐观模式下的一种提交检测
Status OptimisticTransaction::CommitWithSerialValidate() {
  // Set up callback which will call CheckTransactionForConflicts() to
  // check whether this transaction is safe to be committed.
  // 创建一个 callback 函数
  OptimisticTransactionCallback callback(this);
  /**
   * 直接拿到 basedb，调用 WriteWithCallback 函数，其中 callback 的实现是
   * OptimisticTransactionCallback::Callback，也就是 OptimisticTransaction::CheckTransactionForConflicts
   * 如果做完冲突检测，这一批事务的 tracker key都没有问题，则可以提交
   */
  DBImpl* db_impl = static_cast_with_check<DBImpl>(db_->GetRootDB());

  Status s = db_impl->WriteWithCallback(
      write_options_, GetWriteBatch()->GetWriteBatch(), &callback);

  if (s.ok()) {
    Clear();
  }

  return s;
}
// 乐观事务模式下的另外一种 commit 检测模式
// 冲突检测的内部实现就不过多介绍了，都是一样的，主要是这个策略如何调度 冲突检测的
/**
 * 在写入之前 会先对所有的 tracked_locks_ 中的多事务的 key 按照顺序进行加锁，然后统一进行 多事务的冲突检测，冲突检测通过之后直接调度写入。
 */
Status OptimisticTransaction::CommitWithParallelValidate() {
  auto txn_db_impl = static_cast_with_check<OptimisticTransactionDBImpl,
                                            OptimisticTransactionDB>(txn_db_);
  assert(txn_db_impl);
  DBImpl* db_impl = static_cast_with_check<DBImpl>(db_->GetRootDB());
  assert(db_impl);
  const size_t space = txn_db_impl->GetLockBucketsSize();
  std::set<size_t> lk_idxes;
  std::vector<std::unique_lock<std::mutex>> lks;
  std::unique_ptr<LockTracker::ColumnFamilyIterator> cf_it(
      tracked_locks_->GetColumnFamilyIterator());
  assert(cf_it != nullptr);
  while (cf_it->HasNext()) {
    ColumnFamilyId cf = cf_it->Next();
    std::unique_ptr<LockTracker::KeyIterator> key_it(
        tracked_locks_->GetKeyIterator(cf));
    assert(key_it != nullptr);
    while (key_it->HasNext()) {
      const std::string& key = key_it->Next();
      lk_idxes.insert(FastRange64(GetSliceNPHash64(key), space));
    }
  }
  // NOTE: in a single txn, all bucket-locks are taken in ascending order.
  // In this way, txns from different threads all obey this rule so that
  // deadlock can be avoided.
  for (auto v : lk_idxes) {
    // 将涉及的 key 进行加锁
    lks.emplace_back(txn_db_impl->LockBucket(v));
  }
  // 进行检查
  Status s = TransactionUtil::CheckKeysForConflicts(db_impl, *tracked_locks_,
                                                    true /* cache_only */);
  if (!s.ok()) {
    return s;
  }

  s = db_impl->Write(write_options_, GetWriteBatch()->GetWriteBatch());
  if (s.ok()) {
    Clear();
  }

  return s;
}

Status OptimisticTransaction::Rollback() {
  Clear();
  return Status::OK();
}

// Record this key so that we can check it for conflicts at commit time.
//
// 'exclusive' is unused for OptimisticTransaction.
// 乐观模式下尝试加锁 相比于 PessimisticTransaction 的主要差异就是 冲突检测的时机 是在 Commit 阶段才进行。
//它在 TryLock 的时候 会将当前 要更新的 key的信息添加到 LockTracker 中
Status OptimisticTransaction::TryLock(ColumnFamilyHandle* column_family,
                                      const Slice& key, bool read_only,
                                      bool exclusive, const bool do_validate,
                                      const bool assume_tracked) {
  // 这种场景下必须支持 assume_tracked
  assert(!assume_tracked);  // not supported
  (void)assume_tracked;
  if (!do_validate) {
    return Status::OK();
  }
  uint32_t cfh_id = GetColumnFamilyID(column_family);

  SetSnapshotIfNeeded();

  SequenceNumber seq;
  if (snapshot_) {
    seq = snapshot_->GetSequenceNumber();
  } else {
    seq = db_->GetLatestSequenceNumber();
  }

  std::string key_str = key.ToString();
  // 将当前要处理的 key 的 sequence number 记录到 tracker 中
  TrackKey(cfh_id, key_str, seq, read_only, exclusive);

  // Always return OK. Confilct checking will happen at commit time.
  return Status::OK();
}

// Returns OK if it is safe to commit this transaction.  Returns Status::Busy
// if there are read or write conflicts that would prevent us from committing OR
// if we can not determine whether there would be any such conflicts.
//
// Should only be called on writer thread in order to avoid any race conditions
// in detecting write conflicts.
/**
 * 着 前面 TryLock 过程中获取到的 LockTracker 信息 进行seq num的比较，和 PessimisticTransaction
 * 事务中的 ValidSnapshot 做的事情一样，检测 tracker 中要提交的key 的 seq 是否比当前db 中最新的
 * 已经commit 的 seq小，用来确定这个事务执行 中间是否有外部事务 对当前key 的更新(完成 commit的操作)。
 * @param db
 * @return
 */
Status OptimisticTransaction::CheckTransactionForConflicts(DB* db) {
  auto db_impl = static_cast_with_check<DBImpl>(db);

  // Since we are on the write thread and do not want to block other writers,
  // we will do a cache-only conflict check.  This can result in TryAgain
  // getting returned if there is not sufficient memtable history to check
  // for conflicts.
  return TransactionUtil::CheckKeysForConflicts(db_impl, *tracked_locks_,
                                                true /* cache_only */);
}

Status OptimisticTransaction::SetName(const TransactionName& /* unused */) {
  return Status::InvalidArgument("Optimistic transactions cannot be named.");
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
