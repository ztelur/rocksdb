#include <iostream>
#include <string>
#include <vector>
#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/table.h>
#include <rocksdb/options.h>
#include <rocksdb/env.h>
#include <ctime>

using std::cout;
using std::endl;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_transaction_example";
#else
std::string kDBPath = "/tmp/rocksdb_transaction_example";
#endif

int main () {
  rocksdb::DB* db;
  rocksdb::Options options;
  rocksdb::Status s;

  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  column_families.push_back(rocksdb::ColumnFamilyDescriptor(
      rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()));
  column_families.push_back(rocksdb::ColumnFamilyDescriptor(
      "new_cf", rocksdb::ColumnFamilyOptions()));
  std::vector<rocksdb::ColumnFamilyHandle*> handles;

  options.create_if_missing = true;
  options.max_open_files = -1;

  s = rocksdb::DB::Open(options, kDBPath, &db);

  // create column family
  rocksdb::ColumnFamilyHandle* cf;
  s = db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "new_cf", &cf);
  assert(s.ok());

  // close DB
  s = db->DestroyColumnFamilyHandle(cf);
  assert(s.ok());
  delete db;

  s = rocksdb::DB::Open(options, kDBPath, column_families, &handles, &db);

  cout << handles.size() << " open status is : " << s.ToString()  << endl;
  db->Put(rocksdb::WriteOptions(), handles[1], rocksdb::Slice("key1"), rocksdb::Slice("value1"));
  db->Put(rocksdb::WriteOptions(), handles[0], rocksdb::Slice("key2"), rocksdb::Slice("value2"));
  db->Put(rocksdb::WriteOptions(), handles[1], rocksdb::Slice("key3"), rocksdb::Slice("value3"));
  db->Put(rocksdb::WriteOptions(), handles[0], rocksdb::Slice("key4"), rocksdb::Slice("value4"));

  db->Flush(rocksdb::FlushOptions(), handles[1]);
  // key5 and key6 will appear in a new WAL
  db->Put(rocksdb::WriteOptions(), handles[0], rocksdb::Slice("key5"), rocksdb::Slice("value5"));
  db->Put(rocksdb::WriteOptions(), handles[0], rocksdb::Slice("key6"), rocksdb::Slice("value6"));

  db->Flush(rocksdb::FlushOptions(), handles[0]);

  delete db;

  return 0;
}
