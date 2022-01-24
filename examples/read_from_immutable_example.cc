#include "rocksdb/db.h"
#include <cstdio>
#include <string>

using namespace ROCKSDB_NAMESPACE;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_simple_example";
#else
std::string kDBPath = "/tmp/read_from_immutable_example";
#endif

int main() {
  DB* db;
  rocksdb::Options options;

  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  options.create_if_missing = true;

  options.write_buffer_size = 4;

  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  db->Put(WriteOptions(), "key1", "valuevalue");
  s = db->Put(WriteOptions(), "key2", "valuevalue");
  s = db->Put(WriteOptions(), "key3", "valuevalue");
  s = db->Put(WriteOptions(), "key4", "valuevalue");
  s = db->Put(WriteOptions(), "key5", "valuevalue");
  s = db->Put(WriteOptions(), "key6", "valuevalue");
  assert(s.ok());

  std::string value;
  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.ok());
  assert(value == "valuevalue");

  delete db;
  return 0;
}