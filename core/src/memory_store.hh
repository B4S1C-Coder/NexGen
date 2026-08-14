#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <sqlite3.h>
#include "logger.hh"

struct MemoryStoreConfig {
  std::string db_path = "nexgen_memory.db";
  std::string queries_path = "";
};

struct Memory {
  std::string key;
  std::string value;
  int64_t saved_at = 0;
};

// Key convention: "session_<id>/phase/<phase_name>" or "session_<id>/global/<label>"

class MemoryStore {
public:
  MemoryStore() = default;
  ~MemoryStore() { shutdown(); }

  MemoryStore(const MemoryStore&) = delete;
  MemoryStore& operator=(const MemoryStore&) = delete;

  // open (or create) the SQLite DB, parse queries file if configured
  bool init(const MemoryStoreConfig& cfg);
  void shutdown();

  // save a memory - overwrites if key already exists
  bool save(const std::string& key, const std::string& value);

  // fetch one memory by exact key - empty string if not found
  std::string fetch(const std::string& key) const;

  // fetch multiple keys - returns XML Block for context builder
  // missing keys are silently skipped
  std::string fetch_many(const std::vector<std::string>& keys) const;

  // fetch all memories matching a prefix eg. "session_abc/"
  // returns XML Block for Context Builder
  std::string fetch_prefix(const std::string& prefix) const;

  // delete a key - returns false if key didn't exist
  bool remove(const std::string& key);

  // list all keys - for debugging / inspection
  std::vector<std::string> list_keys() const;

private:
  sqlite3* db_ = nullptr;

  // parsed query map - populated from file or hardcoded defaults
  std::unordered_map<std::string, std::string> queries_;

  // get a query by name - asserts if missing (programming error)
  const std::string& q(const std::string& name) const;

  // load named queries from a .sql file
  // returns false if file cannot be opened
  bool load_queries_from_file(const std::string& path);

  // populate queries_ with hardcoded defaults
  void load_default_queries();

  // execute a no-result SQL statement (DDL, DELETE etc.)
  bool exec(const std::string& sql) const;

  // format list of memory records as XML
  static std::string to_xml(const std::vector<Memory>& memories);
};