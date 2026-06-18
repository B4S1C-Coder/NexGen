#include "memory_store.hh"

#include <fstream>
#include <sstream>
#include <stdexcept>

static const std::unordered_map<std::string, std::string> DEFAULT_QUERIES = {
  {"create_table",
    "CREATE TABLE IF NOT EXISTS memories ("
    "  key      TEXT PRIMARY KEY,"
    "  value    TEXT NOT NULL,"
    "  saved_at INTEGER DEFAULT (unixepoch())"
    ");"},

  {"save",
    "INSERT OR REPLACE INTO memories(key, value, saved_at) "
    "VALUES(?, ?, unixepoch());"},

  {"fetch_one",
    "SELECT value FROM memories WHERE key = ?;"},

  {"fetch_prefix",
    "SELECT key, value FROM memories WHERE key LIKE ? ORDER BY saved_at ASC;"},

  {"remove",
    "DELETE FROM memories WHERE key = ?;"},

  {"list_keys",
    "SELECT key FROM memories ORDER BY saved_at ASC;"},
};

bool MemoryStore::init(const MemoryStoreConfig& cfg) {
  bool loaded_from_file = false;
  if (!cfg.queries_path.empty()) {
    loaded_from_file = load_queries_from_file(cfg.queries_path);

    if (!loaded_from_file) {
      LOG_WARN("MemoryStore: queries file '{}' not found or invalid - "
        "using hardcoded defaults", cfg.queries_path);
    }
  }

  if (!loaded_from_file) {
    load_default_queries();
  }

  int rc = sqlite3_open(cfg.db_path.c_str(), &db_);
  if (rc != SQLITE_OK) {
    LOG_ERROR("MemoryStore: failed to open DB '{}': {}",
      cfg.db_path, sqlite3_errmsg(db_));
    db_ = nullptr;
    return false;
  }

  exec("PRAGMA journal=WAL;");
  exec("PRAGMA synchronous=NORMAL;");

  if (!exec(q("create_table"))) {
    LOG_ERROR("MemoryStore: failed to create table");
    return false;
  }

  LOG_INFO("MemoryStore: init OK - db: '{}' queries: {}",
    cfg.db_path,
    loaded_from_file ? cfg.queries_path : "hardcoded defaults"
  );

  return true;
}

void MemoryStore::shutdown() {
  if (db_) {
    sqlite3_close(db_);
    db_ = nullptr;
  }
}

bool MemoryStore::save(const std::string& key, const std::string& value) {
  sqlite3_stmt* stmt = nullptr;
  const std::string& sql = q("save");

  int rc = sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr);
  if (rc != SQLITE_OK) {
    LOG_ERROR("MemoryStore::save: prepare failed: {}", sqlite3_errmsg(db_));
    return false;
  }

  sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_text(stmt, 2, value.c_str(), -1, SQLITE_STATIC);

  rc = sqlite3_step(stmt);
  sqlite3_finalize(stmt);

  if (rc != SQLITE_DONE) {
    LOG_ERROR("MemoryStore::save: step failed for key '{}': {}", key, sqlite3_errmsg(db_));
    return false;
  }

  LOG_DEBUG("MemoryStore: saved key '{}'", key);
  return true;
}

std::string MemoryStore::fetch(const std::string& key) const {
  sqlite3_stmt* stmt = nullptr;
  const std::string& sql = q("fetch_one");

  int rc = sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr);
  if(rc != SQLITE_OK) {
    LOG_ERROR("MemoryStore::fetch: prepare failed: {}", sqlite3_errmsg(db_));
    return "";
  }

  sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_STATIC);

  std::string result;
  if (sqlite3_step(stmt) == SQLITE_ROW) {
    const char* val = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
    if (val) result = val;
  }

  sqlite3_finalize(stmt);
  return result;
}

std::string MemoryStore::fetch_many(const std::vector<std::string>& keys) const {
  std::vector<Memory> found;
  found.reserve(keys.size());

  for (const auto& key : keys) {
    std::string val = fetch(key);
    if (!val.empty()) {
      found.push_back({ key, val, 0 });
    }
  }

  if (found.empty()) return "";
  return to_xml(found);
}

std::string MemoryStore::fetch_prefix(const std::string& prefix) const {
  sqlite3_stmt* stmt = nullptr;
  const std::string& sql = q("fetch_prefix");

  int rc = sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr);
  if (rc != SQLITE_OK) {
    LOG_ERROR("MemoryStore::fetch_prefix: prepare failed: {}", sqlite3_errmsg(db_));
    return "";
  }

  std::string pattern;
  pattern.reserve(prefix.size() + 1);

  for (char c : prefix) {
    if (c == '%' || c == '_' || c == '\\') pattern += '\\';
    pattern += c;
  }

  pattern += '%';

  sqlite3_bind_text(stmt, 1, pattern.c_str(), -1, SQLITE_TRANSIENT);

  std::vector<Memory> found;
  while (sqlite3_step(stmt) == SQLITE_ROW) {
    const char* k = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
    const char* v = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
    if (k && v) found.push_back({ k, v, 0 });
  }

  sqlite3_finalize(stmt);

  if (found.empty()) return "";
  return to_xml(found);
}

bool MemoryStore::remove(const std::string& key) {
  sqlite3_stmt* stmt = nullptr;
  const std::string& sql = q("remove");

  int rc = sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr);
  if (rc != SQLITE_OK) {
    LOG_ERROR("MemoryStore::remove: prepare failed: {}", sqlite3_errmsg(db_));
    return false;
  }

  sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_STATIC);
  rc = sqlite3_step(stmt);
  sqlite3_finalize(stmt);

  if (rc != SQLITE_DONE) {
    LOG_ERROR("MemoryStore::remove: step failed for key '{}': {}", key, sqlite3_errmsg(db_));
    return false;
  }

  bool deleted = sqlite3_changes(db_) > 0;
  if (!deleted){
    LOG_DEBUG("Memory::remove: key '{}' not found", key);
  }

  return deleted;
}

std::vector<std::string> MemoryStore::list_keys() const {
  sqlite3_stmt* stmt = nullptr;
  const std::string& sql = q("list_keys");

  int rc = sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr);
  if (rc != SQLITE_OK) {
    LOG_ERROR("MemoryStore::list_keys: prepare failed: {}", sqlite3_errmsg(db_));
    return {};
  }

  std::vector<std::string> keys;
  while (sqlite3_step(stmt) == SQLITE_ROW) {
    const char* k = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
    if (k) keys.emplace_back(k);
  }

  sqlite3_finalize(stmt);
  return keys;
}

bool MemoryStore::load_queries_from_file(const std::string& path) {
  std::ifstream f(path);
  if (!f.is_open()) return false;

  std::string          current_name;
  std::ostringstream   current_sql;
  std::unordered_map<std::string, std::string> parsed;

  auto flush = [&]() {
    if (!current_name.empty()) {
      std::string sql = current_sql.str();
      // trim leading/trailing whitespace
      size_t start = sql.find_first_not_of(" \t\r\n");
      size_t end   = sql.find_last_not_of(" \t\r\n");
      if (start != std::string::npos) {
        parsed[current_name] = sql.substr(start, end - start + 1);
      }
      current_name.clear();
      current_sql.str("");
      current_sql.clear();
    }
  };

  std::string line;
  while (std::getline(f, line)) {
    // check for marker line: -- @name
    if (line.size() >= 4
        && line[0] == '-' && line[1] == '-'
        && line.find("-- @") == 0) {
      flush();
      current_name = line.substr(4);
      // trim trailing whitespace from name
      size_t end = current_name.find_last_not_of(" \t\r\n");
      if (end != std::string::npos) current_name = current_name.substr(0, end + 1);
      continue;
    }
    // skip comment-only lines that aren't markers
    if (!current_name.empty()) {
      current_sql << line << '\n';
    }
  }
  flush();

  if (parsed.empty()) {
    LOG_ERROR("MemoryStore: queries file '{}' parsed but no queries found", path);
    return false;
  }

  // verify all required queries are present
  for (const auto& [name, _] : DEFAULT_QUERIES) {
    if (parsed.find(name) == parsed.end()) {
      LOG_ERROR("MemoryStore: queries file missing required query '{}' — "
                "falling back to defaults", name);
      return false;
    }
  }

  queries_ = std::move(parsed);
  LOG_INFO("MemoryStore: loaded {} queries from '{}'", queries_.size(), path);
  return true;
}

void MemoryStore::load_default_queries() {
  queries_ = DEFAULT_QUERIES;
  LOG_INFO("MemoryStore: using {} hardcoded default queries", queries_.size());
}

const std::string& MemoryStore::q(const std::string& name) const {
  auto it = queries_.find(name);
  if (it == queries_.end()) {
    // programming error — crash loudly
    throw std::runtime_error("MemoryStore: unknown query name '" + name + "'");
  }
  return it->second;
}

bool MemoryStore::exec(const std::string& sql) const {
  char* errmsg = nullptr;
  int rc = sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &errmsg);
  if (rc != SQLITE_OK) {
    LOG_ERROR("MemoryStore::exec failed: {}", errmsg ? errmsg : "unknown");
    sqlite3_free(errmsg);
    return false;
  }
  return true;
}

std::string MemoryStore::to_xml(const std::vector<Memory>& memories) {
  std::ostringstream ss;
  ss << "<memories>\n";
  for (const auto& m : memories) {
    ss << "  <memory key=\"" << m.key << "\">"
       << m.value
       << "</memory>\n";
  }
  ss << "</memories>";
  return ss.str();
}