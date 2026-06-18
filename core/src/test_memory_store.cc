// test_memory_store.cc
// Tests MemoryStore in isolation — no model needed.

#include "memory_store.hh"
#include "logger.hh"

#include <cassert>
#include <iostream>
#include <fstream>
#include <cstdio>

static int passed = 0;
static int failed = 0;

#define TEST(name) \
  do { std::cout << "[TEST] " << name << " ... "; } while(0)

#define PASS() \
  do { std::cout << "PASS\n"; passed++; } while(0)

#define FAIL(msg) \
  do { std::cout << "FAIL — " << msg << "\n"; failed++; return; } while(0)

#define ASSERT_TRUE(expr, msg) \
  do { if (!(expr)) { FAIL(msg); } } while(0)

#define ASSERT_FALSE(expr, msg) \
  do { if ((expr))  { FAIL(msg); } } while(0)

#define ASSERT_EQ(a, b, msg) \
  do { if ((a) != (b)) { FAIL(msg); } } while(0)

static const std::string DB_PATH      = "./test_nexgen_memory.db";
static const std::string QUERIES_PATH = "./../memory_store_queries.sql";

// ─────────────────────────────────────────────────────────────────────────────
// helpers
// ─────────────────────────────────────────────────────────────────────────────
static MemoryStore* make_store(bool use_queries_file = false) {
  MemoryStoreConfig cfg;
  cfg.db_path      = DB_PATH;
  cfg.queries_path = use_queries_file ? QUERIES_PATH : "";

  MemoryStore* ms = new MemoryStore();
  bool ok = ms->init(cfg);
  assert(ok && "MemoryStore::init failed in make_store");
  return ms;
}

static void write_queries_file() {
  std::ofstream f(QUERIES_PATH);
  f << R"(
-- @create_table
CREATE TABLE IF NOT EXISTS memories (
    key      TEXT PRIMARY KEY,
    value    TEXT NOT NULL,
    saved_at INTEGER DEFAULT (unixepoch())
);

-- @save
INSERT OR REPLACE INTO memories(key, value, saved_at) VALUES(?, ?, unixepoch());

-- @fetch_one
SELECT value FROM memories WHERE key = ?;

-- @fetch_prefix
SELECT key, value FROM memories WHERE key LIKE ? ORDER BY saved_at ASC;

-- @remove
DELETE FROM memories WHERE key = ?;

-- @list_keys
SELECT key FROM memories ORDER BY saved_at ASC;
)";
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 1 — init with default queries
// ─────────────────────────────────────────────────────────────────────────────
void test_init_defaults() {
  TEST("init with hardcoded defaults");
  std::remove(DB_PATH.c_str());

  MemoryStoreConfig cfg;
  cfg.db_path      = DB_PATH;
  cfg.queries_path = "";  // no file

  MemoryStore ms;
  bool ok = ms.init(cfg);
  ASSERT_TRUE(ok, "init failed with default queries");
  PASS();
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 2 — init with queries file
// ─────────────────────────────────────────────────────────────────────────────
void test_init_queries_file() {
  TEST("init with queries file");
  std::remove(DB_PATH.c_str());
  write_queries_file();

  MemoryStoreConfig cfg;
  cfg.db_path      = DB_PATH;
  cfg.queries_path = QUERIES_PATH;

  MemoryStore ms;
  bool ok = ms.init(cfg);
  ASSERT_TRUE(ok, "init failed with queries file");
  PASS();
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 3 — init falls back to defaults when file missing
// ─────────────────────────────────────────────────────────────────────────────
void test_init_fallback() {
  TEST("init falls back to defaults when queries file missing");
  std::remove(DB_PATH.c_str());

  MemoryStoreConfig cfg;
  cfg.db_path      = DB_PATH;
  cfg.queries_path = "/tmp/this_file_does_not_exist.sql";

  MemoryStore ms;
  bool ok = ms.init(cfg);
  ASSERT_TRUE(ok, "init should succeed even with missing queries file");
  PASS();
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 4 — save and fetch exact key
// ─────────────────────────────────────────────────────────────────────────────
void test_save_fetch() {
  TEST("save and fetch exact key");
  std::remove(DB_PATH.c_str());
  auto ms = make_store();

  bool saved = ms->save("session_abc/phase/planning",
                       "The plan is to search for X then summarize.");
  ASSERT_TRUE(saved, "save failed");

  std::string val = ms->fetch("session_abc/phase/planning");
  ASSERT_FALSE(val.empty(), "fetch returned empty for existing key");
  ASSERT_TRUE(val.find("search for X") != std::string::npos,
              "fetched value doesn't contain expected text");

  PASS();
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 5 — fetch missing key returns empty
// ─────────────────────────────────────────────────────────────────────────────
void test_fetch_missing() {
  TEST("fetch missing key returns empty string");
  std::remove(DB_PATH.c_str());
  auto ms = make_store();

  std::string val = ms->fetch("session_xyz/phase/nonexistent");
  ASSERT_TRUE(val.empty(), "expected empty string for missing key");
  PASS();
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 6 — save overwrites existing key
// ─────────────────────────────────────────────────────────────────────────────
void test_save_overwrite() {
  TEST("save overwrites existing key");
  std::remove(DB_PATH.c_str());
  auto ms = make_store();

  ms->save("session_abc/phase/planning", "original value");
  ms->save("session_abc/phase/planning", "updated value");

  std::string val = ms->fetch("session_abc/phase/planning");
  ASSERT_TRUE(val == "updated value",
              "expected updated value, got: " + val);
  PASS();
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 7 — fetch_many returns XML with correct keys
// ─────────────────────────────────────────────────────────────────────────────
void test_fetch_many_xml() {
  TEST("fetch_many returns valid XML with correct keys");
  std::remove(DB_PATH.c_str());
  auto ms = make_store();

  ms->save("session_abc/phase/planning",   "plan summary");
  ms->save("session_abc/phase/execution",  "execution summary");
  ms->save("session_abc/phase/evaluation", "eval summary");

  auto xml = ms->fetch_many({
    "session_abc/phase/planning",
    "session_abc/phase/execution",
    "session_abc/phase/nonexistent",  // missing — should be skipped
  });

  ASSERT_FALSE(xml.empty(), "fetch_many returned empty");
  ASSERT_TRUE(xml.find("<memories>")  != std::string::npos, "missing <memories> tag");
  ASSERT_TRUE(xml.find("</memories>") != std::string::npos, "missing </memories> tag");
  ASSERT_TRUE(xml.find("session_abc/phase/planning")  != std::string::npos,
              "missing planning key in XML");
  ASSERT_TRUE(xml.find("session_abc/phase/execution") != std::string::npos,
              "missing execution key in XML");
  ASSERT_FALSE(xml.find("nonexistent") != std::string::npos,
               "nonexistent key should not appear in XML");
  ASSERT_TRUE(xml.find("plan summary")  != std::string::npos, "missing plan value");
  ASSERT_TRUE(xml.find("execution summary") != std::string::npos, "missing exec value");

  std::cout << "\n    xml:\n" << xml << "\n    ";
  PASS();
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 8 — fetch_many with all missing keys returns empty
// ─────────────────────────────────────────────────────────────────────────────
void test_fetch_many_all_missing() {
  TEST("fetch_many with all missing keys returns empty string");
  std::remove(DB_PATH.c_str());
  auto ms = make_store();

  auto xml = ms->fetch_many({"missing_a", "missing_b"});
  ASSERT_TRUE(xml.empty(), "expected empty string when all keys missing");
  PASS();
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 9 — fetch_prefix returns all matching keys
// ─────────────────────────────────────────────────────────────────────────────
void test_fetch_prefix() {
  TEST("fetch_prefix returns all keys under a session");
  std::remove(DB_PATH.c_str());
  auto ms = make_store();

  ms->save("session_abc/phase/planning",   "plan");
  ms->save("session_abc/phase/execution",  "exec");
  ms->save("session_abc/global/context",   "ctx");
  ms->save("session_xyz/phase/planning",   "other session"); // different session

  auto xml = ms->fetch_prefix("session_abc/");

  ASSERT_FALSE(xml.empty(), "fetch_prefix returned empty");
  ASSERT_TRUE(xml.find("session_abc/phase/planning")  != std::string::npos, "missing planning");
  ASSERT_TRUE(xml.find("session_abc/phase/execution") != std::string::npos, "missing execution");
  ASSERT_TRUE(xml.find("session_abc/global/context")  != std::string::npos, "missing context");
  ASSERT_FALSE(xml.find("session_xyz") != std::string::npos,
               "session_xyz should not appear in session_abc prefix fetch");
  PASS();
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 10 — remove existing key
// ─────────────────────────────────────────────────────────────────────────────
void test_remove() {
  TEST("remove existing key");
  std::remove(DB_PATH.c_str());
  auto ms = make_store();

  ms->save("session_abc/phase/planning", "to be deleted");
  bool removed = ms->remove("session_abc/phase/planning");
  ASSERT_TRUE(removed, "remove returned false for existing key");

  std::string val = ms->fetch("session_abc/phase/planning");
  ASSERT_TRUE(val.empty(), "key still exists after remove");
  PASS();
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 11 — remove missing key returns false
// ─────────────────────────────────────────────────────────────────────────────
void test_remove_missing() {
  TEST("remove missing key returns false");
  std::remove(DB_PATH.c_str());
  auto ms = make_store();

  bool removed = ms->remove("session_abc/phase/nonexistent");
  ASSERT_FALSE(removed, "remove should return false for missing key");
  PASS();
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 12 — list_keys
// ─────────────────────────────────────────────────────────────────────────────
void test_list_keys() {
  TEST("list_keys returns all stored keys");
  std::remove(DB_PATH.c_str());
  auto ms = make_store();

  ms->save("session_abc/phase/planning",  "a");
  ms->save("session_abc/phase/execution", "b");
  ms->save("session_xyz/phase/planning",  "c");

  auto keys = ms->list_keys();
  ASSERT_EQ((int)keys.size(), 3, "expected 3 keys, got " + std::to_string(keys.size()));
  PASS();
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 13 — persistence across re-open
// ─────────────────────────────────────────────────────────────────────────────
void test_persistence() {
  TEST("data persists across shutdown and re-init");
  std::remove(DB_PATH.c_str());

  {
    auto ms = make_store();
    ms->save("session_abc/phase/planning", "persisted value");
    // ms destructor closes DB here
  }

  {
    auto ms = make_store();
    std::string val = ms->fetch("session_abc/phase/planning");
    ASSERT_EQ(val, std::string("persisted value"),
              "value did not persist across re-open: got '" + val + "'");
  }

  PASS();
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 14 — queries file takes effect (custom query behavior)
// ─────────────────────────────────────────────────────────────────────────────
void test_queries_file_used() {
  TEST("queries file is actually used when present");
  std::remove(DB_PATH.c_str());
  write_queries_file();

  MemoryStoreConfig cfg;
  cfg.db_path      = DB_PATH;
  cfg.queries_path = QUERIES_PATH;

  MemoryStore ms;
  ASSERT_TRUE(ms.init(cfg), "init with queries file failed");

  ms.save("session_abc/phase/test", "from queries file");
  std::string val = ms.fetch("session_abc/phase/test");
  ASSERT_FALSE(val.empty(), "fetch failed after init with queries file");
  ASSERT_EQ(val, std::string("from queries file"), "wrong value: " + val);

  PASS();
}

// ─────────────────────────────────────────────────────────────────────────────
// main
// ─────────────────────────────────────────────────────────────────────────────
int main() {
  Logger::set_level(LogLevel::Info);
  std::cout << "=== MemoryStore tests ===\n\n";

  test_init_defaults();
  test_init_queries_file();
  test_init_fallback();
  test_save_fetch();
  test_fetch_missing();
  test_save_overwrite();
  test_fetch_many_xml();
  test_fetch_many_all_missing();
  test_fetch_prefix();
  test_remove();
  test_remove_missing();
  test_list_keys();
  test_persistence();
  test_queries_file_used();

  std::cout << "\n=== results: "
            << passed << " passed, "
            << failed << " failed ===\n";

  // cleanup
  std::remove(DB_PATH.c_str());
  std::remove(QUERIES_PATH.c_str());

  return failed > 0 ? 1 : 0;
}