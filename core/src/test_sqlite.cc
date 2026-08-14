#include <sqlite3.h>
#include <cstdio>

int main() {
    printf("sqlite3 version: %s\n", sqlite3_libversion());

    sqlite3* db = nullptr;
    int rc = sqlite3_open(":memory:", &db);
    if (rc != SQLITE_OK) {
        printf("FAIL: could not open in-memory DB: %s\n", sqlite3_errmsg(db));
        return 1;
    }

    rc = sqlite3_exec(db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);"
        "INSERT INTO t VALUES (1, 'hello');"
        "INSERT INTO t VALUES (2, 'world');",
        nullptr, nullptr, nullptr);

    if (rc != SQLITE_OK) {
        printf("FAIL: exec failed: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }

    sqlite3_stmt* stmt = nullptr;
    sqlite3_prepare_v2(db, "SELECT id, val FROM t ORDER BY id;", -1, &stmt, nullptr);

    printf("rows:\n");
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        int   id  = sqlite3_column_int (stmt, 0);
        const char* val = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
        printf("  %d -> %s\n", id, val);
    }
    sqlite3_finalize(stmt);
    sqlite3_close(db);

    printf("sqlite3 OK\n");
    return 0;
}