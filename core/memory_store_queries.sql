-- NexGen MemoryStore query definitions
-- Format: -- @query_name followed by the SQL statement ending at the next -- @ or EOF
-- Edit these to customize queries without recompiling.

-- @create_table
CREATE TABLE IF NOT EXISTS memories (
    key      TEXT PRIMARY KEY,
    value    TEXT NOT NULL,
    saved_at INTEGER DEFAULT (unixepoch())
);

-- @save
INSERT OR REPLACE INTO memories(key, value, saved_at)
VALUES(?, ?, unixepoch());

-- @fetch_one
SELECT value FROM memories WHERE key = ?;

-- @fetch_prefix
SELECT key, value FROM memories WHERE key LIKE ? ORDER BY saved_at ASC;

-- @remove
DELETE FROM memories WHERE key = ?;

-- @list_keys
SELECT key FROM memories ORDER BY saved_at ASC;