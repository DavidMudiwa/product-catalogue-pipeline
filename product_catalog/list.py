import duckdb

conn = duckdb.connect("dev.duckdb")

# ---------------------------------------------------------------------------
# 1. Show all schemas
# ---------------------------------------------------------------------------
print("=" * 50)
print("SCHEMAS")
print("=" * 50)
print(conn.execute("SHOW SCHEMAS").fetchdf().to_string(index=False))

# ---------------------------------------------------------------------------
# 2. Show all tables (with schema)
# ---------------------------------------------------------------------------
print("\n" + "=" * 50)
print("TABLES")
print("=" * 50)
print(conn.execute("SHOW ALL TABLES").fetchdf().to_string(index=False))

conn.close()