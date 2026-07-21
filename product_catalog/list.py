import duckdb

conn = duckdb.connect("dev.duckdb")
model = 'products_raw.stg_products'

#print(conn.execute("SHOW SCHEMAS").fetchall())
#print(conn.execute("SHOW ALL TABLES").fetchdf())
print("\nSchema:")
print(conn.execute(f"DESCRIBE {model}").fetchdf())

# -----------------------------------------------------------------------------
# Sample records
# -----------------------------------------------------------------------------
print("\nFirst 10 rows:")
print(conn.execute(f"SELECT product_id,brand,title, listing_price,discount_pct,rating,reviews FROM {model} LIMIT 10").fetchdf())

conn.close()