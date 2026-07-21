import duckdb

conn = duckdb.connect("dev.duckdb")
model = 'products_raw.fct_price_snapshots'

#print(conn.execute("SHOW SCHEMAS").fetchall())
#print(conn.execute("SHOW ALL TABLES").fetchdf())
#print("\nSchema:")
#print(conn.execute(f"DESCRIBE {model}").fetchdf())

# -----------------------------------------------------------------------------
# Sample records
# -----------------------------------------------------------------------------
print("\nFirst 10 rows:")
print(conn.execute(f"SELECT product_id,  scraped_at, price_min, price_max, listing_price,pretty_price,discount_pct,is_multi_offer FROM {model} LIMIT 10").fetchdf())

conn.close()