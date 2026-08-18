#!/usr/bin/env python3
"""
DuckDB dbcheckor - Query schemas, tables, columns, and preview data from a DuckDB database.

Usage:
    # List all schemas
    uv run dbcheck.py --db ./dev.duckdb --list-schemas

    # List all tables (across all schemas or a specific one)
    uv run dbcheck.py --db ./dev.duckdb --list-tables
    uv run dbcheck.py --db ./dev.duckdb --list-tables --schema products_raw

    # Describe a specific table (columns, types, constraints)
    uv run dbcheck.py --db ./dev.duckdb --describe products_raw.products

    # Preview first N rows of a table
    uv run dbcheck.py --db ./dev.duckdb --preview products_raw.products --limit 10

    # Full info: schemas + tables + row counts
    uv run dbcheck.py --db ./dev.duckdb --info

    # Run a custom SQL query
    uv run dbcheck.py --db ./dev.duckdb --sql "SELECT COUNT(*) FROM products_raw.products"

    # Export table to CSV
    uv run dbcheck.py --db ./dev.duckdb --export products_raw.products --output ./products.csv
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Any

import duckdb


def connect(db_path: str) -> duckdb.DuckDBPyConnection:
    """Open a connection to the DuckDB database, creating parent dirs if needed."""
    path = Path(db_path)
    if not path.exists():
        raise FileNotFoundError(f"DuckDB database not found: {db_path}")
    return duckdb.connect(str(path))


def list_schemas(conn: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    """Return all schemas in the database (excluding system schemas)."""
    result = conn.execute("""
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'main')
        ORDER BY schema_name
    """).fetchall()
    return [{"schema_name": row[0]} for row in result]


def list_tables(conn: duckdb.DuckDBPyConnection, schema: str | None = None) -> list[dict[str, Any]]:
    """Return all tables, optionally filtered by schema."""
    if schema:
        result = conn.execute("""
            SELECT table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = ?
            ORDER BY table_schema, table_name
        """, [schema]).fetchall()
    else:
        result = conn.execute("""
            SELECT table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_schema, table_name
        """).fetchall()
    return [
        {"schema": row[0], "table": row[1], "type": row[2]}
        for row in result
    ]


def describe_table(conn: duckdb.DuckDBPyConnection, table_ref: str) -> list[dict[str, Any]]:
    """Describe columns of a table. table_ref can be 'schema.table' or 'table'."""
    # Parse schema.table
    if "." in table_ref:
        schema, table = table_ref.split(".", 1)
    else:
        schema, table = "main", table_ref

    # Column info from information_schema
    cols = conn.execute("""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = ? AND table_name = ?
        ORDER BY ordinal_position
    """, [schema, table]).fetchall()

    # Try to get constraints (primary key, unique, etc.)
    constraints = conn.execute(f"""
        SELECT constraint_type, constraint_name
        FROM information_schema.table_constraints
        WHERE table_schema = '{schema}' AND table_name = '{table}'
    """).fetchall()

    # Try to get indexes
    indexes = conn.execute(f"""
        SELECT index_name, sql
        FROM duckdb_indexes()
        WHERE schema_name = '{schema}' AND table_name = '{table}'
    """).fetchall()

    return {
        "columns": [
            {
                "column": row[0],
                "type": row[1],
                "nullable": row[2],
                "default": row[3],
            }
            for row in cols
        ],
        "constraints": [
            {"type": row[0], "name": row[1]}
            for row in constraints
        ],
        "indexes": [
            {"name": row[0], "definition": row[1]}
            for row in indexes
        ],
    }


def get_row_count(conn: duckdb.DuckDBPyConnection, table_ref: str) -> int:
    """Get approximate row count for a table."""
    result = conn.execute(f"SELECT COUNT(*) FROM {table_ref}").fetchone()
    return result[0] if result else 0


def preview_table(conn: duckdb.DuckDBPyConnection, table_ref: str, limit: int = 10) -> tuple[list[str], list[tuple]]:
    """Return column names and rows for a table preview."""
    df = conn.execute(f"SELECT * FROM {table_ref} LIMIT {limit}").fetchdf()
    return list(df.columns), [tuple(row) for row in df.values]


def run_sql(conn: duckdb.DuckDBPyConnection, query: str) -> tuple[list[str], list[tuple]]:
    """Execute arbitrary SQL and return results."""
    df = conn.execute(query).fetchdf()
    return list(df.columns), [tuple(row) for row in df.values]


def export_table(conn: duckdb.DuckDBPyConnection, table_ref: str, output_path: str) -> int:
    """Export a table to CSV. Returns row count."""
    df = conn.execute(f"SELECT * FROM {table_ref}").fetchdf()
    df.to_csv(output_path, index=False)
    return len(df)


def print_table(headers: list[str], rows: list[tuple], max_width: int = 40) -> None:
    """Pretty-print a table with truncated wide columns."""
    if not rows:
        print("(no rows)")
        return

    # Calculate column widths
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            cell_str = str(cell) if cell is not None else "NULL"
            widths[i] = max(widths[i], min(len(cell_str), max_width))

    # Print header
    header_line = " | ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    print(header_line)
    print("-" * len(header_line))

    # Print rows
    for row in rows:
        cells = []
        for i, cell in enumerate(row):
            cell_str = str(cell) if cell is not None else "NULL"
            if len(cell_str) > max_width:
                cell_str = cell_str[: max_width - 3] + "..."
            cells.append(cell_str.ljust(widths[i]))
        print(" | ".join(cells))

    print(f"\n({len(rows)} row{'s' if len(rows) != 1 else ''})")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="dbcheck DuckDB databases: schemas, tables, columns, and data."
    )
    parser.add_argument("--db", required=True, help="path to the DuckDB database file")
    parser.add_argument("--list-schemas", action="store_true", help="list all schemas")
    parser.add_argument("--list-tables", action="store_true", help="list all tables")
    parser.add_argument("--schema", default=None, help="filter tables by schema name")
    parser.add_argument("--describe", metavar="TABLE", help="describe columns of a table (schema.table or table)")
    parser.add_argument("--preview", metavar="TABLE", help="preview rows of a table (schema.table or table)")
    parser.add_argument("--limit", type=int, default=10, help="max rows for --preview (default: 10)")
    parser.add_argument("--info", action="store_true", help="show full database info: schemas, tables, row counts")
    parser.add_argument("--sql", metavar="QUERY", help="run a custom SQL query")
    parser.add_argument("--export", metavar="TABLE", help="export a table to CSV (schema.table or table)")
    parser.add_argument("--output", default="export.csv", help="output path for --export (default: export.csv)")
    args = parser.parse_args()

    try:
        conn = connect(args.db)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    try:
        if args.list_schemas:
            schemas = list_schemas(conn)
            print(f"\nSchemas in {args.db}:")
            print("-" * 40)
            for s in schemas:
                print(f"  • {s['schema_name']}")
            if not schemas:
                print("  (no user schemas found)")
            print()

        if args.list_tables:
            tables = list_tables(conn, args.schema)
            filter_msg = f" in schema '{args.schema}'" if args.schema else ""
            print(f"\nTables{filter_msg} in {args.db}:")
            print("-" * 60)
            print(f"{'Schema':<20} {'Table':<30} {'Type'}")
            print("-" * 60)
            for t in tables:
                print(f"{t['schema']:<20} {t['table']:<30} {t['type']}")
            if not tables:
                print("  (no tables found)")
            print()

        if args.describe:
            info = describe_table(conn, args.describe)
            print(f"\nTable: {args.describe}")
            print("=" * 60)

            print("\nColumns:")
            print("-" * 60)
            headers = ["Column", "Type", "Nullable", "Default"]
            rows = [
                (c["column"], c["type"], c["nullable"], c["default"] or "")
                for c in info["columns"]
            ]
            print_table(headers, rows)

            if info["constraints"]:
                print("\nConstraints:")
                print("-" * 40)
                for c in info["constraints"]:
                    print(f"  [{c['type']}] {c['name']}")

            if info["indexes"]:
                print("\nIndexes:")
                print("-" * 40)
                for idx in info["indexes"]:
                    print(f"  {idx['name']}: {idx['definition']}")
            print()

        if args.preview:
            headers, rows = preview_table(conn, args.preview, args.limit)
            print(f"\nPreview: {args.preview} (first {args.limit} rows)")
            print("=" * 80)
            print_table(headers, rows)

        if args.info:
            print(f"\nDatabase: {args.db}")
            print("=" * 60)

            schemas = list_schemas(conn)
            print(f"\nSchemas ({len(schemas)}):")
            for s in schemas:
                print(f"  • {s['schema_name']}")

            tables = list_tables(conn)
            print(f"\nTables ({len(tables)}):")
            print(f"{'Schema':<20} {'Table':<30} {'Type':<10} {'Rows':>10}")
            print("-" * 75)
            for t in tables:
                full_name = f"{t['schema']}.\"{t['table']}\""
                try:
                    count = get_row_count(conn, full_name)
                except Exception:
                    count = "?"
                print(f"{t['schema']:<20} {t['table']:<30} {t['type']:<10} {count:>10}")
            print()

        if args.sql:
            headers, rows = run_sql(conn, args.sql)
            print(f"\nQuery: {args.sql}")
            print("=" * 60)
            print_table(headers, rows)

        if args.export:
            count = export_table(conn, args.export, args.output)
            print(f"\nExported {count} rows from {args.export} to {args.output}")

        # If no action specified, show help
        if not any([
            args.list_schemas, args.list_tables, args.describe,
            args.preview, args.info, args.sql, args.export
        ]):
            parser.print_help()
            return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())