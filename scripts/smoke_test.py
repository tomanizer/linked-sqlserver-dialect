from __future__ import annotations

import argparse
import os
import sys

# Ensure the dialect is registered for local runs (works even without an
# installed distribution/entrypoints).
import linked_sqlserver_dialect  # noqa: F401

from sqlalchemy import create_engine, inspect


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Tiny smoke test for linked-sqlserver-dialect.\n\n"
            "Requires a working SQL Server connection string using:\n"
            "  linked_mssql+pyodbc://...&linked_server=...&database=...&schema=...\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--url",
        default=os.environ.get("LINKED_MSSQL_URL"),
        help="SQLAlchemy URL. If omitted, reads LINKED_MSSQL_URL.",
    )
    parser.add_argument(
        "--table",
        default=None,
        help="Optional table name to also fetch columns for (uses default schema unless overridden in URL).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Max number of table names to print.",
    )
    args = parser.parse_args()

    if not args.url:
        print(
            "Missing --url (or env LINKED_MSSQL_URL).\n\n"
            "Example:\n"
            "  export LINKED_MSSQL_URL='linked_mssql+pyodbc://user:pass@host/db?"
            "driver=ODBC+Driver+17+for+SQL+Server&linked_server=MyLS&database=RemoteDb&schema=dbo'\n"
            "  python scripts/smoke_test.py\n",
            file=sys.stderr,
        )
        return 2

    engine = create_engine(args.url)
    insp = inspect(engine)

    tables = insp.get_table_names()
    print(f"tables: {len(tables)}")
    for name in tables[: args.limit]:
        print(f"- {name}")

    views = insp.get_view_names()
    print(f"\nviews: {len(views)}")
    for name in views[: args.limit]:
        print(f"- {name}")

    if args.table:
        cols = insp.get_columns(args.table)
        print(f"\ncolumns for {args.table!r}: {len(cols)}")
        for c in cols:
            print(f"- {c['name']}: {c['type']} nullable={c['nullable']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


