# linked-sqlserver-dialect

SQLAlchemy dialect for **SQL Server linked servers** where reflection must use
`INFORMATION_SCHEMA` (no `sp_tables` / `sp_columns` access).

## Requirements

- Python >= 3.12
- SQLAlchemy >= 2.0
- `pyodbc`
- **Microsoft ODBC Driver 17+ for SQL Server**

## Install

```bash
pip install linked-sqlserver-dialect
```

## Usage

Provide linked server metadata via URL query parameters:

```python
import linked_sqlserver_dialect  # registers the dialect for local dev
from sqlalchemy import create_engine, inspect

engine = create_engine(
    "linked_mssql+pyodbc://user:pass@host/db"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&linked_server=MyLinkedServer"
    "&database=RemoteDb"
    "&schema=dbo"
)

insp = inspect(engine)
print(insp.get_table_names())        # uses INFORMATION_SCHEMA.TABLES
print(insp.get_columns("MyTable"))   # uses INFORMATION_SCHEMA.COLUMNS
```

Or via `connect_args`:

```python
engine = create_engine(
    "linked_mssql+pyodbc://user:pass@host/db?driver=ODBC+Driver+17+for+SQL+Server",
    connect_args={"linked_server": "MyLinkedServer", "database": "RemoteDb", "schema": "dbo"},
)
```

## Scope / Limitations (by design)

This first release is intentionally minimal (KISS/YAGNI):

- Tables: `get_table_names()`
- Columns: `get_columns()`
- No PK/FK/index/view reflection yet

## Smoke test

Run a real end-to-end check against your environment:

```bash
# Recommended for local development so SQLAlchemy can discover the entrypoint:
#   pip install -e .
export LINKED_MSSQL_URL='linked_mssql+pyodbc://user:pass@host/db?driver=ODBC+Driver+17+for+SQL+Server&linked_server=MyLinkedServer&database=RemoteDb&schema=dbo'
python scripts/smoke_test.py --table MyTable
```

## Local devcontainer + linked-server sandbox (recommended for manual testing)

This repo includes a local-only Docker Compose setup with:

- `sql1`: primary SQL Server (your app connects here)
- `sql2`: remote SQL Server (hosts the schema/data)
- `init`: seeds `sql2` and creates a linked server on `sql1`

### Start it

From the repo root:

```bash
docker compose up -d --build
```

### Run a real reflection check against the sandbox

```bash
export LINKED_MSSQL_URL='linked_mssql+pyodbc://sa:YourStrong(!)Password1@sql1/master?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes&linked_server=LS_REMOTE&database=RemoteDb&schema=dbo'
python scripts/smoke_test.py --table example_table
```

### Notes / troubleshooting

- Linked server provider support in SQL Server Linux images can vary. If `init` fails while running `sp_addlinkedserver`, check the `init` container logs and we can adjust the provider/options.
- Passwords are hard-coded for local dev only. Change them before sharing this setup.


