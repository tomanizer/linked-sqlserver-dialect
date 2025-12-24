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

For local development from a git checkout:

```bash
pip install -e .
```

## Usage

Provide linked server metadata via URL query parameters:

```python
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

Note: if you're running from a source checkout *without* installing the package,
add `import linked_sqlserver_dialect` once before `create_engine(...)` so the
dialect gets registered.

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
- Views: `get_view_names()`
- No PK/FK/index reflection yet

## Primary keys (missing PKs)

Many linked-server scenarios expose tables/views without primary key metadata.
SQLAlchemy can still reflect them, but ORM mapping typically needs a primary key.

You can provide **primary key overrides** in a few ways:

- **URL query param (string format)**:
  - `pk_overrides=dbo.example_table=id;dbo.other=col1,col2`

- **`connect_args` (dict or string)**:
  - `connect_args={"pk_overrides": {"dbo.example_table": ["id"]}}`
  - `connect_args={"pk_overrides": "dbo.example_table=id;dbo.other=col1,col2"}`

- **Advanced (runtime mutation)**: if you already created an engine and want to inject
  overrides programmatically:

  - `engine.dialect._pk_overrides[("dbo", "example_table")] = ["id"]`

## Smoke test

Run a real end-to-end check against your environment:

```bash
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

From your host (uses published port `14331`):

```bash
export LINKED_MSSQL_URL='linked_mssql+pyodbc://sa:YourStrong(!)Password1@localhost:14331/master?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes&linked_server=LS_REMOTE&database=RemoteDb&schema=dbo'
python scripts/smoke_test.py --table example_table
```

From inside the devcontainer (service DNS name `sql1`):

```bash
export LINKED_MSSQL_URL='linked_mssql+pyodbc://sa:YourStrong(!)Password1@sql1/master?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes&linked_server=LS_REMOTE&database=RemoteDb&schema=dbo'
python scripts/smoke_test.py --table example_table
```

### Notes / troubleshooting

- If using **ODBC Driver 18**, encryption is enabled by default; `TrustServerCertificate=yes` is the simplest local-dev setting.
- On macOS, you can install the Microsoft driver via Homebrew tap:
  - `brew tap microsoft/mssql-release`
  - `brew install microsoft/mssql-release/msodbcsql18 microsoft/mssql-release/mssql-tools18`
- Linked server provider support in SQL Server Linux images can vary. If `init` fails while running `sp_addlinkedserver`, check the `init` container logs and we can adjust the provider/options.
- Passwords are hard-coded for local dev only. Change them before sharing this setup.


