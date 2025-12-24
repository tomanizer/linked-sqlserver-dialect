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
export LINKED_MSSQL_URL='linked_mssql+pyodbc://user:pass@host/db?driver=ODBC+Driver+17+for+SQL+Server&linked_server=MyLinkedServer&database=RemoteDb&schema=dbo'
python scripts/smoke_test.py --table MyTable
```


