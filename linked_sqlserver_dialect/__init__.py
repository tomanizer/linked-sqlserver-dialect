"""
linked_sqlserver_dialect

Minimal SQLAlchemy dialect for SQL Server linked servers.

This package registers the dialect under the SQLAlchemy dialect name:

- linked_mssql+pyodbc  (entrypoint key: linked_mssql.pyodbc)

The dialect is intended for environments where reflection must be performed via
4-part linked-server queries against INFORMATION_SCHEMA, instead of using SQL
Server stored procedures.
"""

from sqlalchemy.dialects import registry

from .base import LinkedMSDialect
from .pyodbc import LinkedMSDialect_pyodbc

# Keep in sync with pyproject.toml version for now (KISS).
__version__ = "0.2.0"

# Allow runtime registration without requiring entrypoints (useful for tests/dev).
registry.register(
    "linked_mssql.pyodbc",
    "linked_sqlserver_dialect.pyodbc",
    "LinkedMSDialect_pyodbc",
)

__all__ = ["LinkedMSDialect", "LinkedMSDialect_pyodbc", "__version__"]


