"""
linked_sqlserver_dialect

Minimal SQLAlchemy dialect for SQL Server linked servers.
"""

from sqlalchemy.dialects import registry

from .base import LinkedMSDialect
from .pyodbc import LinkedMSDialect_pyodbc

# Keep in sync with pyproject.toml version for now (KISS).
__version__ = "0.1.0"

# Allow runtime registration without requiring entrypoints (useful for tests/dev).
registry.register(
    "linked_mssql.pyodbc",
    "linked_sqlserver_dialect.pyodbc",
    "LinkedMSDialect_pyodbc",
)

__all__ = ["LinkedMSDialect", "LinkedMSDialect_pyodbc", "__version__"]


