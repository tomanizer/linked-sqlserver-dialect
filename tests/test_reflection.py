from __future__ import annotations

import pytest
from sqlalchemy.dialects import registry

from linked_sqlserver_dialect.base import LinkedMSDialect
from linked_sqlserver_dialect.pyodbc import LinkedMSDialect_pyodbc


def test_registry_registers_linked_mssql_pyodbc():
    # our package registers it on import, but registering twice is harmless
    registry.register(
        "linked_mssql.pyodbc",
        "linked_sqlserver_dialect.pyodbc",
        "LinkedMSDialect_pyodbc",
    )
    cls = registry.load("linked_mssql.pyodbc")
    assert cls is LinkedMSDialect_pyodbc


def test_requires_linked_server_and_database():
    d = LinkedMSDialect()
    with pytest.raises(ValueError):
        d._require_cfg()  # noqa: SLF001


def test_get_table_names_uses_default_schema_when_none():
    from tests.conftest import FakeConnection

    d = LinkedMSDialect(linked_server="LS", database="DB", schema="dbo")
    sql = (
        "SELECT TABLE_NAME "
        "FROM [LS].[DB].[INFORMATION_SCHEMA].[TABLES] "
        "WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = :schema ORDER BY TABLE_NAME"
    )
    conn = FakeConnection(rows_by_sql={sql: [("t1",), ("t2",)]})

    assert d.get_table_names(conn) == ["t1", "t2"]
    assert conn.calls == [(sql, {"schema": "dbo"})]


def test_get_table_names_overrides_schema():
    from tests.conftest import FakeConnection

    d = LinkedMSDialect(linked_server="LS", database="DB", schema="dbo")
    sql = (
        "SELECT TABLE_NAME "
        "FROM [LS].[DB].[INFORMATION_SCHEMA].[TABLES] "
        "WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = :schema ORDER BY TABLE_NAME"
    )
    conn = FakeConnection(rows_by_sql={sql: [("t3",)]})

    assert d.get_table_names(conn, schema="sales") == ["t3"]
    assert conn.calls == [(sql, {"schema": "sales"})]


def test_get_columns_filters_schema_and_maps_types():
    from tests.conftest import FakeConnection
    from sqlalchemy.sql import sqltypes

    d = LinkedMSDialect(linked_server="LS", database="DB", schema="dbo")
    sql = (
        "SELECT "
        "COLUMN_NAME, DATA_TYPE, "
        "CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, "
        "DATETIME_PRECISION, "
        "IS_NULLABLE, COLUMN_DEFAULT "
        "FROM [LS].[DB].[INFORMATION_SCHEMA].[COLUMNS] "
        "WHERE TABLE_NAME = :table_name AND TABLE_SCHEMA = :schema ORDER BY ORDINAL_POSITION"
    )
    conn = FakeConnection(
        mappings_by_sql={
            sql: [
                {
                    "COLUMN_NAME": "id",
                    "DATA_TYPE": "int",
                    "CHARACTER_MAXIMUM_LENGTH": None,
                    "NUMERIC_PRECISION": None,
                    "NUMERIC_SCALE": None,
                    "DATETIME_PRECISION": None,
                    "IS_NULLABLE": "NO",
                    "COLUMN_DEFAULT": None,
                },
                {
                    "COLUMN_NAME": "name",
                    "DATA_TYPE": "nvarchar",
                    "CHARACTER_MAXIMUM_LENGTH": 50,
                    "NUMERIC_PRECISION": None,
                    "NUMERIC_SCALE": None,
                    "DATETIME_PRECISION": None,
                    "IS_NULLABLE": "YES",
                    "COLUMN_DEFAULT": None,
                },
            ]
        }
    )

    cols = d.get_columns(conn, "users")
    assert [c["name"] for c in cols] == ["id", "name"]
    assert cols[0]["nullable"] is False
    assert cols[1]["nullable"] is True
    assert isinstance(cols[0]["type"], sqltypes.Integer)
    assert getattr(cols[1]["type"], "length", None) == 50
    assert conn.calls == [(sql, {"table_name": "users", "schema": "dbo"})]


def test_create_connect_args_pulls_custom_params():
    from sqlalchemy.engine import make_url

    d = LinkedMSDialect_pyodbc()
    url = make_url(
        "linked_mssql+pyodbc://user:pass@host/db?linked_server=LS&database=DB&schema=dbo&driver=ODBC+Driver+17+for+SQL+Server"
    )
    d.create_connect_args(url)
    cfg = d._require_cfg()  # noqa: SLF001
    assert cfg.linked_server == "LS"
    assert cfg.database == "DB"
    assert cfg.default_schema == "dbo"


