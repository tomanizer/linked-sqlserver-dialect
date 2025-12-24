from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.dialects.mssql.pyodbc import MSDialect_pyodbc
from sqlalchemy.engine import Connection
from sqlalchemy.sql import sqltypes


@dataclass(frozen=True)
class LinkedServerConfig:
    linked_server: str
    database: str
    default_schema: str | None = None


def _bracket(ident: str) -> str:
    # Keep it extremely simple; for safety, reject weird inputs rather than
    # attempting to escape and accidentally allowing injection.
    if not ident or any(c in ident for c in "[];'\n\r\t"):
        raise ValueError(f"Invalid identifier: {ident!r}")
    return f"[{ident}]"


def _info_schema_4part(cfg: LinkedServerConfig, view: str) -> str:
    # [LinkedServer].[Database].[INFORMATION_SCHEMA].[TABLES]
    return f"{_bracket(cfg.linked_server)}.{_bracket(cfg.database)}.[INFORMATION_SCHEMA].{_bracket(view)}"


class LinkedMSDialect(MSDialect_pyodbc):
    """
    Minimal dialect that reflects tables/columns via INFORMATION_SCHEMA on a
    SQL Server linked server.

    Supports:
    - get_table_names()
    - get_columns()
    """

    name = "linked_mssql"

    def __init__(
        self,
        *args: Any,
        linked_server: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)
        self._linked_cfg: LinkedServerConfig | None = (
            LinkedServerConfig(linked_server, database, schema)
            if linked_server and database
            else None
        )

    def _set_cfg_from_connect_params(self, cparams: dict[str, Any]) -> None:
        # Allow passing via create_engine(..., connect_args={...}).
        linked_server = cparams.pop("linked_server", None)
        database = cparams.pop("database", None)
        schema = cparams.pop("schema", None)
        if linked_server and database and self._linked_cfg is None:
            self._linked_cfg = LinkedServerConfig(
                str(linked_server), str(database), str(schema) if schema else None
            )

    def connect(self, *cargs: Any, **cparams: Any):
        # Called for new DB-API connections; intercept our custom args so pyodbc
        # doesn't see them.
        self._set_cfg_from_connect_params(cparams)
        return super().connect(*cargs, **cparams)

    def _require_cfg(self) -> LinkedServerConfig:
        if self._linked_cfg is None:
            raise ValueError(
                "linked_server dialect requires 'linked_server' and 'database'. "
                "Provide them via URL query params or connect_args."
            )
        return self._linked_cfg

    def _effective_schema(self, schema: str | None) -> str | None:
        if schema is not None:
            return schema
        cfg = self._require_cfg()
        return cfg.default_schema

    def get_table_names(
        self, connection: Connection, schema: str | None = None, **kw: Any
    ) -> list[str]:
        cfg = self._require_cfg()
        eff_schema = self._effective_schema(schema)

        from_obj = _info_schema_4part(cfg, "TABLES")
        stmt = (
            f"SELECT TABLE_NAME "
            f"FROM {from_obj} "
            f"WHERE TABLE_TYPE = 'BASE TABLE'"
        )
        params: dict[str, Any] = {}
        if eff_schema is not None:
            stmt += " AND TABLE_SCHEMA = :schema"
            params["schema"] = eff_schema
        stmt += " ORDER BY TABLE_NAME"

        rows = connection.execute(text(stmt), params).all()
        return [r[0] for r in rows]

    def get_columns(
        self,
        connection: Connection,
        table_name: str,
        schema: str | None = None,
        **kw: Any,
    ) -> list[dict[str, Any]]:
        cfg = self._require_cfg()
        eff_schema = self._effective_schema(schema)

        from_obj = _info_schema_4part(cfg, "COLUMNS")
        stmt = (
            f"SELECT "
            f"COLUMN_NAME, DATA_TYPE, "
            f"CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, "
            f"DATETIME_PRECISION, "
            f"IS_NULLABLE, COLUMN_DEFAULT "
            f"FROM {from_obj} "
            f"WHERE TABLE_NAME = :table_name"
        )
        params: dict[str, Any] = {"table_name": table_name}
        if eff_schema is not None:
            stmt += " AND TABLE_SCHEMA = :schema"
            params["schema"] = eff_schema
        stmt += " ORDER BY ORDINAL_POSITION"

        rows = connection.execute(text(stmt), params).mappings().all()

        cols: list[dict[str, Any]] = []
        for row in rows:
            colname = row["COLUMN_NAME"]
            dtype = row["DATA_TYPE"]
            char_len = row["CHARACTER_MAXIMUM_LENGTH"]
            num_prec = row["NUMERIC_PRECISION"]
            num_scale = row["NUMERIC_SCALE"]
            dt_prec = row["DATETIME_PRECISION"]
            is_nullable = row["IS_NULLABLE"]
            default = row["COLUMN_DEFAULT"]

            coltype = self._resolve_type(
                dtype,
                char_len=char_len,
                num_prec=num_prec,
                num_scale=num_scale,
                dt_prec=dt_prec,
            )

            cols.append(
                {
                    "name": colname,
                    "type": coltype,
                    "nullable": (str(is_nullable).upper() == "YES"),
                    "default": default,
                }
            )
        return cols

    def _resolve_type(
        self,
        dtype: Any,
        *,
        char_len: Any,
        num_prec: Any,
        num_scale: Any,
        dt_prec: Any,
    ) -> sqltypes.TypeEngine:
        if dtype is None:
            return sqltypes.NULLTYPE

        dtype_norm = str(dtype).lower()
        type_cls = self.ischema_names.get(dtype_norm)
        if type_cls is None:
            return sqltypes.NULLTYPE

        # Handle common parameterized types.
        try:
            if dtype_norm in {"varchar", "nvarchar", "char", "nchar", "binary", "varbinary"}:
                if char_len in (None, -1):
                    # SQL Server uses -1 for MAX for varchar/nvarchar/varbinary
                    return type_cls()  # type: ignore[misc]
                return type_cls(length=int(char_len))  # type: ignore[misc]
            if dtype_norm in {"decimal", "numeric"}:
                if num_prec is None:
                    return type_cls()  # type: ignore[misc]
                if num_scale is None:
                    return type_cls(precision=int(num_prec))  # type: ignore[misc]
                return type_cls(precision=int(num_prec), scale=int(num_scale))  # type: ignore[misc]
            if dtype_norm in {"datetime2", "time", "datetimeoffset"}:
                if dt_prec is None:
                    return type_cls()  # type: ignore[misc]
                return type_cls(precision=int(dt_prec))  # type: ignore[misc]
        except Exception:
            return sqltypes.NULLTYPE

        # Default for non-parameterized types.
        try:
            return type_cls()  # type: ignore[misc]
        except Exception:
            return sqltypes.NULLTYPE


