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


def _parse_pk_overrides(value: Any) -> dict[tuple[str | None, str], list[str]]:
    """
    Parse primary key overrides.

    Supported inputs:
    - dict: { "schema.table": ["col1", "col2"], "table": ["id"] }
    - str:  "schema.table=col1,col2;table=id"

    Returns a dict keyed by (schema, table) where schema may be None.
    """
    if value is None:
        return {}

    if isinstance(value, dict):
        items = value.items()
    elif isinstance(value, str):
        items = []
        for part in (p.strip() for p in value.split(";")):
            if not part:
                continue
            if "=" not in part:
                raise ValueError(
                    "pk_overrides must be 'schema.table=col1,col2;table=id' or a dict"
                )
            k, v = part.split("=", 1)
            items.append((k.strip(), [c.strip() for c in v.split(",") if c.strip()]))
    else:
        raise TypeError("pk_overrides must be a dict or str")

    out: dict[tuple[str | None, str], list[str]] = {}
    for raw_key, raw_cols in items:
        if not raw_key:
            continue
        if isinstance(raw_cols, str):
            cols = [c.strip() for c in raw_cols.split(",") if c.strip()]
        else:
            cols = [str(c).strip() for c in raw_cols if str(c).strip()]
        if not cols:
            continue

        if "." in raw_key:
            schema, table = raw_key.split(".", 1)
            schema = schema.strip() or None
            table = table.strip()
        else:
            schema = None
            table = raw_key.strip()

        if not table:
            continue
        out[(schema.lower() if schema else None, table.lower())] = cols
    return out


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

    This dialect only customizes reflection; normal SQL compilation/execution is
    inherited from SQLAlchemy's MSSQL+pyodbc dialect.

    Required configuration (provided via URL query params or connect_args):
    - linked_server: the linked server name (first part)
    - database: the remote database/catalog name (second part)
    - schema: optional default schema for reflection (filters TABLE_SCHEMA)
    - pk_overrides: optional primary key overrides for tables/views that don't expose PKs

    Implemented reflection methods:
    - get_table_names()
    - get_columns()
    - get_view_names()
    - get_view_definition() (best-effort; may return None if permissions prevent it)
    - get_pk_constraint() (best-effort + optional overrides)
    """

    name = "linked_mssql"
    supports_statement_cache = True

    def __init__(
        self,
        *args: Any,
        linked_server: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        pk_overrides: Any = None,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)
        self._linked_cfg: LinkedServerConfig | None = (
            LinkedServerConfig(linked_server, database, schema)
            if linked_server and database
            else None
        )
        self._pk_overrides: dict[tuple[str | None, str], list[str]] = _parse_pk_overrides(
            pk_overrides
        )

    def _set_cfg_from_connect_params(self, cparams: dict[str, Any]) -> None:
        # Allow passing via create_engine(..., connect_args={...}).
        linked_server = cparams.pop("linked_server", None)
        database = cparams.pop("database", None)
        schema = cparams.pop("schema", None)
        pk_overrides = cparams.pop("pk_overrides", None)
        if linked_server and database and self._linked_cfg is None:
            self._linked_cfg = LinkedServerConfig(
                str(linked_server), str(database), str(schema) if schema else None
            )
        if pk_overrides:
            self._pk_overrides.update(_parse_pk_overrides(pk_overrides))

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

    def get_view_names(
        self, connection: Connection, schema: str | None = None, **kw: Any
    ) -> list[str]:
        cfg = self._require_cfg()
        eff_schema = self._effective_schema(schema)

        from_obj = _info_schema_4part(cfg, "VIEWS")
        stmt = f"SELECT TABLE_NAME FROM {from_obj} WHERE 1=1"
        params: dict[str, Any] = {}
        if eff_schema is not None:
            stmt += " AND TABLE_SCHEMA = :schema"
            params["schema"] = eff_schema
        stmt += " ORDER BY TABLE_NAME"

        rows = connection.execute(text(stmt), params).all()
        return [r[0] for r in rows]

    def get_view_definition(
        self,
        connection: Connection,
        view_name: str,
        schema: str | None = None,
        **kw: Any,
    ) -> str | None:
        """
        Best-effort view definition lookup via INFORMATION_SCHEMA.VIEWS.

        On SQL Server this may return NULL or raise if the user lacks VIEW
        DEFINITION permissions on the remote objects.
        """
        cfg = self._require_cfg()
        eff_schema = self._effective_schema(schema)

        from_obj = _info_schema_4part(cfg, "VIEWS")
        stmt = (
            f"SELECT VIEW_DEFINITION "
            f"FROM {from_obj} "
            f"WHERE TABLE_NAME = :view_name"
        )
        params: dict[str, Any] = {"view_name": view_name}
        if eff_schema is not None:
            stmt += " AND TABLE_SCHEMA = :schema"
            params["schema"] = eff_schema

        try:
            row = connection.execute(text(stmt), params).first()
        except Exception:
            return None
        if not row:
            return None
        return row[0]

    def get_pk_constraint(
        self,
        connection: Connection,
        table_name: str,
        schema: str | None = None,
        **kw: Any,
    ) -> dict[str, Any]:
        """
        Return primary key constraint information.

        Best-effort:
        - If INFORMATION_SCHEMA constraints are accessible, use them.
        - Otherwise, return an empty PK unless pk_overrides provides one.
        """
        eff_schema = self._effective_schema(schema)

        # 1) Overrides
        if eff_schema is not None:
            cols = self._pk_overrides.get((eff_schema.lower(), table_name.lower()))
            if cols:
                return {"constrained_columns": cols, "name": None}
        cols = self._pk_overrides.get((None, table_name.lower()))
        if cols:
            return {"constrained_columns": cols, "name": None}

        # 2) INFORMATION_SCHEMA
        cfg = self._require_cfg()
        tc = _info_schema_4part(cfg, "TABLE_CONSTRAINTS")
        kcu = _info_schema_4part(cfg, "KEY_COLUMN_USAGE")

        stmt = (
            f"SELECT kcu.COLUMN_NAME, tc.CONSTRAINT_NAME "
            f"FROM {tc} tc "
            f"JOIN {kcu} kcu "
            f"  ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME "
            f" AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA "
            f" AND tc.TABLE_NAME = kcu.TABLE_NAME "
            f"WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' "
            f"  AND tc.TABLE_NAME = :table_name"
        )
        params: dict[str, Any] = {"table_name": table_name}
        if eff_schema is not None:
            stmt += " AND tc.TABLE_SCHEMA = :schema"
            params["schema"] = eff_schema
        stmt += " ORDER BY kcu.ORDINAL_POSITION"

        try:
            rows = connection.execute(text(stmt), params).all()
        except Exception:
            return {"constrained_columns": [], "name": None}

        if not rows:
            return {"constrained_columns": [], "name": None}

        constrained_columns = [r[0] for r in rows]
        name = rows[0][1]
        return {"constrained_columns": constrained_columns, "name": name}

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


