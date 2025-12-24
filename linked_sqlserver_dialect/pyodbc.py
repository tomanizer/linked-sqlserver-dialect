from __future__ import annotations

from sqlalchemy.engine import URL

from .base import LinkedMSDialect, LinkedServerConfig


class LinkedMSDialect_pyodbc(LinkedMSDialect):
    driver = "pyodbc"

    def create_connect_args(self, url: URL):
        # Pull our custom options from query params so they don't get passed into
        # pyodbc.connect(). Keep everything else as normal for MSSQL+pyodbc.
        q = dict(url.query)

        linked_server = q.pop("linked_server", None)
        database = q.pop("database", None)
        schema = q.pop("schema", None)

        if linked_server and database and self._linked_cfg is None:
            self._linked_cfg = LinkedServerConfig(
                str(linked_server), str(database), str(schema) if schema else None
            )

        # Rebuild URL with remaining query params.
        url = url.set(query=q)
        return super().create_connect_args(url)


