from __future__ import annotations

import sys
from pathlib import Path

# Ensure the repository root is importable (so `import linked_sqlserver_dialect`
# works without an installed package).
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class FakeResult:
    def __init__(self, rows=None, mappings=None):
        self._rows = rows or []
        self._mappings = mappings or []

    def all(self):
        return self._rows

    def mappings(self):
        return FakeMappingsResult(self._mappings)


class FakeMappingsResult:
    def __init__(self, mappings):
        self._mappings = mappings

    def all(self):
        return list(self._mappings)


class FakeConnection:
    def __init__(self, *, rows_by_sql=None, mappings_by_sql=None):
        self.rows_by_sql = rows_by_sql or {}
        self.mappings_by_sql = mappings_by_sql or {}
        self.calls = []

    def execute(self, stmt, params=None):
        sql = getattr(stmt, "text", None) or str(stmt)
        self.calls.append((sql, params or {}))

        if sql in self.mappings_by_sql:
            return FakeResult(mappings=self.mappings_by_sql[sql])
        return FakeResult(rows=self.rows_by_sql.get(sql, []))


