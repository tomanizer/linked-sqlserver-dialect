# Changelog

## 0.2.1 (2025-12-24)

### Changed
- Support Python >= 3.11.

## 0.2.0 (2025-12-24)

### Added
- View reflection:
  - `get_view_names()`
  - `get_view_definition()` (best-effort)
- Primary key handling:
  - `get_pk_constraint()` (best-effort via INFORMATION_SCHEMA)
  - `pk_overrides` to supply PKs when metadata is missing (URL query param or `connect_args`)
- Local devcontainer + Docker Compose sandbox for manual testing (linked server + seeded schema).

## 0.1.0 (2025-12-24)

Initial release.

### Added
- `linked_mssql+pyodbc` dialect for SQL Server linked servers.
- Reflection via `INFORMATION_SCHEMA` (no stored procedure usage):
  - `get_table_names()`
  - `get_columns()`
- Support for passing `linked_server`, `database`, and optional default `schema` via:
  - URL query params
  - `connect_args`
- Unit tests and a tiny end-to-end smoke test script.


