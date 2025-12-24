# Changelog

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


