#!/usr/bin/env bash
set -euo pipefail

echo "Seeding remote (sql2)..."
/opt/mssql-tools18/bin/sqlcmd -C -S sql2 -U sa -P "${SA2_PASSWORD}" -i /mssql/remote.sql

echo "Configuring linked server on primary (sql1)..."
/opt/mssql-tools18/bin/sqlcmd -C -S sql1 -U sa -P "${SA1_PASSWORD}" -i /mssql/primary.sql

echo "Done."


