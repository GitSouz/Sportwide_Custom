#!/usr/bin/env bash
#
# apply.sh — no-Flyway fallback: apply migrations to Azure SQL with sqlcmd.
#
# This runs every .sql file under ./migrations in filename order and records
# what has been applied in a simple dbo.schema_history table so scripts are
# not re-run. It is intentionally minimal; for real change management prefer
# Flyway (see README.md).
#
# Required environment variables:
#   AZ_SQL_SERVER    e.g. myserver.database.windows.net
#   AZ_SQL_DATABASE  e.g. sportwide
#   AZ_SQL_USER      SQL auth username
#   AZ_SQL_PASSWORD  SQL auth password
#
# Requires the `sqlcmd` client (part of mssql-tools / go-sqlcmd).
#
set -euo pipefail

: "${AZ_SQL_SERVER:?set AZ_SQL_SERVER}"
: "${AZ_SQL_DATABASE:?set AZ_SQL_DATABASE}"
: "${AZ_SQL_USER:?set AZ_SQL_USER}"
: "${AZ_SQL_PASSWORD:?set AZ_SQL_PASSWORD}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MIGRATIONS_DIR="$SCRIPT_DIR/migrations"

run_sql() {
  sqlcmd -S "$AZ_SQL_SERVER" -d "$AZ_SQL_DATABASE" \
    -U "$AZ_SQL_USER" -P "$AZ_SQL_PASSWORD" \
    -b -N -l 30 "$@"
}

# Ensure the tracking table exists.
run_sql -Q "
IF OBJECT_ID('dbo.schema_history', 'U') IS NULL
CREATE TABLE dbo.schema_history (
    filename     NVARCHAR(260) NOT NULL PRIMARY KEY,
    applied_utc  DATETIME2(0)  NOT NULL DEFAULT SYSUTCDATETIME()
);"

shopt -s nullglob
for file in $(ls "$MIGRATIONS_DIR"/*.sql | sort); do
  name="$(basename "$file")"
  already="$(run_sql -h -1 -W -Q \
    "SET NOCOUNT ON; SELECT COUNT(*) FROM dbo.schema_history WHERE filename = N'$name';")"
  if [ "$already" = "0" ]; then
    echo ">> applying $name"
    run_sql -i "$file"
    run_sql -Q "INSERT INTO dbo.schema_history (filename) VALUES (N'$name');"
  else
    echo "-- skipping $name (already applied)"
  fi
done

echo "All migrations applied."
