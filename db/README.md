# Sportwide — Azure SQL Database (schema & migrations)

This folder holds the **versioned schema** for the Sportwide Azure SQL database.
The database itself lives in Azure; this repo only tracks the SQL that defines
and evolves it. Changes are applied as ordered, run-once **migrations**.

```
db/
├── migrations/              # Ordered SQL migrations (V1__..., V2__..., ...)
│   └── V1__initial_schema.sql
├── flyway.conf              # Flyway config (NON-SECRET; credentials via env)
├── apply.sh                 # sqlcmd fallback runner (no Flyway needed)
└── README.md                # this file
```

## How migrations work

- Each change to the schema is a new file: `V<n>__<description>.sql`
  (double underscore after the number). Numbers increase: `V1`, `V2`, `V3`…
- Each file runs **exactly once**. The runner records applied migrations in a
  history table, so re-running is safe.
- **Never edit a migration that has already been applied** to a shared or
  production database — add a new one instead.

## Prerequisites

- An Azure SQL server + database already provisioned (this repo does not
  provision it). If you need to create one quickly:
  ```bash
  az sql server create -n <server> -g <resource-group> -l <region> \
    -u <admin-user> -p <admin-password>
  az sql db create -g <resource-group> -s <server> -n <database> \
    --service-objective S0
  # Allow your client IP through the server firewall:
  az sql server firewall-rule create -g <resource-group> -s <server> \
    -n allow-my-ip --start-ip-address <ip> --end-ip-address <ip>
  ```
- Credentials with permission to create objects in the database.

## Option A — Apply with Flyway (recommended)

[Flyway](https://documentation.red-gate.com/fd) is language-agnostic and
supports Azure SQL. Install the CLI, then set credentials via environment
variables (nothing secret is committed):

```bash
export FLYWAY_URL="jdbc:sqlserver://<server>.database.windows.net:1433;database=<db>;encrypt=true;trustServerCertificate=false;loginTimeout=30"
export FLYWAY_USER="<sql-user>"
export FLYWAY_PASSWORD="<sql-password>"

cd db
flyway -configFiles=flyway.conf info      # show pending/applied migrations
flyway -configFiles=flyway.conf migrate   # apply pending migrations
```

For CI or local secrets, you can instead create a git-ignored
`db/flyway.local.conf` with `flyway.url` / `flyway.user` / `flyway.password`
and pass it via `-configFiles=flyway.conf,flyway.local.conf`.

## Option B — Apply with sqlcmd (no extra tooling)

If you don't want Flyway, `apply.sh` runs the migrations with `sqlcmd` and
tracks them in a `dbo.schema_history` table:

```bash
export AZ_SQL_SERVER="<server>.database.windows.net"
export AZ_SQL_DATABASE="<db>"
export AZ_SQL_USER="<sql-user>"
export AZ_SQL_PASSWORD="<sql-password>"

./db/apply.sh
```

> The two options use **different** history tables (`flyway_schema_history`
> vs `dbo.schema_history`). Pick one approach per database and stick with it.

## Secrets

Connection strings and passwords are **never** committed. Provide them through
environment variables, a git-ignored local config, or (best for production)
Azure Key Vault / managed identity. See `../.gitignore`.

## Adding a new migration

1. Create `db/migrations/V<next>__<what_changed>.sql`.
2. Write forward-only SQL (e.g. `ALTER TABLE`, `CREATE TABLE`).
3. Run `flyway migrate` (or `./db/apply.sh`).
4. Commit the new file.
