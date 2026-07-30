# Sync — Databricks → Azure SQL (daily)

Daily full-overwrite sync of ticket-transaction data from Databricks into the
Sportwide Azure SQL database (the same database whose schema is versioned under
[`../db`](../db)).

| | |
|---|---|
| **Source** | `esxccc.global.vwfacttransaction` |
| **Filter** | `ProviderCode = 'STXWBK'` and `YEAR(OrderDate) >= YEAR(current_date) - 1` (current + previous order year) |
| **Target** | `dbo.FactTransaction` in Azure SQL |
| **Load type** | Full overwrite — `TRUNCATE` + reload every run |
| **Engine** | Spark SQL Server connector (`com.microsoft.sqlserver.jdbc.spark`, bulk insert) |
| **Auth** | Entra ID (Azure AD) **service principal** — access token, no SQL login |
| **Schedule** | Databricks Job / Workflow, daily trigger |

Notebook: [`fact_transaction_to_azure_sql.py`](fact_transaction_to_azure_sql.py)
(a Databricks notebook stored as source).

## 1. Service principal & secret

Auth is via an Entra ID (Azure AD) **service principal**. You need three things:
`tenant_id`, `client_id`, and a **client secret** — `tenant_id` + `client_id`
alone cannot authenticate. Store only the client secret in a Databricks **secret
scope** (ideally Key Vault-backed); the tenant/client IDs are non-secret job
parameters.

```bash
databricks secrets create-scope kv-scope
databricks secrets put-secret kv-scope sp-client-secret   # the SP client secret
```

The service principal must also exist as a user **inside the Azure SQL
database** with rights to truncate/insert into the target table. Connect once as
an Entra admin and run:

```sql
-- <sp-display-name> is the app registration's name in Entra
CREATE USER [<sp-display-name>] FROM EXTERNAL PROVIDER;
ALTER ROLE db_datawriter ADD MEMBER [<sp-display-name>];
ALTER ROLE db_ddladmin  ADD MEMBER [<sp-display-name>];  -- needed for TRUNCATE / first-run table create
```

## 2. Cluster libraries

Install on the job cluster:

- **Spark SQL Server connector** (Maven) — match your Spark/Scala version, e.g.
  `com.microsoft.azure:spark-mssql-connector_2.12:1.4.0`.
- **`azure-identity`** (PyPI) — used to acquire the Entra access token.

## 3. Create the job

Point a Databricks Job at the notebook and pass these parameters (widgets):

| Parameter | Example |
|---|---|
| `catalog` | `esxccc` |
| `source_table` | `global.vwfacttransaction` |
| `provider_code` | `STXWBK` |
| `sql_server` | `<server>.database.windows.net` |
| `sql_database` | `<db>` |
| `target_table` | `dbo.FactTransaction` |
| `tenant_id` | `<entra-tenant-id>` |
| `client_id` | `<service-principal-client-id>` |
| `secret_scope` | `kv-scope` |
| `secret_client_secret_key` | `sp-client-secret` |

Add a **daily schedule** (off-peak hour), enable **retries**, and set a failure
notification. That's the whole pipeline.

## 4. Azure SQL firewall

Allow the Databricks workspace outbound IPs (or its VNet, if VNet-injected)
through the Azure SQL **server firewall**, otherwise the connection is refused.

## Notes & gotchas

- **`truncate=true` is important.** In `overwrite` mode without it, the connector
  DROPs and recreates the target table each run — losing indexes, constraints
  and grants. With `truncate=true` it keeps the table and just replaces rows.
- **Not atomic.** During truncate+reload the table is briefly empty; readers can
  see a partial table mid-load. If that matters, load a staging table and swap.
- **First run infers the schema.** For production-grade column types and
  indexes, pre-create `dbo.FactTransaction` as a `db/migrations/V<n>__...sql`
  migration (define the columns to match the view) so the table shape is
  versioned like the rest of the schema; the notebook will then just
  truncate + reload it.
- **`GETDATE()` → `current_date()`.** The original filter used T-SQL
  `YEAR(GETDATE())`; Spark SQL has no `GETDATE()`, so the notebook uses
  `year(current_date())`, which is equivalent.
- **Full reload cost.** This reloads all matching rows every day. If the
  filtered set grows large, switch the write step to incremental append (date
  watermark) or staging + `MERGE`.
