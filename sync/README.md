# Sync — Databricks → Azure SQL (daily)

Daily full-overwrite sync of one or more Databricks tables/views from Databricks
into the Sportwide Azure SQL database (the same database whose schema is
versioned under [`../db`](../db)).

| | |
|---|---|
| **Target DB** | `Sportwide` on `tcsqlsrvuksdatamgmtprod02.database.windows.net` |
| **Load type** | Full overwrite — `TRUNCATE` + reload every run, per table |
| **Engine** | Spark built-in `jdbc` data source (Microsoft SQL Server driver, bundled in DBR) |
| **Auth** | Entra ID (Azure AD) **service principal** — access token, no SQL login |
| **Schedule** | Databricks Job / Workflow, daily trigger |

Notebook: [`fact_transaction_to_azure_sql.py`](fact_transaction_to_azure_sql.py)
(a Databricks notebook stored as source).

### Tables synced

The tables to sync are defined in the `TABLES` list at the top of the notebook.
**Adding a table is one entry** — `source`, `target`, and an optional `where`
filter; connection and auth are shared across all of them.

The **source catalog is appended to the target table name** at runtime (schema
kept): with `catalog = esxccc`, a `target` of `Insights.CustomerStage` is written
to `Insights.CustomerStage_esxccc`. Keep the `target` values below catalog-free —
the suffix is added by `qualify_target()`.

| Source (under `catalog`) | Target (before catalog suffix) | Filter |
|---|---|---|
| `global.dim_customer` | `Insights.CustomerStage` | `ProviderCode = 'STXWBK'` and not excluded (selected columns only) |
| `global.dim_product_band` | `Insights.ProductBandStage` | not excluded |
| `global.dim_product_bridge` | `Insights.ProductStage` | not excluded, real product types |
| `global.vwfacttransaction` | `Insights.TransactionStage` | `ProviderCode = 'STXWBK'` |

## 1. Service principal & secret

Auth is via an Entra ID (Azure AD) **service principal**. You need three things:
`tenant_id`, `client_id`, and a **client secret** — `tenant_id` + `client_id`
alone cannot authenticate. The tenant/client IDs are non-secret job parameters;
the client secret is read from Azure Key Vault through a Databricks secret scope.

The client secret already lives in Key Vault:

| | |
|---|---|
| Key Vault | `kv-int-uks-prd-01` |
| Secret name | `datamgmt-sp-key` |

Create a Databricks **secret scope backed by that Key Vault** (Databricks reads
secrets by *scope* + *key*, not by vault name). Name the scope the same as the
vault for clarity — do this once in the UI at
`https://<workspace-url>#secrets/createScope`, pointing it at the
`kv-int-uks-prd-01` resource. The notebook then reads
`dbutils.secrets.get("kv-int-uks-prd-01", "datamgmt-sp-key")`.

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

- **`azure-identity`** (PyPI) — used to acquire the Entra access token.

The write uses Spark's built-in `jdbc` data source; the Microsoft SQL Server
JDBC driver ships with the Databricks runtime, so no connector JAR is required.
(If you later need faster bulk `BULK INSERT` throughput on large loads, install
the `com.microsoft.azure:spark-mssql-connector` Maven library matching your
Spark version and switch the write's `.format(...)` back to
`com.microsoft.sqlserver.jdbc.spark`.)

## 3. Create the job

Point a Databricks Job at the notebook and pass these parameters (widgets):

| Parameter | Example |
|---|---|
| `catalog` | `esxccc` |
| `provider_code` | `STXWBK` |
| `sql_server` | `tcsqlsrvuksdatamgmtprod02.database.windows.net` |
| `sql_database` | `Sportwide` |
| `tenant_id` | `<entra-tenant-id>` |
| `client_id` | `<service-principal-client-id>` |
| `secret_scope` | `kv-int-uks-prd-01` |
| `secret_client_secret_key` | `datamgmt-sp-key` |

(The list of tables is code, not a parameter — edit the `TABLES` list in the
notebook.)

Add a **daily schedule** (off-peak hour), enable **retries**, and set a failure
notification. That's the whole pipeline.

## Adding another table

Add one entry to the `TABLES` list at the top of the notebook:

```python
{
    "source": "global.<view>",
    "target": "Insights.<Table>",
    "where": "<optional predicate or None>",
    "columns": ["Col1", "Col2"],   # optional; omit or None = SELECT *
},
```

- `source` is resolved under the `catalog` widget (`esxccc`).
- `where` is an optional **Spark SQL** predicate (use `current_date()`, not
  `GETDATE()`; use bare/backtick identifiers, not `[brackets]`); `None` = no
  filter. Put `{provider_code}` where you want the `provider_code` widget value
  injected, e.g. `ProviderCode = '{provider_code}'` — don't hard-code it.
- `columns` is an optional whitelist — list only the columns you want, or omit
  for all. Handy to keep the target narrow or to drop an unwanted complex
  column.
- The SP needs write access to each new target (the `db_datawriter` /
  `db_ddladmin` grant already covers the whole database).

## 4. Azure SQL firewall

Allow the Databricks workspace outbound IPs (or its VNet, if VNet-injected)
through the Azure SQL **server firewall**, otherwise the connection is refused.

## Notes & gotchas

- **`truncate=true` is important.** In `overwrite` mode without it, the connector
  DROPs and recreates the target table each run — losing indexes, constraints
  and grants. With `truncate=true` it keeps the table and just replaces rows.
- **Not atomic.** During truncate+reload the table is briefly empty; readers can
  see a partial table mid-load. If that matters, load a staging table and swap.
- **Complex columns → JSON.** SQL Server has no array/map/struct type, so a
  `SELECT *` over a source with such a column fails with *"Can't get JDBC type
  for array<...>"*. The notebook's `jdbc_safe()` step auto-serializes any
  complex column to a JSON string (lands as `nvarchar`). To drop it instead, use
  the per-table `columns` whitelist.
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
