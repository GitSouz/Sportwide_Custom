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

Every table selects an explicit `columns` list that includes
`current_date() as LoadDate`, so each row carries the run date.

| Source (under `catalog`) | Target (before catalog suffix) | Filter |
|---|---|---|
| `global.dim_customer` | `Insights.CustomerStage` | not excluded |
| `global.dim_product_band` | `Insights.ProductBandStage` | not excluded |
| `global.dim_product` | `Insights.ProductStage` | classified, not excluded, real product types |
| `global.vwfacttransaction` | `Insights.TransactionStage` | `YEAR(OrderDate) >= YEAR(current_date) - 1` (current + previous order year) |

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
  filter.
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
- **Load date.** Each table's `columns` list includes
  `current_date() as LoadDate`, so every row records the run date (lands as a
  SQL Server `date`). Drop it from a table's `columns` to omit it there.
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

---

# Load — Azure Blob (ADLS) JSON → Azure SQL (daily)

A second notebook loads a day's Magento **order JSON** from the ADLS Gen2
landing zone into the `AELTC` Azure SQL database.

| | |
|---|---|
| **Source** | `abfss://raw@tcadluksdatamgmtprod01.dfs.core.windows.net/LANDING/AELTC/MAGENTO/MAGENTO_DATA/CART/_date=<YYYYMMDD>/` (recurses all `_time=*` folders) |
| **Target DB** | `Insights.MagentoOrdersStage` in `AELTC` on `tcsqlsrvuksdatamgmtprod02.database.windows.net` |
| **Load type** | Full overwrite — `TRUNCATE` + reload with the selected day's data |
| **Storage auth** | Account key from a Key Vault connection string (scope `key-vault`, secret `prod-blob-connection-string`), set as `fs.azure.account.key.<account>...` |
| **Azure SQL auth** | Same Entra ID service principal (access token) |

Notebook:
[`magento_orders_blob_to_azure_sql.py`](magento_orders_blob_to_azure_sql.py).

## How it works

- **Which day.** `load_date` defaults to **yesterday** (UTC, `YYYYMMDD`) and
  forms the `_date=` partition — so a daily morning run picks up the previous
  day's completed data. Override the widget to backfill a specific day.
- **Recursion.** `recursiveFileLookup=true` reads every file under the day's
  folder, so all `_time=HHMMSS` subfolders are picked up automatically.
- **JSON shape.** `multiline_json=false` (default) treats each file as JSON
  Lines (one object per line). If a file is a single pretty-printed object/array
  spanning lines, set it to `true`.
- **Field mapping.** Each file is an object with a top-level array of records
  (`RECORDS_PATH`, default `items`). The notebook explodes that array to one row
  per record (aliased `rec`), then selects `COLUMNS` — Spark-SQL expressions
  `rec.<json field> as <SQL column>`, e.g. `cast(rec.created_at as timestamp) as
  created_at`. Edit `COLUMNS` (near the top of the notebook) to map the fields
  you want. `RECORDS_PATH = None` skips the explode; `COLUMNS = None` loads every
  field unmapped.
- **Nested JSON.** Order JSON is deeply nested; `jdbc_safe()` serializes any
  struct/array/map column to a JSON string so it lands as `nvarchar`.
- **Provenance.** Each row gets `SourceDate` (the partition date) and `LoadDate`
  (the run date).
- **Storage auth.** The account key is parsed from the Key Vault connection
  string (`storage_secret_scope` / `storage_secret_key`, default scope
  `key-vault`, secret `prod-blob-connection-string`) and set as
  `fs.azure.account.key.<account>.dfs.core.windows.net` before any read. The
  scope is the same Databricks secret scope used for the SP client secret.
- **Cluster type matters for storage.** Serverless / shared (Spark Connect)
  clusters **block** runtime `fs.azure.*` config, so the account-key set is
  best-effort there (fails with `CONFIG_NOT_AVAILABLE`, then continues). To read
  the blob on those clusters, grant access via a **Unity Catalog external
  location** over the container. On a **single-user (dedicated) cluster** the
  runtime key set works as written; alternatively bake it into the cluster's
  Spark config as `fs.azure.account.key.<account>...` =
  `{{secrets/<scope>/<raw-account-key-secret>}}` (the raw key, not the
  connection string).
- **No data yet.** If the day's partition doesn't exist, the notebook lists the
  `_date=*` partitions that do exist under `base_path`, then exits cleanly
  (`No data ...`) rather than failing — safe for an early-morning run.

## Adjusting / other feeds

The notebook is single-source, driven by widgets. To load a different feed
(e.g. `.../MAGENTO/DEFAULT/INVOICES`), point `base_path` and `target_table` at
it — or clone the notebook per feed. Auth and the write logic are unchanged.
The default write is a **full overwrite** with the selected day only; switch
`.mode("overwrite")` → `.mode("append")` (drop `truncate`) to accumulate days.
