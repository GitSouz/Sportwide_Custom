# Databricks notebook source
# MAGIC %md
# MAGIC # Sync `vwfacttransaction` -> Azure SQL (daily, full overwrite)
# MAGIC
# MAGIC Reads `esxccc.global.vwfacttransaction` (filtered to ProviderCode `STXWBK`
# MAGIC for the current and previous order year) and writes a full daily mirror to
# MAGIC Azure SQL via the Spark SQL Server connector (bulk insert).
# MAGIC
# MAGIC **Auth:** Entra ID (Azure AD) **service principal** — the notebook acquires
# MAGIC an access token from `tenant_id` + `client_id` + client secret and connects
# MAGIC with it (no SQL username/password).
# MAGIC
# MAGIC **Load type:** full overwrite with `truncate=true` (TRUNCATE + reload).
# MAGIC Using `truncate` keeps the target table's shape, indexes, constraints and
# MAGIC grants; without it the connector would DROP and recreate the table.
# MAGIC
# MAGIC Schedule this notebook as a **Databricks Job / Workflow** with a daily
# MAGIC trigger. See `sync/README.md` for setup (secret scope, cluster libraries,
# MAGIC granting the SP access to the database, firewall, scheduling).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC Non-secret values come from job parameters (widgets). The service
# MAGIC principal's **client secret** lives in a Databricks secret scope backed by
# MAGIC Azure Key Vault. Nothing secret is hard-coded.

# COMMAND ----------

dbutils.widgets.text("catalog", "esxccc", "Source Unity Catalog")
dbutils.widgets.text("source_table", "global.vwfacttransaction", "Source schema.table")
dbutils.widgets.text("provider_code", "STXWBK", "ProviderCode filter")

dbutils.widgets.text("sql_server", "tcsqlsrvuksdatamgmtprod02.database.windows.net", "Azure SQL server")
dbutils.widgets.text("sql_database", "Sportwide", "Azure SQL database")
dbutils.widgets.text("target_table", "dbo.FactTransaction", "Target schema.table")

# Entra ID service principal
dbutils.widgets.text("tenant_id", "", "Entra tenant_id")
dbutils.widgets.text("client_id", "", "Service principal client_id")
dbutils.widgets.text("secret_scope", "kv-int-uks-prd-01", "Databricks secret scope (backed by Key Vault)")
dbutils.widgets.text("secret_client_secret_key", "datamgmt-sp-key", "Secret key: SP client secret (Key Vault secret name)")

catalog = dbutils.widgets.get("catalog")
source_table = dbutils.widgets.get("source_table")
provider_code = dbutils.widgets.get("provider_code")

sql_server = dbutils.widgets.get("sql_server")
sql_database = dbutils.widgets.get("sql_database")
target_table = dbutils.widgets.get("target_table")

tenant_id = dbutils.widgets.get("tenant_id")
client_id = dbutils.widgets.get("client_id")
secret_scope = dbutils.widgets.get("secret_scope")
client_secret = dbutils.secrets.get(secret_scope, dbutils.widgets.get("secret_client_secret_key"))

assert sql_server, "sql_server parameter is required"
assert sql_database, "sql_database parameter is required"
assert tenant_id, "tenant_id parameter is required"
assert client_id, "client_id parameter is required"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Acquire an Entra ID access token for Azure SQL
# MAGIC Uses the service principal (`tenant_id` + `client_id` + client secret) to
# MAGIC get a token scoped to Azure SQL, then passes it to the JDBC driver via the
# MAGIC `accessToken` option. Requires the `azure-identity` PyPI library on the
# MAGIC cluster (see `sync/README.md`).

# COMMAND ----------

from azure.identity import ClientSecretCredential

credential = ClientSecretCredential(
    tenant_id=tenant_id,
    client_id=client_id,
    client_secret=client_secret,
)
# Resource scope for Azure SQL Database.
access_token = credential.get_token("https://database.windows.net/.default").token

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read source with filters
# MAGIC ```
# MAGIC WHERE ProviderCode = 'STXWBK'
# MAGIC AND   YEAR(OrderDate) >= YEAR(GETDATE()) - 1
# MAGIC ```
# MAGIC Note: Spark SQL has no `GETDATE()`; the equivalent is `current_date()`.

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")

df = spark.sql(
    f"""
    SELECT *
    FROM {source_table}
    WHERE ProviderCode = '{provider_code}'
    AND   year(OrderDate) >= year(current_date()) - 1
    """
)

row_count = df.count()
print(f"Source rows to load: {row_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write full overwrite to Azure SQL
# MAGIC Uses Spark's built-in `jdbc` data source (the Microsoft SQL Server driver
# MAGIC is bundled in the Databricks runtime — no extra library needed).
# MAGIC Authenticates with the Entra `accessToken` (no user/password). First run
# MAGIC creates the target table (inferred schema); subsequent runs TRUNCATE +
# MAGIC reload. For production-grade column types/indexes, pre-create the table
# MAGIC via a `db/` migration (see `sync/README.md`).

# COMMAND ----------

jdbc_url = (
    f"jdbc:sqlserver://{sql_server}:1433;"
    f"database={sql_database};"
    "encrypt=true;trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;loginTimeout=30"
)

(
    df.write.format("jdbc")
    .mode("overwrite")
    .option("truncate", "true")  # TRUNCATE + reload; keep table shape/indexes/grants
    .option("url", jdbc_url)
    .option("dbtable", target_table)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("accessToken", access_token)  # Entra ID service-principal auth
    .option("batchsize", "10000")  # rows per insert batch (tune for throughput)
    .save()
)

print(f"Loaded {row_count:,} rows into {sql_database}.{target_table}")
