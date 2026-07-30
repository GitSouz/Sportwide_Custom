# Databricks notebook source
# MAGIC %md
# MAGIC # Sync `vwfacttransaction` -> Azure SQL (daily, full overwrite)
# MAGIC
# MAGIC Reads `esxccc.global.vwfacttransaction` (filtered to ProviderCode `STXWBK`
# MAGIC for the current and previous order year) and writes a full daily mirror to
# MAGIC Azure SQL via the Spark SQL Server connector (bulk insert).
# MAGIC
# MAGIC **Load type:** full overwrite with `truncate=true` (TRUNCATE + reload).
# MAGIC Using `truncate` keeps the target table's shape, indexes, constraints and
# MAGIC grants; without it the connector would DROP and recreate the table.
# MAGIC
# MAGIC Schedule this notebook as a **Databricks Job / Workflow** with a daily
# MAGIC trigger. See `sync/README.md` for setup (secrets, firewall, scheduling).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC Connection details come from job parameters (widgets); the SQL login lives
# MAGIC in a Databricks secret scope backed by Azure Key Vault. Nothing secret is
# MAGIC hard-coded.

# COMMAND ----------

dbutils.widgets.text("catalog", "esxccc", "Source Unity Catalog")
dbutils.widgets.text("source_table", "global.vwfacttransaction", "Source schema.table")
dbutils.widgets.text("provider_code", "STXWBK", "ProviderCode filter")

dbutils.widgets.text("sql_server", "", "Azure SQL server (<name>.database.windows.net)")
dbutils.widgets.text("sql_database", "", "Azure SQL database")
dbutils.widgets.text("target_table", "dbo.FactTransaction", "Target schema.table")

dbutils.widgets.text("secret_scope", "kv-scope", "Databricks secret scope")
dbutils.widgets.text("secret_user_key", "sql-user", "Secret key: SQL user")
dbutils.widgets.text("secret_password_key", "sql-password", "Secret key: SQL password")

catalog = dbutils.widgets.get("catalog")
source_table = dbutils.widgets.get("source_table")
provider_code = dbutils.widgets.get("provider_code")

sql_server = dbutils.widgets.get("sql_server")
sql_database = dbutils.widgets.get("sql_database")
target_table = dbutils.widgets.get("target_table")

secret_scope = dbutils.widgets.get("secret_scope")
sql_user = dbutils.secrets.get(secret_scope, dbutils.widgets.get("secret_user_key"))
sql_password = dbutils.secrets.get(secret_scope, dbutils.widgets.get("secret_password_key"))

assert sql_server, "sql_server parameter is required"
assert sql_database, "sql_database parameter is required"

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
# MAGIC First run creates the target table (inferred schema); subsequent runs
# MAGIC TRUNCATE + reload. For production-grade column types/indexes, pre-create
# MAGIC the table via a `db/` migration (see `sync/README.md`).

# COMMAND ----------

jdbc_url = (
    f"jdbc:sqlserver://{sql_server}:1433;"
    f"database={sql_database};"
    "encrypt=true;trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;loginTimeout=30"
)

(
    df.write.format("com.microsoft.sqlserver.jdbc.spark")
    .mode("overwrite")
    .option("truncate", "true")  # TRUNCATE + reload; keep table shape/indexes/grants
    .option("url", jdbc_url)
    .option("dbtable", target_table)
    .option("user", sql_user)
    .option("password", sql_password)
    .option("schemaCheckEnabled", "false")
    .save()
)

print(f"Loaded {row_count:,} rows into {sql_database}.{target_table}")
