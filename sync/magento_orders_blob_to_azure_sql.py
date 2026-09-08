# Databricks notebook source
# MAGIC %md
# MAGIC # Load Magento orders JSON (Azure Blob / ADLS) -> Azure SQL (AELTC)
# MAGIC
# MAGIC Reads a day's worth of Magento order JSON from the ADLS Gen2 landing zone
# MAGIC and writes it to the `AELTC` Azure SQL database via Spark's built-in `jdbc`
# MAGIC data source (Microsoft SQL Server driver, bundled in the Databricks runtime).
# MAGIC
# MAGIC **Source layout**
# MAGIC ```
# MAGIC abfss://raw@tcadluksdatamgmtdev01.dfs.core.windows.net/
# MAGIC     LANDING/DEVCL1/MAGENTO/DEFAULT/ORDERS/_date=YYYYMMDD/_time=HHMMSS/<files>.json
# MAGIC ```
# MAGIC The notebook reads `_date=<run date>` and **recurses into every `_time=*`
# MAGIC folder below it** (`recursiveFileLookup=true`).
# MAGIC
# MAGIC **Storage auth:** already configured (Unity Catalog external location /
# MAGIC mount), so the `abfss://` path is read directly — no credentials set here.
# MAGIC
# MAGIC **Azure SQL auth:** Entra ID (Azure AD) service principal — an access token
# MAGIC from `tenant_id` + `client_id` + client secret (no SQL username/password).
# MAGIC
# MAGIC **Load type:** full overwrite with `truncate=true` (TRUNCATE + reload) of
# MAGIC the target stage table with the selected day's data. Nested JSON
# MAGIC (structs/arrays) is serialized to JSON strings so it fits relational columns.
# MAGIC
# MAGIC Schedule as a **Databricks Job / Workflow** with a daily trigger. See
# MAGIC `sync/README.md` for setup (secret scope, `azure-identity`, SP access to the
# MAGIC database, firewall, scheduling).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC Non-secret values come from job parameters (widgets). The service
# MAGIC principal's **client secret** lives in a Databricks secret scope backed by
# MAGIC Azure Key Vault. Nothing secret is hard-coded.

# COMMAND ----------

# Source (ADLS Gen2 landing zone)
dbutils.widgets.text("storage_account", "tcadluksdatamgmtdev01", "ADLS storage account")
dbutils.widgets.text("container", "raw", "ADLS container")
dbutils.widgets.text("base_path", "LANDING/DEVCL1/MAGENTO/DEFAULT/ORDERS", "Path above the _date partitions")
dbutils.widgets.text("load_date", "", "Date partition YYYYMMDD (blank = yesterday, UTC)")
dbutils.widgets.dropdown("multiline_json", "false", ["false", "true"], "multiLine JSON (one object spanning lines)")

# Target (Azure SQL)
dbutils.widgets.text("sql_server", "tcsqlsrvuksdatamgmtprod02.database.windows.net", "Azure SQL server")
dbutils.widgets.text("sql_database", "AELTC", "Azure SQL database")
dbutils.widgets.text("target_table", "Insights.MagentoOrdersStage", "Target schema.table")

# Entra ID service principal (Azure SQL auth)
dbutils.widgets.text("tenant_id", "afa21132-558b-4712-9dd1-72dbaf33febb", "Entra tenant_id")
dbutils.widgets.text("client_id", "e5a7dd31-c5b9-4fea-a286-7ee303c36985", "Service principal client_id")
dbutils.widgets.text("secret_scope", "key-vault", "Databricks secret scope (backed by Key Vault)")
dbutils.widgets.text("secret_client_secret_key", "datamgmt-sp-key", "Secret key: SP client secret (Key Vault secret name)")

from datetime import datetime, timedelta, timezone

storage_account = dbutils.widgets.get("storage_account")
container = dbutils.widgets.get("container")
base_path = dbutils.widgets.get("base_path").strip("/")
yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y%m%d")
load_date = dbutils.widgets.get("load_date").strip() or yesterday
multiline_json = dbutils.widgets.get("multiline_json") == "true"

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

source_path = (
    f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
    f"{base_path}/_date={load_date}/"
)
print(f"Source: {source_path}")
print(f"Target: {sql_database}.{target_table}")

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

jdbc_url = (
    f"jdbc:sqlserver://{sql_server}:1433;"
    f"database={sql_database};"
    "encrypt=true;trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;loginTimeout=30"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read the day's JSON (recursively) and write to Azure SQL
# MAGIC Full overwrite with `truncate=true`. First run creates the target with an
# MAGIC inferred schema; later runs TRUNCATE + reload. Nested JSON is serialized to
# MAGIC JSON strings by `jdbc_safe()` so it fits relational columns.

# COMMAND ----------

from pyspark.sql.functions import col, current_date, lit, to_json
from pyspark.sql.types import ArrayType, MapType, StructType


def jdbc_safe(df):
    """SQL Server/JDBC has no array/map/struct types. Serialize any complex
    column to a JSON string so it lands as nvarchar instead of failing with
    'Can't get JDBC type for array<...>'. Scalar columns pass through unchanged.
    """
    projected = []
    for field in df.schema.fields:
        if isinstance(field.dataType, (ArrayType, MapType, StructType)):
            projected.append(to_json(col(field.name)).alias(field.name))
        else:
            projected.append(col(field.name))
    return df.select(*projected)


def path_exists(p):
    try:
        dbutils.fs.ls(p)
        return True
    except Exception:
        return False


# Bail out cleanly if the day's partition hasn't landed yet -- but first list the
# _date=* partitions that DO exist under base_path, so a "no data" run shows the
# real layout (wrong date, casing, or naming) instead of failing silently.
if not path_exists(source_path):
    base_uri = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{base_path}/"
    print(f"No data at {source_path}")
    try:
        available = sorted(f.name.rstrip("/") for f in dbutils.fs.ls(base_uri))
        print(f"Partitions found under {base_uri} ({len(available)}):")
        for name in available:
            print("  ", name)
    except Exception as e:
        print(f"Could not list {base_uri}: {e}")
    dbutils.notebook.exit(f"No data at {source_path} - nothing to load.")

reader = spark.read.option("recursiveFileLookup", "true")
if multiline_json:
    reader = reader.option("multiLine", "true")
df = reader.json(source_path)

# Provenance columns: which partition this came from, and when it was loaded.
df = df.withColumn("SourceDate", lit(load_date)).withColumn("LoadDate", current_date())

df = jdbc_safe(df)

row_count = df.count()
print(f"{source_path} -> {sql_database}.{target_table}: {row_count:,} rows")

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
