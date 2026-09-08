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
# MAGIC **Storage auth:** the account key is taken from a connection string held in
# MAGIC Key Vault (`storage_secret_scope` / `storage_secret_key`) and set as
# MAGIC `fs.azure.account.key.<account>...` before any `abfss://` access.
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
# MAGIC ## Column mapping
# MAGIC Each file is a JSON object with a top-level array (`RECORDS_PATH`) of one
# MAGIC record per cart/order. We **explode** that array to one row per record
# MAGIC (aliased `rec`), then select `COLUMNS` — Spark-SQL expressions of the form
# MAGIC `rec.<json field> as <SQL column>`. Cast where you want a real type (e.g.
# MAGIC `cast(rec.created_at as timestamp)`); anything still nested is serialized to
# MAGIC a JSON string by `jdbc_safe()`.
# MAGIC
# MAGIC - Set `RECORDS_PATH = None` to keep the file's top-level rows as-is.
# MAGIC - Set `COLUMNS = None` to load every field (no mapping).

# COMMAND ----------

# The top-level array to explode into one row per record (None = don't explode).
RECORDS_PATH = "items"

# Map JSON fields -> SQL columns. Reference the exploded record as `rec`.
COLUMNS = [
    "rec.id as CartID",
    "cast(rec.created_at as timestamp) as created_at",
    "cast(rec.updated_at as timestamp) as updated_at",
    "rec.is_active as is_active",
    "rec.is_virtual as is_virtual",
    "rec.items_count as items_count",
    "rec.items_qty as items_qty",
    "rec.customer.id as CustomerID",          # nested under customer
    "rec.customer_is_guest as customer_is_guest",
    # add more fields here, e.g. "rec.billing_address.postcode as PostCode",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC Plain config values. The service principal's **client secret** and the
# MAGIC storage connection string are read from a Databricks secret scope backed by
# MAGIC Azure Key Vault. Nothing secret is hard-coded.

# COMMAND ----------

from datetime import datetime, timedelta, timezone

# Source (ADLS Gen2 landing zone)
storage_account = "tcadluksdatamgmtprod01"
container = "raw"
base_path = "LANDING/AELTC/MAGENTO/MAGENTO_DATA/CART".strip("/")
load_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y%m%d")  # yesterday (UTC); hard-code a YYYYMMDD to backfill
multiline_json = True  # files are single pretty-printed JSON objects spanning lines

# Storage auth: connection string (holds the account key) from Key Vault.
storage_secret_scope = "key-vault"
storage_secret_key = "prod-blob-connection-string"

# Target (Azure SQL)
sql_server = "tcsqlsrvuksdatamgmtprod02.database.windows.net"
sql_database = "AELTC"
target_table = "Insights.MagentoOrdersStage"

# Entra ID service principal (Azure SQL auth)
tenant_id = "afa21132-558b-4712-9dd1-72dbaf33febb"
client_id = "e5a7dd31-c5b9-4fea-a286-7ee303c36985"
secret_scope = "key-vault"
secret_client_secret_key = "datamgmt-sp-key"
client_secret = dbutils.secrets.get(secret_scope, secret_client_secret_key)

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
# MAGIC ## Configure storage access (account key from a connection string)
# MAGIC Read the connection string from Key Vault, pull the `AccountKey` out, and
# MAGIC set `fs.azure.account.key.<account>.dfs.core.windows.net` before any
# MAGIC `abfss://` access.
# MAGIC
# MAGIC **Serverless / shared (Spark Connect) clusters block runtime `fs.azure.*`
# MAGIC config**, so the `spark.conf.set` below is best-effort. If it's rejected,
# MAGIC the read still runs — relying on access granted another way. To make it
# MAGIC work on this account, use one of:
# MAGIC - a **single-user (dedicated) cluster** (runtime config allowed), or
# MAGIC - a **Unity Catalog external location** over the container (no key needed), or
# MAGIC - a **cluster Spark config**: `fs.azure.account.key.<account>.dfs.core.windows.net`
# MAGIC   = `{{secrets/<scope>/<raw-account-key-secret>}}` (raw key, not the
# MAGIC   connection string).

# COMMAND ----------


def account_key_from_connection_string(conn_str: str) -> str:
    # e.g. "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"
    # Split on ';' (the base64 key has no ';'); split each part on the FIRST '='
    # so the key's own '=' padding is preserved.
    parts = dict(p.split("=", 1) for p in conn_str.split(";") if "=" in p)
    key = parts.get("AccountKey")
    if not key:
        raise ValueError("Connection string has no AccountKey.")
    return key


conf_key = f"fs.azure.account.key.{storage_account}.dfs.core.windows.net"
try:
    connection_string = dbutils.secrets.get(storage_secret_scope, storage_secret_key)
    spark.conf.set(conf_key, account_key_from_connection_string(connection_string))
    print(f"Set {conf_key} from connection string.")
except Exception as e:
    # Serverless/shared clusters reject runtime fs.azure.* config
    # (CONFIG_NOT_AVAILABLE). Continue in case access is granted via a Unity
    # Catalog external location; if not, the read below fails with a 403.
    print(
        f"Could not set {conf_key} at runtime: {e}\n"
        "This is expected on serverless / shared (Spark Connect) clusters. "
        "Use a single-user cluster, a UC external location, or a cluster Spark "
        "config (see the cell notes). Continuing in case access is already "
        "configured..."
    )

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

# Explode the top-level array so there's one row per record (aliased `rec`).
if RECORDS_PATH:
    from pyspark.sql.functions import explode
    df = df.select(explode(col(RECORDS_PATH)).alias("rec"))

# Map JSON fields -> SQL columns (rec.<field> as <column>).
if COLUMNS:
    df = df.selectExpr(*COLUMNS)

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
