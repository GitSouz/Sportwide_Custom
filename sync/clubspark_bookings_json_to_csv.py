# Databricks notebook source
# MAGIC %md
# MAGIC # ClubSpark bookings JSON (lake) -> CSV (lake)
# MAGIC
# MAGIC Reads a day's ClubSpark **bookings** JSON from ADLS Gen2, maps the fields
# MAGIC we want, and writes a single header CSV back to the lake landing zone.
# MAGIC No Azure SQL — this is a lake-to-lake reformat.
# MAGIC
# MAGIC **Source:** `abfss://raw@tcadluksdatamgmtprod01.dfs.core.windows.net/<INPUT_BASE_PATH>/...`
# MAGIC (read recursively).
# MAGIC
# MAGIC **Output:** one CSV at
# MAGIC `.../NATIVE/ECBGLB/LANDING/ECBGLB/CLUBSPARK/ECB_CLUBSPARK/BOOKINGS/YYYY/MM/DD/Bookings_YYYYMMDD.csv`.
# MAGIC
# MAGIC **Storage auth:** account key from a Key Vault connection string (works on a
# MAGIC single-user cluster; best-effort on serverless — see `sync/README.md`).
# MAGIC
# MAGIC Schedule as a **Databricks Job / Workflow** with a daily trigger.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column mapping
# MAGIC If the feed is an object with a top-level array (`RECORDS_PATH`), it's
# MAGIC exploded to one row per record (aliased `rec`); then `COLUMNS` selects
# MAGIC Spark-SQL expressions `rec.<field> as <column>`.
# MAGIC
# MAGIC - `RECORDS_PATH = None` keeps the file's rows as-is.
# MAGIC - `COLUMNS = None` writes every field (nested values are serialized to JSON
# MAGIC   strings so CSV can hold them).
# MAGIC
# MAGIC TODO: confirm the ClubSpark envelope and set the real field mapping once a
# MAGIC sample file is available. Defaults below assume the same `items` envelope as
# MAGIC the Magento feed.

# COMMAND ----------

# The top-level array to explode into one row per record (None = don't explode).
RECORDS_PATH = "items"

# Map JSON fields -> CSV columns. Reference the exploded record as `rec`.
# None = write all fields (complex ones become JSON strings).
COLUMNS = None
# e.g.
# COLUMNS = [
#     "rec.id as BookingID",
#     "cast(rec.created_at as timestamp) as created_at",
#     "rec.venue as Venue",
# ]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC Plain config values. The storage connection string is read from a
# MAGIC Databricks secret scope backed by Azure Key Vault. Nothing secret is
# MAGIC hard-coded.

# COMMAND ----------

from datetime import datetime, timezone

storage_account = "tcadluksdatamgmtprod01"
container = "raw"

# Source: where the bookings JSON is read FROM (recursive).
# TODO: set this to the real input location.
input_base_path = "LANDING/ECBGLB/CLUBSPARK/ECB_CLUBSPARK/BOOKINGS".strip("/")
multiline_json = True  # True if each file is a single object/array spanning lines

# Output: date-partitioned CSV landing zone (YYYY/MM/DD folders).
output_base_path = "NATIVE/ECBGLB/LANDING/ECBGLB/CLUBSPARK/ECB_CLUBSPARK/BOOKINGS".strip("/")

# Run date drives both the output YYYY/MM/DD folders and the file name.
run_dt = datetime.now(timezone.utc)  # hard-code a datetime to backfill a specific day

# Storage auth: connection string (holds the account key) from Key Vault.
storage_secret_scope = "key-vault"
storage_secret_key = "prod-blob-connection-string"

input_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{input_base_path}/"
output_dir = (
    f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
    f"{output_base_path}/{run_dt.strftime('%Y/%m/%d')}"
)
output_file = f"{output_dir}/Bookings_{run_dt.strftime('%Y%m%d')}.csv"

print(f"Input:  {input_path}")
print(f"Output: {output_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure storage access (account key from a connection string)
# MAGIC Set `fs.azure.account.key.<account>...` before any `abfss://` access.
# MAGIC Serverless / shared (Spark Connect) clusters block this at runtime, so it's
# MAGIC best-effort — use a single-user cluster or a UC external location there.

# COMMAND ----------


def account_key_from_connection_string(conn_str: str) -> str:
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
    print(
        f"Could not set {conf_key} at runtime: {e}\n"
        "Expected on serverless / shared (Spark Connect) clusters. Use a "
        "single-user cluster or a UC external location. Continuing in case "
        "access is already configured..."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read JSON, map fields, write one CSV
# MAGIC CSV can't hold nested structures, so any struct/array/map column is
# MAGIC serialized to a JSON string first. Spark writes a folder of part files, so
# MAGIC we coalesce to one part and move it to the final `...Bookings_YYYYMMDD.csv`.

# COMMAND ----------

from pyspark.sql.functions import col, explode, lit, to_json
from pyspark.sql.types import ArrayType, MapType, StructType


def csv_safe(df):
    """CSV has no array/map/struct types. Serialize any complex column to a JSON
    string so it lands as text. Scalar columns pass through unchanged.
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


if not path_exists(input_path):
    dbutils.notebook.exit(f"No data at {input_path} - nothing to load.")

reader = spark.read.option("recursiveFileLookup", "true")
if multiline_json:
    reader = reader.option("multiLine", "true")
df = reader.json(input_path)

if RECORDS_PATH:
    df = df.select(explode(col(RECORDS_PATH)).alias("rec"))

if COLUMNS:
    df = df.selectExpr(*COLUMNS)

df = df.withColumn("LoadDate", lit(run_dt.strftime("%Y-%m-%d")))
df = csv_safe(df)

row_count = df.count()
print(f"{input_path} -> {output_file}: {row_count:,} rows")

# Write one part file to a temp dir, then move it to the final CSV name.
tmp_dir = f"{output_dir}/_tmp"
(
    df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(tmp_dir)
)
part = next(f.path for f in dbutils.fs.ls(tmp_dir) if f.name.endswith(".csv"))
dbutils.fs.rm(output_file, recurse=True)  # replace any existing file for this day
dbutils.fs.mv(part, output_file)
dbutils.fs.rm(tmp_dir, recurse=True)

print(f"Wrote {row_count:,} rows to {output_file}")
