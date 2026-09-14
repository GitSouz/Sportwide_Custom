# Databricks notebook source
# MAGIC %md
# MAGIC # ClubSpark coaching: join JSON sources -> CSV (lake)
# MAGIC
# MAGIC Reads several ClubSpark JSON landing folders, registers each as a SQL temp
# MAGIC view, runs a **join query**, and writes the result as a single header CSV
# MAGIC back to the lake. Lake-to-lake, no Azure SQL.
# MAGIC
# MAGIC Each source lands under
# MAGIC `.../<DATASET>/_date=YYYYMMDD/_time=HHMMSS/<files>.json` and is read for the
# MAGIC chosen day (recursively over all `_time=*` folders).
# MAGIC
# MAGIC **Storage auth:** account key from a Key Vault connection string (works on a
# MAGIC single-user cluster; best-effort on serverless — see `sync/README.md`).
# MAGIC
# MAGIC Schedule as a **Databricks Job / Workflow** with a daily trigger.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sources and join query
# MAGIC - `SOURCES` — one entry per JSON dataset. Each is read from its
# MAGIC   `_date=<load_date>/` folder, exploded on `records_path` (the top-level
# MAGIC   array; `None` = don't explode) to one row per record, and registered as a
# MAGIC   temp view named `view` with the record's fields as columns.
# MAGIC - `QUERY` — the Spark SQL that joins those views into the CSV output.
# MAGIC
# MAGIC TODO: set the real join keys / output columns once a sample of each feed is
# MAGIC available.

# COMMAND ----------

SOURCES = [
    {
        "view": "courses",
        "base_path": "LANDING/ECBGLB/CLUBSPARK/ECB_CLUBSPARK/COACHING_COURSES",
        "records_path": "items",
    },
    {
        "view": "registrants",
        "base_path": "LANDING/ECBGLB/CLUBSPARK/ECB_CLUBSPARK/COACHING_REGISTRANTS",
        "records_path": "items",
    },
]

# The SQL that builds the CSV. Reference the views above. Adjust the join key and
# the selected columns to the real ClubSpark fields.
QUERY = """
SELECT
    c.id            AS CourseID,
    c.name          AS CourseName,
    r.id            AS RegistrantID,
    r.first_name    AS FirstName,
    r.last_name     AS LastName,
    r.status        AS RegistrationStatus
FROM courses c
JOIN registrants r
    ON r.course_id = c.id
"""

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

# Which day's _date=<...> partition to read for every source.
load_date = datetime.now(timezone.utc).strftime("%Y%m%d")  # today (UTC); hard-code YYYYMMDD to backfill
multiline_json = True  # True if each file is a single object/array spanning lines

# Output: date-partitioned CSV (YYYY/MM/DD folders). TODO: confirm output path.
output_base_path = "NATIVE/ECBGLB/LANDING/ECBGLB/CLUBSPARK/ECB_CLUBSPARK/COACHING".strip("/")
output_name = "Coaching"  # -> Coaching_YYYYMMDD.csv

# Storage auth: connection string (holds the account key) from Key Vault.
storage_secret_scope = "key-vault"
storage_secret_key = "prod-blob-connection-string"

run_dt = datetime.strptime(load_date, "%Y%m%d").replace(tzinfo=timezone.utc)
output_dir = (
    f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
    f"{output_base_path}/{run_dt.strftime('%Y/%m/%d')}"
)
output_file = f"{output_dir}/{output_name}_{load_date}.csv"

print(f"Load date: {load_date}")
print(f"Output:    {output_file}")

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
# MAGIC ## Read each source into a temp view, run the join, write one CSV
# MAGIC CSV can't hold nested structures, so any struct/array/map column in the
# MAGIC result is serialized to a JSON string first. Spark writes a folder of part
# MAGIC files, so we coalesce to one part and move it to the final file name.

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


# Read each source's _date partition into a temp view.
for s in SOURCES:
    src_path = (
        f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
        f"{s['base_path'].strip('/')}/_date={load_date}/"
    )
    if not path_exists(src_path):
        dbutils.notebook.exit(f"No data for '{s['view']}' at {src_path} - nothing to load.")

    reader = spark.read.option("recursiveFileLookup", "true")
    if multiline_json:
        reader = reader.option("multiLine", "true")
    df = reader.json(src_path)

    records_path = s.get("records_path")
    if records_path:
        # Explode the array, then lift the record's fields to top-level columns
        # so the SQL can reference them as <view>.<field>.
        df = df.select(explode(col(records_path)).alias("rec")).select("rec.*")

    df.createOrReplaceTempView(s["view"])
    print(f"View '{s['view']}': {df.count():,} rows from {src_path}")

# COMMAND ----------

result = spark.sql(QUERY)
result = result.withColumn("LoadDate", lit(run_dt.strftime("%Y-%m-%d")))
result = csv_safe(result)

row_count = result.count()
print(f"Join result -> {output_file}: {row_count:,} rows")

# Write one part file to a temp dir, then move it to the final CSV name.
tmp_dir = f"{output_dir}/_tmp"
(
    result.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(tmp_dir)
)
part = next(f.path for f in dbutils.fs.ls(tmp_dir) if f.name.endswith(".csv"))
dbutils.fs.rm(output_file, recurse=True)  # replace any existing file for this day
dbutils.fs.mv(part, output_file)
dbutils.fs.rm(tmp_dir, recurse=True)

print(f"Wrote {row_count:,} rows to {output_file}")
