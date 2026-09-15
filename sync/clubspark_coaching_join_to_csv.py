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
# MAGIC ## Sources and outputs
# MAGIC - `SOURCES` — every JSON dataset to read, once. Each is read from its
# MAGIC   `_date=<load_date>/` folder and registered as a temp view named `view`.
# MAGIC   `records_path` is only needed when records are wrapped in a top-level
# MAGIC   array **field** (e.g. Magento's `items`). If the file's root is the array
# MAGIC   itself (flat objects), leave `records_path = None` — with multiline JSON,
# MAGIC   Spark reads each array element as a row directly.
# MAGIC - `OUTPUTS` — one entry per CSV to produce. Each has its own `query` (over
# MAGIC   the views above) and its own destination folder. Add as many as you like;
# MAGIC   the sources are read once and shared across all of them.
# MAGIC
# MAGIC TODO: set the real join keys / output columns once a sample of each feed is
# MAGIC available.

# COMMAND ----------

SOURCES = [
    {
        "view": "courses",
        "base_path": "LANDING/ECBGLB/CLUBSPARK/ECB_CLUBSPARK/COACHING_COURSES",
        "records_path": None,  # file root is a flat array -> rows read directly
    },
    {
        "view": "registrants",
        "base_path": "LANDING/ECBGLB/CLUBSPARK/ECB_CLUBSPARK/COACHING_REGISTRANTS",
        "records_path": None,  # file root is a flat array -> rows read directly
    },
]

# One entry per CSV. `output_base_path` is the folder root (YYYY/MM/DD is appended);
# the file is written as <output_name>_YYYYMMDD.csv. `query` runs over the views.
OUTPUTS = [
    {
        "output_base_path": "NATIVE/ECBGLB/LANDING/ECBGLB/CLUBSPARK/ECB_CLUBSPARK/COACHING",
        "output_name": "Coaching",
        "query": """
            SELECT
                c.ID               AS CourseID,
                c.Name             AS CourseName,
                c.Code             AS CourseCode,
                c.CoachingSchemeID AS CoachingSchemeID,
                c.Cost             AS Cost,
                c.MinimumAge       AS MinimumAge,
                c.MaximumAge       AS MaximumAge,
                -- TODO: real registrant columns/join key once a REGISTRANTS sample is shared
                r.ID               AS RegistrantID
            FROM courses c
            JOIN registrants r
                ON r.CourseID = c.ID   -- TODO: confirm the registrants -> courses key
        """,
    },
    # Add more outputs here, each to a different folder, e.g.:
    # {
    #     "output_base_path": "NATIVE/ECBGLB/LANDING/ECBGLB/CLUBSPARK/ECB_CLUBSPARK/COURSES",
    #     "output_name": "Courses",
    #     "query": "SELECT * FROM courses",
    # },
]

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

# Storage auth: connection string (holds the account key) from Key Vault.
storage_secret_scope = "key-vault"
storage_secret_key = "prod-blob-connection-string"

run_dt = datetime.strptime(load_date, "%Y%m%d").replace(tzinfo=timezone.utc)
run_time = datetime.now(timezone.utc).strftime("%H%M%S")  # HHMMSS of this run
partition = f"_date={load_date}/_time={run_time}"  # landing-zone output partition

print(f"Load date: {load_date}")
print(f"Partition: {partition}")
print(f"Outputs:   {len(OUTPUTS)}")

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
# MAGIC ## Read each source into a temp view, then write each OUTPUT's CSV
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

def write_single_csv(df, output_dir, output_file):
    """Coalesce to one part file and move it to the final CSV name."""
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


# Produce each output CSV from its own query, to its own folder.
for o in OUTPUTS:
    output_dir = (
        f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
        f"{o['output_base_path'].strip('/')}/{partition}"
    )
    output_file = f"{output_dir}/{o['output_name']}_{load_date}.csv"

    result = spark.sql(o["query"])
    result = result.withColumn("LoadDate", lit(run_dt.strftime("%Y-%m-%d")))
    result = csv_safe(result)

    row_count = result.count()
    write_single_csv(result, output_dir, output_file)
    print(f"Wrote {row_count:,} rows to {output_file}")
