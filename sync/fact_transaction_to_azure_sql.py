# Databricks notebook source
# MAGIC %md
# MAGIC # Sync Databricks -> Azure SQL (daily, full overwrite)
# MAGIC
# MAGIC Copies one or more Databricks tables/views into the `Sportwide` Azure SQL
# MAGIC database as a full daily mirror, using Spark's built-in `jdbc` data source
# MAGIC (the Microsoft SQL Server driver is bundled in the Databricks runtime).
# MAGIC
# MAGIC **Adding a table = one entry in the `TABLES` list below.** Everything else
# MAGIC (connection, auth, load logic) is shared.
# MAGIC
# MAGIC **Auth:** Entra ID (Azure AD) **service principal** — the notebook acquires
# MAGIC an access token from `tenant_id` + `client_id` + client secret and connects
# MAGIC with it (no SQL username/password).
# MAGIC
# MAGIC **Load type:** full overwrite with `truncate=true` (TRUNCATE + reload) per
# MAGIC table. `truncate` keeps each target table's shape, indexes, constraints and
# MAGIC grants; without it the driver would DROP and recreate the table.
# MAGIC
# MAGIC Schedule this notebook as a **Databricks Job / Workflow** with a daily
# MAGIC trigger. See `sync/README.md` for setup (secret scope, `azure-identity`,
# MAGIC granting the SP access to the database, firewall, scheduling).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tables to sync
# MAGIC One entry per table:
# MAGIC - `source` / `target` — schema.table names (source resolved under `catalog`).
# MAGIC - `where` — optional Spark-SQL predicate (omit or `None` = no filter).
# MAGIC - `columns` — optional list of columns to select (omit or `None` = all,
# MAGIC   i.e. `SELECT *`). Use this to load only the columns you need, or to skip
# MAGIC   an unwanted complex column entirely.

# COMMAND ----------

TABLES = [
    {
        "source": "global.dim_customer",
        "target": "Insights.CustomerStage",
        "where": "ProviderCode = 'STXWBK' AND coalesce(ExclusionFilter, false) <> true",
        "columns": ["CustomerID","DateofBirth","Address1","Address2","Address3","Address4","PostCode","City","Title","Gender","FirstName","LastName","Telephone","MobilePhone","EmailAddress","CreatedDate","'3' as OrgsId"],  # optional
    },
    {
        "source": "global.dim_product_band",
        "target": "Insights.ProductBandStage",
        "where": "coalesce(ExclusionFilter, false) <> true",
    },
    {
        "source": "global.dim_product_bridge",
        "target": "Insights.ProductStage",
        "where": (
            "coalesce(ExclusionFilter, false) <> true "
            "AND ProductType NOT LIKE 'Infer From%' "
            "AND ProductType != 'All Products'"
        ),
    },
    {
        "source": "global.vwfacttransaction",
        "target": "Insights.TransactionStage",
        "where": "ProviderCode = 'STXWBK'",
    },
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC Non-secret values come from job parameters (widgets). The service
# MAGIC principal's **client secret** lives in a Databricks secret scope backed by
# MAGIC Azure Key Vault. Nothing secret is hard-coded.

# COMMAND ----------

dbutils.widgets.text("catalog", "esxccc", "Source Unity Catalog")

dbutils.widgets.text("sql_server", "tcsqlsrvuksdatamgmtprod02.database.windows.net", "Azure SQL server")
dbutils.widgets.text("sql_database", "Sportwide", "Azure SQL database")

# Entra ID service principal
dbutils.widgets.text("tenant_id", "afa21132-558b-4712-9dd1-72dbaf33febb", "Entra tenant_id")
dbutils.widgets.text("client_id", "e5a7dd31-c5b9-4fea-a286-7ee303c36985", "Service principal client_id")
dbutils.widgets.text("secret_scope", "key-vault", "Databricks secret scope (backed by Key Vault)")
dbutils.widgets.text("secret_client_secret_key", "datamgmt-sp-key", "Secret key: SP client secret (Key Vault secret name)")

catalog = dbutils.widgets.get("catalog")

sql_server = dbutils.widgets.get("sql_server")
sql_database = dbutils.widgets.get("sql_database")

tenant_id = dbutils.widgets.get("tenant_id")
client_id = dbutils.widgets.get("client_id")
secret_scope = dbutils.widgets.get("secret_scope")
client_secret = dbutils.secrets.get('key-vault', 'datamgmt-sp-key')

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

jdbc_url = (
    f"jdbc:sqlserver://{sql_server}:1433;"
    f"database={sql_database};"
    "encrypt=true;trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;loginTimeout=30"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync each table (read with optional filter -> full overwrite)
# MAGIC Full overwrite with `truncate=true`. First run of a target creates it with
# MAGIC an inferred schema; later runs TRUNCATE + reload. For production-grade
# MAGIC column types/indexes, pre-create the target via a `db/` migration.

# COMMAND ----------

from pyspark.sql.functions import col, to_json
from pyspark.sql.types import ArrayType, MapType, StructType

spark.sql(f"USE CATALOG {catalog}")


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


def qualify_target(target: str, catalog: str) -> str:
    """Append the source catalog as a suffix on the target *table* name, keeping
    the schema prefix. e.g. "Insights.CustomerStage_OrgsId_3" + "esxccc"
    -> "Insights.CustomerStage_OrgsId_3_esxccc".
    """
    if "." in target:
        schema, tbl = target.split(".", 1)
        return f"{schema}.{tbl}_{catalog}"
    return f"{target}_{catalog}"


def sync_table(source, target, where=None, columns=None) -> int:
    select_list = ", ".join(columns) if columns else "*"
    query = f"SELECT {select_list} FROM {source}"
    if where:
        query += f" WHERE {where}"
    df = jdbc_safe(spark.sql(query))

    row_count = df.count()
    print(f"{source} -> {sql_database}.{target}: {row_count:,} rows")

    (
        df.write.format("jdbc")
        .mode("overwrite")
        .option("truncate", "true")  # TRUNCATE + reload; keep table shape/indexes/grants
        .option("url", jdbc_url)
        .option("dbtable", target)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("accessToken", access_token)  # Entra ID service-principal auth
        .option("batchsize", "10000")  # rows per insert batch (tune for throughput)
        .save()
    )
    return row_count


results = []
for t in TABLES:
    target = qualify_target(t["target"], catalog)
    n = sync_table(t["source"], target, t.get("where"), t.get("columns"))
    results.append((t["source"], target, n))

print("\nSync complete:")
for source, target, n in results:
    print(f"  {source} -> {target}: {n:,} rows")
