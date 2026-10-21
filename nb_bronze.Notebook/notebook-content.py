# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ac9e8e90-88bc-4264-9749-a0c58a032e55",
# META       "default_lakehouse_name": "lh_employee_hub",
# META       "default_lakehouse_workspace_id": "0180ef92-64b2-4fab-a6c4-40e6b3f0d56e",
# META       "known_lakehouses": [
# META         {
# META           "id": "ac9e8e90-88bc-4264-9749-a0c58a032e55"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Bronze: incremental + change-data-capture load
# Reads the app's replicated tables (the `dbo` shortcuts) and appends **only new versions** of each employee to `bronze.employees`. Every row gets:
# - `_op`: `I` insert, `U` update, `D` delete (a tombstone, written when an id disappears from the source)
# - `_row_hash`: a hash of the business columns, so identical versions are never appended twice
# - `_ingested_at`: when this run wrote the row
# Watermark: rows with `CreatedAt`/`UpdatedAt` at or after *last watermark − look-back* are re-checked. The hash makes the overlap harmless and catches rows committed late with an older timestamp. Set `full_compare = True` (for example on a daily run) to hash-compare **every** row, which also catches edits made directly in SQL without updating `UpdatedAt`.

# PARAMETERS CELL ********************

# Parameters (a pipeline Notebook activity can override these)
full_compare = False
lookback_minutes = 60

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SOURCE = "lh_employee_hub.dbo.employees"
BRONZE = "lh_employee_hub.bronze.employees"
# Business columns that define a version of an employee. Metadata columns are excluded.
HASH_COLS = ["id", "EmployeeID", "FullName", "Email", "Salary", "JoiningDate",
             "dept_id", "title_id", "CreatedAt", "UpdatedAt", "createdBy_id", "updatedBy_id"]

def row_hash():
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in HASH_COLS]), 256)

def with_hash(df):
    return df.withColumn("_row_hash", row_hash())

def col_or_null(df, name):
    return F.col(name) if name in df.columns else F.lit(None)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Current state of bronze: the latest version of each id (or nothing on the first run) ---
bronze_exists = spark.catalog.tableExists(BRONZE)
if bronze_exists:
    bronze_df = spark.read.table(BRONZE)
    # Rows written before this version of the notebook have null (or no) _op/_row_hash/_ingested_at.
    bronze_df = (bronze_df
                 .withColumn("_op", F.coalesce(col_or_null(bronze_df, "_op"), F.lit("I")))
                 .withColumn("_ingested_at", F.coalesce(col_or_null(bronze_df, "_ingested_at"), F.col("ingestion_time")))
                 .withColumn("_row_hash", F.coalesce(col_or_null(bronze_df, "_row_hash"), row_hash())))
    latest = (bronze_df
              .withColumn("_rn", F.row_number().over(
                  Window.partitionBy("id").orderBy(F.col("_ingested_at").desc_nulls_last())))
              .filter("_rn = 1").drop("_rn"))
    last_watermark = (bronze_df.filter(F.col("_op") != "D")
                      .agg(F.max(F.greatest("CreatedAt", F.coalesce("UpdatedAt", "CreatedAt")))).collect()[0][0])
else:
    latest = None
    last_watermark = None

print("bronze exists:", bronze_exists, "| last watermark:", last_watermark, "| full compare:", full_compare)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Candidate rows from the source ---
source_df = spark.read.table(SOURCE)
changed_at = F.greatest("CreatedAt", F.coalesce("UpdatedAt", "CreatedAt"))
if full_compare or last_watermark is None:
    candidates = source_df                     # first run, empty bronze, or a full compare
else:
    since = F.lit(last_watermark) - F.expr(f"INTERVAL {int(lookback_minutes)} MINUTES")
    candidates = source_df.filter(changed_at >= since)
candidates = with_hash(candidates)

# Keep only versions that bronze doesn't already hold as the latest live row for that id.
if latest is not None:
    live = latest.filter(F.col("_op") != "D").select("id", F.col("_row_hash").alias("_prev_hash"))
    known = latest.select("id").withColumn("_known", F.lit(True))
    new_versions = (candidates
                    .join(live, "id", "left")
                    .filter(F.col("_prev_hash").isNull() | (F.col("_prev_hash") != F.col("_row_hash")))
                    .join(known, "id", "left")
                    .withColumn("_op", F.when(F.col("_known") & F.col("_prev_hash").isNotNull(), "U").otherwise("I"))
                    .drop("_prev_hash", "_known"))
else:
    new_versions = candidates.withColumn("_op", F.lit("I"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Deletes: ids whose latest bronze version is live but that are gone from the source ---
if latest is not None:
    source_ids = source_df.select("id")
    tombstones = (latest.filter(F.col("_op") != "D")
                  .join(source_ids, "id", "left_anti")
                  .withColumn("_op", F.lit("D")))
else:
    tombstones = None

run_ts = F.current_timestamp()
def stamp(df):
    return (df.withColumn("_ingested_at", run_ts)
              .withColumn("ingestion_time", run_ts)      # kept for compatibility with earlier rows
              .withColumn("ingested_by", F.expr("current_user()")))

out = stamp(new_versions)
if tombstones is not None:
    out = out.unionByName(stamp(tombstones.drop("_ingested_at", "ingestion_time", "ingested_by")),
                          allowMissingColumns=True)

counts = {r["_op"]: r["count"] for r in out.groupBy("_op").count().collect()}
print("rows to append:", counts or "none")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if counts:
    (out.write.mode("append").format("delta")
        .option("mergeSchema", "true")
        .saveAsTable(BRONZE))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Lookups: small tables, refreshed in full each run ---
for src, dst in [("Departments", "departments"), ("JobTitles", "jobtitles")]:
    (spark.read.table(f"lh_employee_hub.dbo.{src}")
        .withColumn("ingestion_time", F.current_timestamp())
        .withColumn("ingested_by", F.expr("current_user()"))
        .write.mode("overwrite").format("delta")
        .option("overwriteSchema", "true")
        .saveAsTable(f"lh_employee_hub.bronze.{dst}"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
