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

# ## Stream bronze: landing → typed, de-duplicated event log
#
# The Eventstream writes raw events to `dbo.employee_events`. This notebook moves them to `bronze.employee_events`:
# - **Incremental:** a Structured Streaming read with a checkpoint picks up only rows that arrived since the last run (`availableNow` processes the backlog, then stops, so it runs fine as a scheduled or pipeline step).
# - **Idempotent:** one row per `event_id`. Re-sent events and replays are skipped with a MERGE, not appended twice.
# - **Typed and flat:** the `employee` struct becomes columns, `Salary` becomes `DECIMAL(12,2)` (Eventstream inferred it as a whole number) and `JoiningDate` becomes a `DATE`.
# - **Validated, not filtered:** every event is kept, with `_is_valid` and `_reject_reason`. The apply step only uses valid events, and rejects stay queryable.
#
# Set `reset = True` once to rebuild the table and checkpoint from the full landing history (for example after changing this notebook).

# PARAMETERS CELL ********************

reset = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from delta.tables import DeltaTable

LANDING = "lh_employee_hub.dbo.employee_events"
BRONZE = "lh_employee_hub.bronze.employee_events"
CHECKPOINT = "Files/checkpoints/stream_bronze_employee_events"

STREAM_ID_MIN = 900_001                       # the producer's reserved EmployeeID range
EVENT_TYPES = ["employee.created", "employee.updated", "employee.deleted"]

# Invalid dates such as 2026-02-30 become NULL instead of raising an upgrade error.
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

# A table from the first version of this notebook (struct + flattened columns) is replaced.
old_shape = spark.catalog.tableExists(BRONZE) and "_is_valid" not in spark.read.table(BRONZE).columns
if reset or old_shape:
    print("rebuilding", BRONZE, "from the full landing history")
    spark.sql(f"DROP TABLE IF EXISTS {BRONZE}")
    try:
        notebookutils.fs.rm(CHECKPOINT, True)
    except Exception:
        pass                                  # no checkpoint yet

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE} (
    event_id        STRING,
    event_type      STRING,
    event_time      TIMESTAMP,
    source          STRING,
    schema_version  BIGINT,
    change          STRING,
    EmployeeID      BIGINT,
    FullName        STRING,
    Email           STRING,
    Salary          DECIMAL(12,2),
    JoiningDate     DATE,
    JoiningDateRaw  STRING,
    Department      STRING,
    JobTitle        STRING,
    _is_valid       BOOLEAN,
    _reject_reason  STRING,
    _ingested_at    TIMESTAMP,
    _batch_id       BIGINT
) USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def shape(df):
    """Flatten, type and validate the raw Eventstream rows."""
    e = "employee."
    typed = df.select(
        F.col("event_id"),
        F.col("event_type"),
        F.col("event_time").cast("timestamp").alias("event_time"),
        F.col("source"),
        F.col("schema_version").cast("bigint").alias("schema_version"),
        F.col("change"),
        F.col(e + "EmployeeID").cast("bigint").alias("EmployeeID"),
        F.trim(F.col(e + "FullName")).alias("FullName"),
        F.lower(F.trim(F.col(e + "Email"))).alias("Email"),
        F.col(e + "Salary").cast("decimal(12,2)").alias("Salary"),
        F.to_date(F.col(e + "JoiningDate"), "yyyy-MM-dd").alias("JoiningDate"),
        F.col(e + "JoiningDate").cast("string").alias("JoiningDateRaw"),
        F.trim(F.col(e + "Department")).alias("Department"),
        F.trim(F.col(e + "JobTitle")).alias("JobTitle"),
    )
    upsert = F.col("event_type").isin("employee.created", "employee.updated")
    # First failing rule wins. Department/title existence is checked in the apply step,
    # where the app's lookup tables are available.
    reason = (
        F.when(F.col("event_id").isNull(), "missing event_id")
         .when(~F.col("event_type").isin(EVENT_TYPES) | F.col("event_type").isNull(), "unknown event_type")
         .when(F.col("event_time").isNull(), "missing event_time")
         .when(F.col("EmployeeID").isNull(), "missing EmployeeID")
         .when(F.col("EmployeeID") < STREAM_ID_MIN, "EmployeeID outside the stream range")
         .when(upsert & (F.col("FullName").isNull() | (F.length("FullName") == 0) | (F.length("FullName") > 150)), "invalid FullName")
         .when(upsert & (F.col("Email").isNull() | ~F.col("Email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$") | (F.length("Email") > 254)), "invalid Email")
         .when(upsert & (F.col("Salary").isNull() | (F.col("Salary") < 0)), "invalid Salary")
         .when(upsert & F.col("JoiningDate").isNull(), "invalid JoiningDate")
         .when(upsert & (F.col("Department").isNull() | F.col("JobTitle").isNull()), "missing Department or JobTitle")
    )
    return (typed
            .withColumn("_reject_reason", reason)
            .withColumn("_is_valid", F.col("_reject_reason").isNull()))


def upsert_batch(batch_df, batch_id):
    rows = (shape(batch_df)
            .dropDuplicates(["event_id"])                 # duplicates inside one micro-batch
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_batch_id", F.lit(batch_id).cast("bigint")))
    (DeltaTable.forName(spark, BRONZE).alias("t")
        .merge(rows.alias("s"), "t.event_id = s.event_id")  # duplicates across batches
        .whenNotMatchedInsertAll()
        .execute())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

before = spark.read.table(BRONZE).count()
query = (spark.readStream.table(LANDING)
         .writeStream
         .foreachBatch(upsert_batch)
         .option("checkpointLocation", CHECKPOINT)
         .trigger(availableNow=True)
         .start())
query.awaitTermination()

bronze = spark.read.table(BRONZE)
print(f"new events: {bronze.count() - before} | total: {bronze.count()}")
display(bronze.groupBy("event_type", "_is_valid", "_reject_reason").count().orderBy("event_type"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
