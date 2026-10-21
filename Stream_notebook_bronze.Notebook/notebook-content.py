# Fabric notebook source


# MARKDOWN ********************

# ## Stream bronze: landing → typed, de-duplicated event log
# # The Eventstream writes raw events to `dbo.employee_events`. This notebook moves them to `bronze.employee_events`:
# - **Incremental:** a Structured Streaming read with a checkpoint picks up only rows that arrived since the last run (`availableNow` processes the backlog, then stops, so it runs fine as a scheduled or pipeline step).
# - **Idempotent:** one row per `event_id`. Re-sent events and replays are skipped with a MERGE, not appended twice.
# - **Typed and flat:** the `employee` struct becomes columns, `Salary` becomes `DECIMAL(12,2)` (Eventstream inferred it as a whole number) and `JoiningDate` becomes a `DATE`.
# - **Validated, not filtered:** every event is kept, with `_is_valid` and `_reject_reason`. The apply step only uses valid events, and rejects stay queryable.
# # Set `reset = True` once to rebuild the table and checkpoint from the full landing history (for example after changing this notebook).
# # The second half turns bronze into **SQL-ready tables** in the `sql_ready` schema, one per app table, in that table's exact column layout.

# PARAMETERS CELL ********************

reset = False
create_missing_lookups = False   # True: unknown departments / job titles become new lookup rows instead of rejects

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

# MARKDOWN ********************

# ## SQL-ready tables: what the app's database still needs
# # For each stream employee, the **newest valid event** is compared with the app's current row (read through the `dbo` shortcuts). Only the differences are written, so a rerun never repeats a change that has already landed:
# # | Table in `sql_ready` | Apply to | How |
# | --- | --- | --- |
# | `AppUsers_insert` | `AppUsers` | INSERT: the `hr-feed` user that stream rows are attributed to (only on the first run) |
# | `Departments_insert`, `JobTitles_insert` | `Departments`, `JobTitles` | INSERT: only with `create_missing_lookups = True`; otherwise unknown names are rejected |
# | `Employees_insert` | `Employees` | INSERT: stream employees the app doesn't have yet |
# | `Employees_update` | `Employees` | UPDATE by `id`: employees whose stream state differs from the app |
# | `Employees_delete` | `Employees` | DELETE by `id`: leavers still in the app |
# | `AuditEvents_insert` | `AuditEvents` | INSERT: one audit row per change above, as the app writes them |
# | `rejects` | not applied | events that can't be applied, with the reason |
# # Apply the lookups and `AppUsers_insert` first, then the employee tables, then `AuditEvents_insert`. Rules:
# - **Newest wins:** late or re-sent events lose to the newest valid event for that `EmployeeID`.
# - **People win over the stream:** a change made in the app after the newest event isn't overwritten, and an employee deleted in the app isn't re-created.
# - **Stable ids:** `id` values are derived from `EmployeeID` / `event_id`, so a rerun before you apply produces the same ids.

# CELL ********************

from pyspark.sql import Window

spark.conf.set("spark.sql.session.timeZone", "UTC")
APP = "lh_employee_hub.dbo"
OUT = "lh_employee_hub.sql_ready"
STREAM_USER = "hr-feed@stream.employeehub"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OUT}")


def stable_uuid(*parts):
    """A deterministic UUID-formatted id from the given columns (md5, so a rerun gives the same id)."""
    h = F.md5(F.concat_ws("|", *[F.col(p).cast("string") if isinstance(p, str) else p for p in parts]))
    return F.concat_ws("-", F.substring(h, 1, 8), F.substring(h, 9, 4), F.substring(h, 13, 4),
                       F.substring(h, 17, 4), F.substring(h, 21, 12))


app_emp = spark.read.table(f"{APP}.Employees")
app_dept = spark.read.table(f"{APP}.Departments")
app_title = spark.read.table(f"{APP}.JobTitles")
app_users = spark.read.table(f"{APP}.AppUsers")
EMP_COLS = ["id", "EmployeeID", "FullName", "Email", "Salary", "JoiningDate", "CreatedAt",
            "UpdatedAt", "dept_id", "title_id", "createdBy_id", "updatedBy_id"]

events = spark.read.table(BRONZE)
valid = events.filter("_is_valid")

# The attribution user: reuse it if the app already has it.
existing_user = app_users.filter(F.lower("Email") == STREAM_USER).select("id").limit(1).collect()
stream_user_id = existing_user[0]["id"] if existing_user else \
    spark.range(1).select(stable_uuid(F.lit("app-user"), F.lit(STREAM_USER)).alias("id")).first()["id"]
appusers_insert = (spark.range(0 if existing_user else 1)
                   .select(F.lit(stream_user_id).alias("id"), F.lit(STREAM_USER).alias("Email"),
                           F.current_timestamp().alias("FirstSeenAt")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Newest valid event per employee, plus when it was first created ---
w = Window.partitionBy("EmployeeID").orderBy(F.col("event_time").desc(), F.col("event_id").desc())
latest = (valid.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
          .join(valid.filter(F.col("event_type") == "employee.created")
                     .groupBy("EmployeeID").agg(F.min("event_time").alias("created_time")),
                "EmployeeID", "left"))

# --- Lookups: department / job title names -> app ids (case- and space-insensitive) ---
dept_ids = app_dept.select(F.lower(F.trim("Name")).alias("_dept_key"), F.col("id").alias("dept_id"))
title_ids = app_title.select(F.lower(F.trim("Title")).alias("_title_key"), F.col("id").alias("title_id"))
upserts = (latest.filter(F.col("event_type") != "employee.deleted")
           .withColumn("_dept_key", F.lower("Department")).withColumn("_title_key", F.lower("JobTitle"))
           .join(dept_ids, "_dept_key", "left").join(title_ids, "_title_key", "left"))

missing_dept = upserts.filter(F.col("dept_id").isNull()).select("Department").distinct()
missing_title = upserts.filter(F.col("title_id").isNull()).select("JobTitle").distinct()
if create_missing_lookups:
    departments_insert = missing_dept.select(stable_uuid(F.lit("dept"), F.lower("Department")).alias("id"),
                                             F.col("Department").alias("Name"))
    jobtitles_insert = missing_title.select(stable_uuid(F.lit("title"), F.lower("JobTitle")).alias("id"),
                                            F.col("JobTitle").alias("Title"))
    upserts = (upserts
               .withColumn("dept_id", F.coalesce("dept_id", stable_uuid(F.lit("dept"), F.lower("Department"))))
               .withColumn("title_id", F.coalesce("title_id", stable_uuid(F.lit("title"), F.lower("JobTitle")))))
else:
    departments_insert = missing_dept.limit(0).select(F.lit(None).cast("string").alias("id"), F.col("Department").alias("Name"))
    jobtitles_insert = missing_title.limit(0).select(F.lit(None).cast("string").alias("id"), F.col("JobTitle").alias("Title"))

# --- Compare with the app ---
app_cur = app_emp.select(*[F.col(c).alias(f"app_{c}") for c in EMP_COLS]) \
                 .withColumn("app_changed_at", F.coalesce("app_UpdatedAt", "app_CreatedAt"))
# Employees deleted in the app after the event (seen by the CDC bronze as a tombstone).
app_deleted = (spark.read.table("lh_employee_hub.bronze.employees").filter(F.col("_op") == "D")
               .groupBy("EmployeeID").agg(F.max("_ingested_at").alias("app_deleted_at")))

candidates = (upserts.join(app_cur, upserts.EmployeeID == app_cur.app_EmployeeID, "left")
                     .join(app_deleted, "EmployeeID", "left"))
email_owner = app_emp.select(F.lower("Email").alias("_email"), F.col("EmployeeID").alias("_email_owner"))
candidates = (candidates.withColumn("_email", F.lower("Email"))
              .join(email_owner, "_email", "left")
              .withColumn("_email_dupes", F.count("*").over(Window.partitionBy("_email"))))

reject_reason = (
    F.when(F.col("dept_id").isNull(), F.concat(F.lit("unknown Department: "), F.col("Department")))
     .when(F.col("title_id").isNull(), F.concat(F.lit("unknown JobTitle: "), F.col("JobTitle")))
     .when(F.col("_email_owner").isNotNull() & (F.col("_email_owner") != F.col("EmployeeID")),
           F.concat(F.lit("Email already used by EmployeeID "), F.col("_email_owner").cast("string")))
     .when(F.col("_email_dupes") > 1, F.lit("Email shared by several stream employees"))
)
candidates = candidates.withColumn("_reject", reject_reason)
ok = candidates.filter(F.col("_reject").isNull())

is_new = F.col("app_id").isNull() & (F.col("app_deleted_at").isNull() | (F.col("app_deleted_at") < F.col("event_time")))
differs = ~(F.col("FullName").eqNullSafe(F.col("app_FullName")) & F.col("Email").eqNullSafe(F.col("app_Email"))
            & F.col("Salary").eqNullSafe(F.col("app_Salary").cast("decimal(12,2)"))
            & F.col("JoiningDate").eqNullSafe(F.col("app_JoiningDate").cast("date"))
            & F.col("dept_id").eqNullSafe(F.col("app_dept_id")) & F.col("title_id").eqNullSafe(F.col("app_title_id")))
newer = F.col("event_time") > F.col("app_changed_at")

employees_insert = ok.filter(is_new).select(
    stable_uuid(F.lit("employee"), "EmployeeID").alias("id"), "EmployeeID", "FullName", "Email", "Salary",
    "JoiningDate", F.coalesce("created_time", "event_time").alias("CreatedAt"),
    F.when(F.col("event_type") == "employee.updated", F.col("event_time")).alias("UpdatedAt"),
    "dept_id", "title_id", F.lit(stream_user_id).alias("createdBy_id"),
    F.when(F.col("event_type") == "employee.updated", F.lit(stream_user_id)).alias("updatedBy_id"),
    "event_id", "event_type", "change")

employees_update = ok.filter(F.col("app_id").isNotNull() & differs & newer).select(
    F.col("app_id").alias("id"), "EmployeeID", "FullName", "Email", "Salary", "JoiningDate",
    F.col("app_CreatedAt").alias("CreatedAt"), F.col("event_time").alias("UpdatedAt"), "dept_id", "title_id",
    F.col("app_createdBy_id").alias("createdBy_id"), F.lit(stream_user_id).alias("updatedBy_id"),
    "event_id", "event_type", "change")

deletes = (latest.filter(F.col("event_type") == "employee.deleted")
           .join(app_cur, latest.EmployeeID == app_cur.app_EmployeeID, "inner")
           .filter(F.col("event_time") > F.col("app_changed_at")))
employees_delete = deletes.select(F.col("app_id").alias("id"), "EmployeeID", "event_id", "event_type", "change")

rejects = (events.filter(~F.col("_is_valid"))
           .select("event_id", "EmployeeID", "event_type", "event_time", "change", F.col("_reject_reason").alias("reason"))
           .unionByName(candidates.filter(F.col("_reject").isNotNull())
                        .select("event_id", "EmployeeID", "event_type", "event_time", "change", F.col("_reject").alias("reason"))))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Audit rows, as the app writes them (one per change) ---
def audit(df, action, record_col="id"):
    return df.select(
        stable_uuid(F.lit("audit"), "event_id", F.lit(action)).alias("id"),
        F.current_timestamp().alias("OccurredAt"),
        F.lit(STREAM_USER).alias("UserEmail"),
        F.lit(action).alias("Action"),
        F.lit("Employee").alias("EntityType"),
        F.col(record_col).alias("RecordId"),
        F.lit("success").alias("Status"),
        F.to_json(F.struct(F.lit("hr-feed-simulator").alias("source"), "event_id",
                           "change", "EmployeeID")).alias("Details"),
        F.lit(None).cast("string").alias("ErrorDetails"))

audit_events_insert = (audit(employees_insert, "employee.create")
                       .unionByName(audit(employees_update, "employee.update"))
                       .unionByName(audit(employees_delete, "employee.delete")))

# --- Write: each run replaces the previous pending set ---
outputs = {
    "AppUsers_insert": appusers_insert,
    "Departments_insert": departments_insert,
    "JobTitles_insert": jobtitles_insert,
    "Employees_insert": employees_insert.select(*EMP_COLS),
    "Employees_update": employees_update.select(*EMP_COLS),
    "Employees_delete": employees_delete.select("id", "EmployeeID"),
    "AuditEvents_insert": audit_events_insert,
    "rejects": rejects,
}
for name, df in outputs.items():
    df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(f"{OUT}.{name}")

summary = [(name, spark.read.table(f"{OUT}.{name}").count()) for name in outputs]
display(spark.createDataFrame(summary, "table string, pending_rows long"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
