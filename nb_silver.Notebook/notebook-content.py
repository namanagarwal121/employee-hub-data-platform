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

# ## Silver: current state of each employee
# # - Keeps the **latest bronze version per `id`** (the primary key), not per `EmployeeID`, because the app can reuse an `EmployeeID` after a delete.
# - **Drops employees whose latest version is a delete** (`_op = 'D'`), so deletes flow through to gold.
# - Writes the same columns, in the same order, as `employee_hub_warehouse.silver.employees`, so the pipeline's copy step keeps working.
# - **Fails the run** on real problems (duplicate ids, missing required fields, broken department/title links) instead of only printing them.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

LH = "lh_employee_hub"
# Column order matches the warehouse table silver.employees.
EMPLOYEE_COLS = ["id", "CreatedAt", "Email", "EmployeeID", "FullName", "JoiningDate", "Salary",
                 "UpdatedAt", "createdBy_id", "dept_id", "title_id", "updatedBy_id"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze = spark.read.table(f"{LH}.bronze.employees")
ingested = (F.coalesce(F.col("_ingested_at"), F.col("ingestion_time"))
            if "_ingested_at" in bronze.columns else F.col("ingestion_time"))
op = F.coalesce(F.col("_op"), F.lit("I")) if "_op" in bronze.columns else F.lit("I")

latest = (bronze
          .withColumn("_ingested", ingested)
          .withColumn("_op_", op)
          .withColumn("_rn", F.row_number().over(
              Window.partitionBy("id").orderBy(F.col("_ingested").desc_nulls_last(),
                                               F.greatest("CreatedAt", F.coalesce("UpdatedAt", "CreatedAt")).desc())))
          .filter("_rn = 1"))

deleted_count = latest.filter(F.col("_op_") == "D").count()
employees = latest.filter(F.col("_op_") != "D").select(*EMPLOYEE_COLS)
print("current employees:", employees.count(), "| deleted (excluded):", deleted_count)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(employees.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{LH}.silver.employees"))

for name in ["departments", "jobtitles"]:
    (spark.read.table(f"{LH}.bronze.{name}")
        .drop("ingested_by", "ingestion_time")
        .write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{LH}.silver.{name}"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validation.** Raises an error, failing the pipeline, when silver isn't safe to load into gold.

# CELL ********************

emp = spark.read.table(f"{LH}.silver.employees")
dept = spark.read.table(f"{LH}.silver.departments")
titles = spark.read.table(f"{LH}.silver.jobtitles")

problems = {
    "duplicate ids": emp.groupBy("id").count().filter("count > 1").count(),
    "duplicate EmployeeIDs": emp.groupBy("EmployeeID").count().filter("count > 1").count(),
    "missing required fields": emp.filter(F.col("id").isNull() | F.col("EmployeeID").isNull()
                                          | F.col("FullName").isNull() | F.col("Email").isNull()).count(),
    "department link to a missing department": emp.filter(F.col("dept_id").isNotNull())
        .join(dept.select(F.col("id").alias("dept_id")), "dept_id", "left_anti").count(),
    "job title link to a missing title": emp.filter(F.col("title_id").isNotNull())
        .join(titles.select(F.col("id").alias("title_id")), "title_id", "left_anti").count(),
}
# Not fatal: gold maps these to the "Unknown" member instead of dropping the employee.
warnings = {
    "no department": emp.filter(F.col("dept_id").isNull()).count(),
    "no job title": emp.filter(F.col("title_id").isNull()).count(),
}
for k, v in {**problems, **warnings}.items():
    print(f"{k}: {v}")

failed = {k: v for k, v in problems.items() if v}
if failed:
    raise ValueError(f"Silver validation failed: {failed}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
