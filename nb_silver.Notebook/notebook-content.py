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

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

emp_df = spark.read.format("delta")\
                    .load("abfss://0180ef92-64b2-4fab-a6c4-40e6b3f0d56e@onelake.dfs.fabric.microsoft.com/ac9e8e90-88bc-4264-9749-a0c58a032e55/Tables/bronze/employees")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

w = Window.partitionBy("EmployeeID")\
        .orderBy(greatest("UpdatedAt","CreatedAt").desc(),col("ingestion_time").desc())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

emp_df = emp_df.withColumn("rn",row_number().over(w))\
                .filter(col("rn") == 1)\
                .drop("rn","ingested_by","ingestion_time")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

emp_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("lh_employee_hub.silver.Employees")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dept_df = spark.read.format("delta")\
                .load("abfss://0180ef92-64b2-4fab-a6c4-40e6b3f0d56e@onelake.dfs.fabric.microsoft.com/ac9e8e90-88bc-4264-9749-a0c58a032e55/Tables/bronze/departments")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dept_df.drop("ingested_by","ingestion_time")\
        .write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("lh_employee_hub.silver.departments")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

titles_df = spark.read.format("delta")\
                    .load("abfss://0180ef92-64b2-4fab-a6c4-40e6b3f0d56e@onelake.dfs.fabric.microsoft.com/ac9e8e90-88bc-4264-9749-a0c58a032e55/Tables/bronze/jobtitles")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

titles_df.drop("ingested_by","ingestion_time")\
        .write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("lh_employee_hub.silver.jobtitles")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validation**

# CELL ********************

from pyspark.sql.functions import col


emp = spark.read.table("lh_employee_hub.silver.Employees")
dept = spark.read.table("lh_employee_hub.silver.Departments")
titles = spark.read.table("lh_employee_hub.silver.JobTitles")


duplicate_emp = emp.groupBy("EmployeeID") \
    .count() \
    .filter(col("count") > 1)

print("Duplicate EmployeeIDs:", duplicate_emp.count())


required_cols = ["EmployeeID", "FullName"]

null_condition = col(required_cols[0]).isNull()

for c in required_cols[1:]:
    null_condition = null_condition | col(c).isNull()

print("Rows with null required fields:", emp.filter(null_condition).count())


invalid_dept = emp.filter(col("dept_id").isNotNull()).join(
    dept.select(col("id").alias("dept_id")),
    on="dept_id",
    how="left_anti"
)

print("Invalid Department IDs:", invalid_dept.count())

invalid_titles = emp.filter(col("title_id").isNotNull()).join(
    titles.select(col("id").alias("title_id")),
    on="title_id",
    how="left_anti"
)

print("Invalid Job Title IDs:", invalid_titles.count())


print("Employees with null dept_id:",
      emp.filter(col("dept_id").isNull()).count())

print("Employees with null title_id:",
      emp.filter(col("title_id").isNull()).count())


bronze = spark.read.format("delta").load(
    "abfss://0180ef92-64b2-4fab-a6c4-40e6b3f0d56e@onelake.dfs.fabric.microsoft.com/ac9e8e90-88bc-4264-9749-a0c58a032e55/Tables/bronze/employees"
)

bronze_count = bronze.count()
distinct_employee_ids = bronze.select("EmployeeID").distinct().count()
silver_count = emp.count()

print("Bronze row count:", bronze_count)
print

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
