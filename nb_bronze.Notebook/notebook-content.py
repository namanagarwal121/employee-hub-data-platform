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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

old_employee_df = spark.read.table("lh_employee_hub.bronze.employees")
last_watermark = old_employee_df.agg(max(greatest("CreatedAt","UpdatedAt"))).collect()[0][0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

new_emp_df = spark.read.table("lh_employee_hub.dbo.employees")\
                    .filter(greatest("CreatedAt", "UpdatedAt") > last_watermark )\
                    .withColumn("ingestion_time", current_timestamp())\
                    .withColumn("ingested_by", current_user())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

new_emp_df.write.mode("append")\
                .format("delta")\
                .option("mergeSchema","true")\
                .saveAsTable("lh_employee_hub.bronze.employees")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dept_df = spark.read.table("lh_employee_hub.dbo.Departments")\
                    .withColumn("ingestion_time", current_timestamp())\
                    .withColumn("ingested_by", current_user())
dept_df.write.mode("overwrite")\
            .format("delta")\
            .option("mergeSchema","true")\
            .saveAsTable("lh_employee_hub.bronze.departments")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

titles_df = spark.read.table("lh_employee_hub.dbo.JobTitles")\
                    .withColumn("ingestion_time", current_timestamp())\
                    .withColumn("ingested_by", current_user())
titles_df.write.mode("overwrite")\
            .format("delta")\
            .option("mergeSchema","true")\
            .saveAsTable("lh_employee_hub.bronze.JobTitles")

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
