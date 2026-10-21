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

# Welcome to your new notebook
# Type here in the cell editor to add code!
df = spark.read.table("dbo.employee_events")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.select("*", "employee.*") \
    .write \
    .mode("append") \
    .format("delta") \
    .saveAsTable("bronze.employee_events")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_employee_hub.bronze.employee_events LIMIT 1000")
display(df)

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
