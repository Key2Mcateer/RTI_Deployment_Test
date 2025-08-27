# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "91411bb2-3cbb-4e1e-94df-d07f49079540",
# META       "default_lakehouse_name": "eh_audit_log_monitor_history",
# META       "default_lakehouse_workspace_id": "e3ae1e1f-85de-4c90-b852-bbc5afe2d174",
# META       "known_lakehouses": [
# META         {
# META           "id": "91411bb2-3cbb-4e1e-94df-d07f49079540"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

execution_error_raw_df = spark.read.table("eh_audit_log_monitor_history.raw.Execution_Error").orderBy("ItemRunID")
display(execution_error_raw_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import from_utc_timestamp
df_convert_timestamp = execution_error_raw_df.withColumn("CreatedDateTime", from_utc_timestamp("CreatedDateTime", "America/New_York"))

display(df_convert_timestamp) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import when, col, row_number
from pyspark.sql.window import Window

window_spec = Window.orderBy("ItemRunID")

df_EE_with_key = df_convert_timestamp.withColumn(
    "ExecutionErrorKey",row_number().over(window_spec))
display(df_EE_with_key)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Item_Execution_DF = spark.read.table("eh_audit_log_monitor_history.refined.ItemExecution")
display(Item_Execution_DF)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Execution_Key_DF = Item_Execution_DF.select(
    "ItemExecutionKey",
    "ItemRunID")
display(Execution_Key_DF)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Item_Data_Entity_DF = spark.read.table("eh_audit_log_monitor_history.refined.itemdataentity")
display(Item_Data_Entity_DF)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DataEntity_Key_DF = Item_Data_Entity_DF.select(
    "ItemDataEntityKey",
    "ItemExecutionKey"
).orderBy("ItemExecutionKey", "ItemDataEntityKey")
display(DataEntity_Key_DF)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

EE_join_df = df_EE_with_key.join(Execution_Key_DF, ["ItemRunID"], "inner")
display(EE_join_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

joined_df = df_EE_with_key.join(Execution_Key_DF, ["ItemRunID"], "inner")\
                          .join(DataEntity_Key_DF, ["ItemExecutionKey"], "inner")
                          
                          
display(joined_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import IntegerType, TimestampType

Refined_df = joined_df.select(
    "ExecutionErrorKey",
    "ItemExecutionKey",
    "ExecutionErrorMessage",
    "ExecutionErrorScope",
    col("ItemDataEntityKey").alias("ExecutionErrorReferenceKey"),
    "CreatedBy",
    col("CreatedDateTime").cast(TimestampType())
)

display(Refined_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Refined_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"Refined.ExecutionError")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
