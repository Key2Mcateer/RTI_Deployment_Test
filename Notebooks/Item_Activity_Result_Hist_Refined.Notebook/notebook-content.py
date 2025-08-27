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

item_activity_result_raw_df = spark.read.table("eh_audit_log_monitor_history.raw.Item_Activity_Result")
display(item_activity_result_raw_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import from_utc_timestamp
df_convert_timestamp = item_activity_result_raw_df.withColumn("ItemActivityStartDateTime", from_utc_timestamp("ItemActivityStartDateTime", "America/New_York")) \
                                            .withColumn("ItemActivityEndDateTime", from_utc_timestamp("ItemActivityEndDateTime", "America/New_York")) 

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

df_IAR_with_key = df_convert_timestamp.withColumn(
    "ItemActivityResultKey",row_number().over(window_spec))
display(df_IAR_with_key)

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

joined_df = df_IAR_with_key.join(Execution_Key_DF, on="ItemRunID")
display(joined_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import IntegerType, DateType

Refined_df = joined_df.select(
    "ItemActivityResultKey",
    "ItemExecutionKey",
    "ItemResultName",
    "ItemResultValue"
)

display(Refined_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Refined_df.write.mode("overwrite").saveAsTable(f"Refined.ItemActivityResult")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
