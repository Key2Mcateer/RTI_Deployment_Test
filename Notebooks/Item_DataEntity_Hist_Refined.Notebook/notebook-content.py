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

# MARKDOWN ********************

# ## bring raw data into dataframe ##

# CELL ********************

item_DataEntity_raw_df = spark.read.table("eh_audit_log_monitor_history.raw.Item_DataEntity")
display(item_DataEntity_raw_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import from_utc_timestamp
df_convert_timestamp = item_DataEntity_raw_df.withColumn("DataEntityStartDateTime", from_utc_timestamp("DataEntityStartDateTime", "America/New_York")) \
                                            .withColumn("DataEntityEndDateTime", from_utc_timestamp("DataEntityEndDateTime", "America/New_York")) 

display(df_convert_timestamp) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import date_format

df_date_key = df_convert_timestamp.withColumn("DateKey", date_format("DataEntityStartDateTime", "yyyyMMdd").cast("int"))

display(df_date_key)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

item_DataEntity_raw_df.createOrReplaceTempView("raw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT count(*) FROM raw

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import when, col, row_number
from pyspark.sql.window import Window

window_spec = Window.orderBy("DataEntityActivityRunID")

df_IDE_with_key = df_date_key.withColumn(
    "ItemDataEntityKey",row_number().over(window_spec))
display(df_IDE_with_key)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_IDE_with_key.createOrReplaceTempView("IDE")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) FROM IDE

# METADATA ********************

# META {
# META   "language": "sparksql",
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

joined_df = df_IDE_with_key.join(Execution_Key_DF, on="ItemRunID")
display(joined_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import IntegerType, TimestampType

Refined_df = joined_df.select(\
"ItemDataEntityKey",
"ItemExecutionKey",
"DateKey",
"FlowType",
"DataEntityWorkspaceID",
"DataEntityWorkspaceName",
"DataEntityLakehouseID",
"DataEntityLakehouseName",
"DataEntityName",
"DataEntityNameSpace",
"DataEntityLoadType",
col("DataEntityStartDateTime").cast(TimestampType()),
col("DataEntityEndDateTime").cast(TimestampType()),
"DataEntityActivityType",
"DataEntityActivityRunID",
"DataEntityExecutionStatus",
"RelativePath",
"ABFSPath",
"FileNameWithExt",
"FileName",
col("RowsRead").cast(IntegerType()),
col("RowsCopied").cast(IntegerType()),
col("CopyDuration").cast(IntegerType()),
col("FilesRead").cast(IntegerType()),
col("FilesWritten").cast(IntegerType()),
col("InsertCount").cast(IntegerType()),
col("UpdateCount").cast(IntegerType()),
col("DeleteCount").cast(IntegerType()),
"MaxWatermark",
"CreatedBy",
col("CreatedDateTime").cast(TimestampType()),
col("IntegrationGroupKey").cast(IntegerType()),
col("IntegrationTaskKey").cast(IntegerType()),
col("RelatedIntegrationTaskKey").cast(IntegerType())
)

display(Refined_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Refined_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"Refined.ItemDataEntity")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
