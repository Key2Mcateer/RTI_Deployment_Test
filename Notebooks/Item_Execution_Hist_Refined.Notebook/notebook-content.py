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

# ## **_ItemExecution_ need to be produced first for this schema, other tables will rely on the new _ItemExecutionKey_ that we are adding here** ##

# MARKDOWN ********************

# ## bring raw data into dataframe ##

# CELL ********************

item_execution_raw_df = spark.read.table("eh_audit_log_monitor_history.raw.Item_Execution")
display(item_execution_raw_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import from_utc_timestamp
df_convert_timestamp = item_execution_raw_df.withColumn("EventDateTime", from_utc_timestamp("EventDateTime", "America/New_York")) \
                                            .withColumn("TriggerDateTime", from_utc_timestamp("TriggerDateTime", "America/New_York")) \
                                            .withColumn("CreatedDateTime", from_utc_timestamp("CreatedDateTime", "America/New_York"))

display(df_convert_timestamp)   

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Partition by **_ItemRunID_** and add an index so we can pivot later ##

# CELL ********************

from pyspark.sql.functions import when, col, row_number
from pyspark.sql.window import Window

window_spec = Window.partitionBy("ItemRunID").orderBy("EventDateTime")
status_row_num_df = df_convert_timestamp.withColumn("index", row_number().over(window_spec))

display(status_row_num_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Pivot **_ItemRunStatus_** based on the previously produced **_Index_**
# We need one row of data per **ItemRunID** but we also need both "_In Progress_" and "_Failed_" or "_Succeeded_" statuses.

# CELL ********************

pivot_status_df = status_row_num_df.select(
    "ItemRunStatus",
    "ItemRunID",
    "index"
    ).groupBy("ItemRunID").pivot("index").agg({"ItemRunStatus": "first"})

display(pivot_status_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pivot_status_df = pivot_status_df.withColumnRenamed("1", "Start_Status").withColumnRenamed("2", "End_Status")
display(pivot_status_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## We also need **_ItemRunStartDateTime_** and _**ItemRunEndDateTime**_ for the schema ##
# **ItemRunStartDateTime** correlates with the In Progress status while **ItemRunEndDateTime** correlates with the Failed or Succeeded status.

# CELL ********************

pivot_event_date_df = status_row_num_df.select(
    "EventDateTime",
    "ItemRunID",
    "index"
    ).groupBy("ItemRunID").pivot("index").agg({"EventDateTime": "first"})

display(pivot_event_date_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pivot_event_date_df = pivot_event_date_df.withColumnRenamed("1", "ItemRunStartDate").withColumnRenamed("2", "ItemRunEndDate")
display(pivot_event_date_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pivot_alerts_teams_group_df = status_row_num_df.select(
    "AlertToTeamsGroupID",
    "ItemRunID",
    "index"
    ).groupBy("ItemRunID").pivot("index").agg({"AlertToTeamsGroupID": "first"})

display(pivot_alerts_teams_group_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pivot_alerts_teams_group_df = pivot_alerts_teams_group_df.withColumnRenamed("2", "AlertToTeamsGroupID")
display(pivot_alerts_teams_group_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pivot_alert_teams_channel_df = status_row_num_df.select(
    "ItemRunID",
    "AlertToTeamsChannelID",
    "index"
    ).groupBy("ItemRunID").pivot("index").agg({"AlertToTeamsChannelID": "first"})

display(pivot_alert_teams_channel_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pivot_alert_teams_channel_df = pivot_alert_teams_channel_df.withColumnRenamed("2", "AlertToTeamsChannelID")
display(pivot_alert_teams_channel_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pivot_alert_email_df = status_row_num_df.select(
    "ItemRunID",
    "AlertToEmail",
    "index"
    ).groupBy("ItemRunID").pivot("index").agg({"AlertToEmail": "first"})

display(pivot_alert_email_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pivot_alert_email_df = pivot_alert_email_df.withColumnRenamed("2", "AlertToEmail")
display(pivot_alert_email_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

distinct_raw_df = item_execution_raw_df.select(
    "ItemWorkspaceID",
    "ItemWorkspaceName",
    "ItemID",
    "ItemName",
    "ItemType",
    "ItemGroupID",
    "ItemRunID",
    "ItemFolderPath",
    "ParentItemID",
    "ParentItemName",
    "ParentItemRunID",
    "TriggerID",
    "TriggerName",
    "TriggerType",
    "TriggerDateTime",
    "TriggerEventSource",
    "TriggerEventSubject",
    "TriggerFileName",
    "TriggerFolderPath",
    "CreatedBy",
    "CreatedDateTime",
).distinct().orderBy("ItemRunID")

display(distinct_raw_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Join each dataframe that we performed pivots on, based on **_ItemRunID_** ##

# CELL ********************

join_status_date_pivoted_df = pivot_status_df.join(pivot_event_date_df, on="ItemRunID")
display(join_status_date_pivoted_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_join_df = distinct_raw_df.join(join_status_date_pivoted_df, on="ItemRunID")\
                               .join(pivot_alerts_teams_group_df, on="ItemRunID")\
                               .join(pivot_alert_teams_channel_df, on="ItemRunID")\
                               .join(pivot_alert_email_df, on="ItemRunID").orderBy("ItemRunID")
display(final_join_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Add _**ItemExecutionKey**_ based on _**ItemRunID**_ now that we are done with our pivots and joins

# CELL ********************

window_spec = Window.orderBy("ItemRunID")

df_with_key = final_join_df.withColumn(
    "ItemExecutionKey",row_number().over(window_spec))
display(df_with_key)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import concat, lit

df_with_monitor_url = df_with_key.withColumn("Monitoring_Url", concat(
    lit("https://app.fabric.microsoft.com/workloads/data-pipeline/monitoring/workspaces/"),
    df_with_key["ItemWorkspaceID"],
    lit("/pipelines/"),
    df_with_key["ItemID"],
    lit("/"),
    df_with_key["ItemRunID"]
))

display(df_with_monitor_url)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# monitoring_url = f"https://app.fabric.microsoft.com/workloads/data-pipeline/monitoring/workspaces/{WorkspaceID}/pipelines/{PipelineID}/{PipelineRunID}"

# CELL ********************

from pyspark.sql.functions import date_format

df_date_key = df_with_monitor_url.withColumn("DateKey", date_format("ItemRunStartDate", "yyyyMMdd").cast("int"))

display(df_date_key)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Select and _**alias/cast**_ where necessary ##

# CELL ********************

from pyspark.sql.types import IntegerType, TimestampType

refined_df = df_date_key.select(
    "ItemExecutionKey",
    "DateKey",
    "ItemWorkspaceID",
    "ItemWorkspaceName",
    "ItemID",
    "ItemName",
    "ItemType",
    "ItemGroupID",
    "ItemRunID",
    col("End_Status").alias("ItemRunStatus"),
    "ItemFolderPath",
    "ParentItemID",
    "ParentItemName",
    "ParentItemRunID",
    col("ItemRunStartDate").alias("ItemRunStartDateTime").cast(TimestampType()),
    col("ItemRunEndDate").alias("ItemRunEndDateTime").cast(TimestampType()),
    "TriggerID",
    "TriggerName",
    "TriggerType",
    col("TriggerDateTime").cast(TimestampType()),
    "TriggerEventSource",
    "TriggerEventSubject",
    "TriggerFileName",
    "TriggerFolderPath",
    "AlertToTeamsGroupID",
    "AlertToTeamsChannelID",
    "AlertToEmail",
    "Monitoring_Url",
    "CreatedBy",
    col("CreatedDateTime").cast(TimestampType()),
)

display(refined_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import unix_timestamp, expr

duration_df = refined_df.withColumn("Start", unix_timestamp("ItemRunStartDateTime", "HH:mm:ss")) \
                        .withColumn("End", unix_timestamp("ItemRunEndDateTime", "HH:mm:ss")) \
                        .withColumn("DurationSec", col("End") - col("Start"))


formated_duration_df = duration_df.withColumn("ItemDuration", expr("""
    concat(
       lpad(floor(DurationSec / 3600), 2, '0'), ':',
       lpad(floor((DurationSec % 3600) / 60), 2, '0'), ':',
       lpad(DurationSec % 60, 2, '0')
    )
"""))

display(formated_duration_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df = formated_duration_df.select(
    "ItemExecutionKey",
    "DateKey",
    "ItemWorkspaceID",
    "ItemWorkspaceName",
    "ItemID",
    "ItemName",
    "ItemType",
    "ItemGroupID",
    "ItemRunID",
    "ItemRunStatus",
    "ItemFolderPath",
    "ParentItemID",
    "ParentItemName",
    "ParentItemRunID",
    "ItemRunStartDateTime",
    "ItemRunEndDateTime",
    "ItemDuration",
    "TriggerID",
    "TriggerName",
    "TriggerType",
    "TriggerDateTime",
    "TriggerEventSource",
    "TriggerEventSubject",
    "TriggerFileName",
    "TriggerFolderPath",
    "AlertToTeamsGroupID",
    "AlertToTeamsChannelID",
    "AlertToEmail",
    "Monitoring_Url",
    "CreatedBy",
    "CreatedDateTime"
)

display(final_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Write you refined _**dataframe**_ to a delta table in the refined layer

# CELL ********************

final_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"Refined.ItemExecution")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
