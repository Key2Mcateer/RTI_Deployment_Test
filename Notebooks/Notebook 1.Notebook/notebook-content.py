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

import pandas as pd
import sempy.fabric as fabric

client = fabric.FabricRestClient()
WorkspaceID = "3c1e7d50-a3b2-412e-ab7e-6d4b4cbbfc2c"
url = "https://api.fabric.microsoft.com/v1/workspaces/3c1e7d50-a3b2-412e-ab7e-6d4b4cbbfc2c/dataPipelines"

response = client.get(url)

df_items = pd.json_normalize(response.json()['value'])
print(df_items)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
