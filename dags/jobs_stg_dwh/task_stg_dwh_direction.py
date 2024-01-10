from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

DB_CONNECTION = 'snowflake_stg'  # Connection to the DB
#DWH_DB_CONNECTION = 'snowflake_dwh'  # Connection to the DB
SOURCE_TABLE = 'STG.staging.stg_direction'  # Source table
TARGET_TABLE = 'DWH.normalized.dwh_direction' # Target table to store data trasformed

# MERGE
copy_query = f"""
CREATE OR REPLACE TABLE {TARGET_TABLE} CLONE {SOURCE_TABLE};  
"""

# Merge task: copy table stg_direction into table dwh_direction
copy_dwh_direction = SnowflakeOperator(
    task_id='stg_to_dwh_direction',
    sql=copy_query,
    snowflake_conn_id=DB_CONNECTION,
    autocommit=True
)
