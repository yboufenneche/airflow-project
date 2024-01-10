from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

DB_CONNECTION = 'snowflake_stg'  # Connection to the DB
#DWH_DB_CONNECTION = 'snowflake_dwh'  # Connection to the DB
SOURCE_TABLE = 'STG.staging.stg_produit'  # Source table
TARGET_TABLE = 'DWH.normalized.dwh_produit' # Target table to store data trasformed

# MERGE
copy_query = f"""
CREATE OR REPLACE TABLE {TARGET_TABLE} CLONE {SOURCE_TABLE};  
"""

# Merge task: copy table stg_produit into table dwh_produit
copy_dwh_produit = SnowflakeOperator(
    task_id='stg_to_dwh_produit',
    sql=copy_query,
    snowflake_conn_id=DB_CONNECTION,
    autocommit=True
)
