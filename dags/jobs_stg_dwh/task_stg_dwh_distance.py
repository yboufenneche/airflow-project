from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

DB_CONNECTION = 'snowflake_stg'  # Connection to the DB
#DWH_DB_CONNECTION = 'snowflake_dwh'  # Connection to the DB
SOURCE_TABLE = 'STG.staging.stg_distance'  # Source table
TARGET_TABLE = 'DWH.normalized.dwh_distance' # Target table to store data trasformed

# MERGE
merge_query = f"""
    TRUNCATE TABLE {TARGET_TABLE};
    MERGE INTO {TARGET_TABLE} AS target
    USING {SOURCE_TABLE} AS source
    ON target."Id_Distance" = source."Id_Distance"

    WHEN NOT MATCHED AND CONTAINS(upper(source."Desc_Distance"), 'FIXE') THEN
        INSERT ("Id_Distance", "Lib_Distance", "Desc_Distance", "Reseau")
        VALUES (source."Id_Distance", source."Lib_Distance", source."Desc_Distance", 'FIXE')
    
    WHEN NOT MATCHED AND CONTAINS(upper(source."Desc_Distance"), 'PORTABLE') THEN
        INSERT ("Id_Distance", "Lib_Distance", "Desc_Distance", "Reseau")
        VALUES (source."Id_Distance", source."Lib_Distance", source."Desc_Distance", 'GSM')

    WHEN NOT MATCHED AND NOT CONTAINS(upper(source."Desc_Distance"), 'PORTABLE')
                    AND NOT CONTAINS(upper(source."Desc_Distance"), 'FIXE')THEN
        INSERT ("Id_Distance", "Lib_Distance", "Desc_Distance", "Reseau")
        VALUES (source."Id_Distance", source."Lib_Distance", source."Desc_Distance", 'INCONNU');
"""

# Merge task: copy table stg_distance into table dwh_distance
merge_dwh_distance = SnowflakeOperator(
    task_id='stg_to_dwh_distance',
    sql=merge_query,
    snowflake_conn_id=DB_CONNECTION,
    autocommit=True
)
