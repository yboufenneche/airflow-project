from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

DB_CONNECTION = 'snowflake_stg'  # Connection to the DB
#DWH_DB_CONNECTION = 'snowflake_dwh'  # Connection to the DB
SOURCE_TABLE = 'STG.staging.stg_offre'  # Source table
TARGET_TABLE = 'DWH.normalized.dwh_offre' # Target table to store data trasformed

# MERGE
merge_query = f"""
    TRUNCATE TABLE {TARGET_TABLE};
    
    MERGE INTO {TARGET_TABLE} AS target
    USING {SOURCE_TABLE} AS source
    ON target."Id_Offre" = source."Id_Offre"

    WHEN NOT MATCHED AND STARTSWITH(source."Id_Offre", '2') THEN
        INSERT ("Id_Offre", "Lib_Offre", "Desc_Offre", "Type_Offre")
        VALUES (source."Id_Offre", source."Lib_Offre", source."Desc_Offre", 'PREPAID')
    
    WHEN NOT MATCHED AND STARTSWITH(source."Id_Offre", '5') THEN
        INSERT ("Id_Offre", "Lib_Offre", "Desc_Offre", "Type_Offre")
        VALUES (source."Id_Offre", source."Lib_Offre", source."Desc_Offre", 'POSTPAID')

    WHEN NOT MATCHED AND NOT STARTSWITH(source."Id_Offre", '2')
                     AND NOT STARTSWITH(source."Id_Offre", '5') THEN
        INSERT ("Id_Offre", "Lib_Offre", "Desc_Offre", "Type_Offre")
        VALUES (source."Id_Offre", source."Lib_Offre", source."Desc_Offre", 'INCONNU');
"""

# Merge task: copy table stg_offre into table dwh_offre
merge_dwh_offre = SnowflakeOperator(
    task_id='stg_to_dwh_offre',
    sql=merge_query,
    snowflake_conn_id=DB_CONNECTION,
    autocommit=True
)
