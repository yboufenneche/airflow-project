from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

DB_CONNECTION = 'snowflake_stg'  # Connection to the DB
#DWH_DB_CONNECTION = 'snowflake_dwh'  # Connection to the DB
SOURCE_TABLE = 'STG.staging.stg_client'  # Source table
TARGET_TABLE = 'DWH.normalized.dwh_client' # Target table to store data trasformed
JOIN_TABLE = 'STG.staging.stg_offre'  # Join table to get 'Lib_Offre'

# MERGE
merge_query = f"""
    TRUNCATE TABLE {TARGET_TABLE};
    
    MERGE INTO {TARGET_TABLE} AS target
    USING ( SELECT "Id_Client", "Nom_Client", "Prenom_Client", "Numero", "Id_Offre", "Lib_Offre", "Date_abonnement" FROM {SOURCE_TABLE} NATURAL JOIN {JOIN_TABLE} ) AS source
    ON target."Id_Client" = source."Id_Client"

    WHEN NOT MATCHED AND STARTSWITH(source."Id_Offre", '2') THEN
        INSERT ("Id_Client", "Nom_Client", "Prenom_Client", "Numero", "Id_Offre", "Lib_Offre", "Type_Offre", "Date_abonnement", "Trim_abonnement" )
        VALUES (source."Id_Client", source."Nom_Client", source."Prenom_Client", source."Numero", source."Id_Offre",  source."Lib_Offre", 'PREPAID', source."Date_abonnement", YEAR(source."Date_abonnement") || '0' || QUARTER(source."Date_abonnement"))
    
    WHEN NOT MATCHED AND STARTSWITH(source."Id_Offre", '5') THEN
        INSERT ("Id_Client", "Nom_Client", "Prenom_Client", "Numero", "Id_Offre", "Lib_Offre", "Type_Offre", "Date_abonnement", "Trim_abonnement" )
        VALUES (source."Id_Client", source."Nom_Client", source."Prenom_Client", source."Numero", source."Id_Offre",  source."Lib_Offre", 'POSTPAID', source."Date_abonnement", YEAR(source."Date_abonnement") || '0' || QUARTER(source."Date_abonnement"))

    WHEN NOT MATCHED AND NOT STARTSWITH(source."Id_Offre", '2')
                     AND NOT STARTSWITH(source."Id_Offre", '5') THEN
        INSERT ("Id_Client", "Nom_Client", "Prenom_Client", "Numero", "Id_Offre", "Lib_Offre", "Type_Offre", "Date_abonnement", "Trim_abonnement" )
        VALUES (source."Id_Client", source."Nom_Client", source."Prenom_Client", source."Numero", source."Id_Offre", source."Lib_Offre", 'INCONNU', source."Date_abonnement", YEAR(source."Date_abonnement") || '0' || QUARTER(source."Date_abonnement"));
"""

# Merge task: copy table stg_client into table dwh_client
merge_dwh_client = SnowflakeOperator(
    task_id='stg_to_dwh_client',
    sql=merge_query,
    snowflake_conn_id=DB_CONNECTION,
    autocommit=True
)
