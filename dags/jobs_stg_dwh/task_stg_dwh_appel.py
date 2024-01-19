from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

DB_CONNECTION = 'snowflake_stg'  # Connection to the DB
# DWH_DB_CONNECTION = 'snowflake_dwh'  # Connection to the DB
SOURCE_TABLE = 'STG.staging.stg_appel'  # Source table
# Target table to store data trasformed
TARGET_TABLE = 'DWH.normalized.dwh_appel'
JOIN_CLI = 'DWH.normalized.dwh_client'
JOIN_PRD = 'DWH.normalized.dwh_produit'
JOIN_DIS = 'DWH.normalized.dwh_distance'

# MERGE
# Pour une melleiure expérience on peut utiliser une table de transcodage
merge_query = f"""
    CREATE OR REPLACE FUNCTION pays(id_disantce int, no_appelant string, no_appele string)
    returns string
    language python
    runtime_version = '3.10'
    handler = 'pays_py'
    AS
    $$
def pays_py(id_distance, no_appelant, no_appele):
    if (id_distance != 5):
        return 'France'
    if (no_appelant.startswith("30") or no_appele.startswith("30")):
        return "Grèce"
    if (no_appelant.startswith("33") or no_appele.startswith("33")):
        return "Fracne"
    if (no_appelant.startswith("352") or no_appele.startswith("352")):
        return "Luxembourg"
    if (no_appelant.startswith("377") or no_appele.startswith("377")):
        return "Monaco"
    if (no_appelant.startswith("41") or no_appele.startswith("41")):
        return "Suisse"
    if (no_appelant.startswith("44") or no_appele.startswith("44")):
        return "Royaume Uni"
    if (no_appelant.startswith("45") or no_appele.startswith("45")):
        return "Danemark"
    if (no_appelant.startswith("49") or no_appele.startswith("49")):
        return "Allemagne"

    return "Autre"
$$;  

    TRUNCATE TABLE {TARGET_TABLE};
    
    MERGE INTO {TARGET_TABLE} AS target
    USING (SELECT "Id_Client", "Date_appel", "Heure_appel", "Lib_Offre", "Type_Offre", "No_appelant", "No_appele", "Id_Direction", "Id_Produit", "Lib_Produit", "Id_Distance", "Lib_Distance", "Reseau", "Duree"
    FROM {SOURCE_TABLE} NATURAL JOIN {JOIN_CLI} NATURAL JOIN {JOIN_PRD} NATURAL JOIN {JOIN_DIS}) AS source
    ON target."Id_Client" = source."Id_Client" AND  target."Date_appel" = source."Date_appel" AND target."Heure_appel" = source."Heure_appel"

    WHEN NOT MATCHED THEN
        INSERT ("Id_Client", "Date_appel", "Heure_appel", "Lib_Offre", "Type_Offre", "No_appelant", "No_appele", "Id_Direction", "Id_Produit", "Lib_Produit", "Id_Distance", "Lib_Distance", "Reseau", "Duree", "Pays")
        VALUES (source."Id_Client", Source."Date_appel", source."Heure_appel", source."Lib_Offre", source."Type_Offre", source."No_appelant", source."No_appele", source."Id_Direction", source."Id_Produit", source."Lib_Produit", source."Id_Distance", source."Lib_Distance", source."Reseau", source."Duree", PAYS(source."Id_Distance", source."No_appelant", source."No_appele"));
"""

# Merge task: copy table stg_appel into table dwh_appel
merge_dwh_appel = SnowflakeOperator(
    task_id='stg_to_dwh_appel',
    sql=merge_query,
    snowflake_conn_id=DB_CONNECTION,
    autocommit=True
)
