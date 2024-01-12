from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

DB_CONNECTION = 'snowflake_stg'  # Connection to the DB
# DWH_DB_CONNECTION = 'snowflake_dwh'  # Connection to the DB
SOURCE_TABLE = 'DWH.NORMALIZED.DWH_APPEL'  # Source table
# Target table to store data trasformed
TARGET_TABLE = 'DWH.NORMALIZED.DWH_AGG_APPEL_DISTANCE'

# MERGE
merge_query = f"""
    TRUNCATE TABLE {TARGET_TABLE};
    
    INSERT INTO {TARGET_TABLE}
    SELECT "Lib_Distance", "Pays", MONTH("Date_appel") || YEAR("Date_appel") AS "Mois_appel", "Type_Offre", "Id_Direction", "Lib_Produit", AVG ("Duree") AS "Duree", COUNT (*) AS "Nb_appel"
    FROM {SOURCE_TABLE}
    GROUP BY "Lib_Distance", "Pays", "Mois_appel", "Type_Offre", "Id_Direction", "Lib_Produit", "Reseau";
"""

# Merge task: copy table stg_appel into table dwh_appel
merge_dwh_agg_appel_distance = SnowflakeOperator(
    task_id='dwh_agg_appel_distance',
    sql=merge_query,
    snowflake_conn_id=DB_CONNECTION,
    autocommit=True
)
