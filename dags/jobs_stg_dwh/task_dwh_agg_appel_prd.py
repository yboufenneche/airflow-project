from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

DB_CONNECTION = 'snowflake_stg'  # Connection to the DB
# DWH_DB_CONNECTION = 'snowflake_dwh'  # Connection to the DB
SOURCE_TABLE = 'DWH.NORMALIZED.DWH_APPEL'  # Source table
# Target table to store data trasformed
TARGTE_TABLE = 'DWH.NORMALIZED.DWH_AGG_APPEL_PRD'

# MERGE
merge_query = f"""
    INSERT INTO {TARGTE_TABLE}
    SELECT "Lib_Produit", MONTH("Date_appel") || YEAR("Date_appel") AS "Mois_appel", "Type_Offre", "Id_Direction", "Lib_Distance", "Reseau", AVG ("Duree") AS "Duree", COUNT (*) AS "Nb_appel"
    FROM {SOURCE_TABLE}
    GROUP BY "Lib_Produit","Mois_appel", "Type_Offre", "Id_Direction", "Lib_Distance", "Reseau";
"""

# Merge task: copy table stg_appel into table dwh_appel
merge_dwh_agg_appel_prd = SnowflakeOperator(
    task_id='dwh_agg_appel_prd',
    sql=merge_query,
    snowflake_conn_id=DB_CONNECTION,
    autocommit=True
)
