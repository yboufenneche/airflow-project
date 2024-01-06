from airflow.decorators import task, task_group

from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from sqlalchemy import create_engine

import pandas as pd
import os

DB_CONNECTION = 'snowflake_dev'  # Connection to the DB
SOURCE_TABLE = 'SRC.source.src_produit'  # Source table
TARGET_TABLE = 'STG.staging.stg_produit'  # Target table to store data trasformed


# MERGE
merge_query = f"""
    MERGE INTO STG.staging.stg_produit AS target
    USING SRC.source.src_produit AS source
    ON target."Id_Produit" = source."Id_Produit"
    WHEN NOT MATCHED AND source."Id_Produit" > 0 AND source."Lib_Produit" IS NOT NULL THEN
        INSERT ("Id_Produit", "Lib_Produit", "Desc_Produit")
        VALUES (source."Id_Produit", source."Lib_Produit", source."Desc_Produit")
    WHEN NOT MATCHED AND source."Id_Produit" > 0 AND source."Lib_Produit" IS NULL THEN
        INSERT ("Id_Produit", "Lib_Produit", "Desc_Produit")
        VALUES (source."Id_Produit", 'Inconnu', source."Desc_Produit");
    
    UPDATE STG.staging.stg_produit
    SET "Lib_Produit" = UPPER("Lib_Produit");   
"""

# Merge task: exclude products with Id_Product <= 0 and replace null values by 'Inconnu'
merge_produit = SnowflakeOperator(
    task_id='elt_merge_product',
    sql=merge_query,
    snowflake_conn_id=DB_CONNECTION,
    autocommit=True
)

