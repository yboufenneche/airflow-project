from airflow.decorators import task, task_group

from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from sqlalchemy import create_engine

import pandas as pd
import os

DB_CONNECTION = 'snowflake_dev'  # Connection to the DB
SCHEMA = 'staging'
TARGET_TABLE = 'stg_distance'  # Target table to store data trasformed

INPUT_FILE = "/data/src_data/src_distance.csv"  # Source CSV file name
REJECT_FILE = "/data/rejected/reject_stg_distance.csv"
CSV_SEPARATOR = ";"  # Separator in the CSV files

@task
def src_to_stg_distance():
        
    file_path = os.path.dirname(__file__) + INPUT_FILE

    df = pd.read_csv(file_path, sep=CSV_SEPARATOR)
    
    # Rejected data: rows with Id_Distance < 0
    rejected_df = df[df['Id_Distance'] < 0]
    df = df[df['Id_Distance'] >= 0]  # Exclude rows with Id_Distance < 0
    
    # Replace missing Lib_Distance with "Non renseigné"
    df['Lib_Distance'].fillna("Non renseigné", inplace=True)
    
    # Save rejected data to a CSV file
    rejected_df.to_csv(os.path.dirname(__file__) +
                        REJECT_FILE, sep=CSV_SEPARATOR, index=False)

    # Copy df to a Snowflake table
    hook = SnowflakeHook(snowflake_conn_id=DB_CONNECTION)
    connection_uri = hook.get_uri()
    engine = create_engine(connection_uri)
    df.to_sql(TARGET_TABLE, engine, schema=SCHEMA, if_exists='append', index=False)
