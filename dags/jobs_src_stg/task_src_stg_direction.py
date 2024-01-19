from airflow.decorators import task

from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.hooks.postgres_hook import PostgresHook
from sqlalchemy import create_engine

import pandas as pd
import os

SOURCE_DB_CONNECTION = 'postgres_src'  # Connection to the source DB (PostgreSQL)
TARGET_DB_CONNECTION = 'snowflake_stg'  # Connection to the target DB (Snowflake)
SCHEMA = 'staging' # Schema where the target table is located
SOURCE_TABLE = 'src_direction'
TARGET_TABLE = 'stg_direction'  # Target table to store data trasformed

CSV_SEPARATOR = ';'

DF_CHUNK_SIZE = 20000 # Chunksize value for the Pandas to_sql() method

REJECT_FILE = "/data/rejected/reject_stg_direction.csv"


@task
def src_to_stg_direction():

    hook = PostgresHook(postgres_conn_id=SOURCE_DB_CONNECTION)
    connection = hook.get_conn()
    connection_uri = hook.get_uri()
    engine = create_engine(connection_uri)

    # Create a Pandas dataframe from source table content
    query = f"SELECT * FROM {SOURCE_TABLE}"
    df = pd.read_sql(query, con=connection)
    df = df.rename(columns={'id_direction': 'Id_Direction',
                   'lib_direction': 'Lib_Direction'})

    # Rejected data: rows with Id_Distance < 0 --> put this data in a new dataframe (rejected_df)
    rejected_df = df[df['Id_Direction'] < 0]

    df = df[df['Id_Direction'] >= 0]  # Exclude rows with Id_Direction < 0 from df

    # Save rejected data to a CSV file
    rejected_df.to_csv(os.path.dirname(os.path.dirname(__file__)) +
                       REJECT_FILE, sep=CSV_SEPARATOR, index=False)

    # Copy df to the target a Snowflake table
    hook = SnowflakeHook(snowflake_conn_id=TARGET_DB_CONNECTION)
    connection_uri = hook.get_uri()
    engine = create_engine(connection_uri)
    df.to_sql(TARGET_TABLE, engine, schema=SCHEMA,
              if_exists='replace', index=False, chunksize=DF_CHUNK_SIZE)
