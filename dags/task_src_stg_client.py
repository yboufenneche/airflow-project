from airflow.decorators import task

from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from sqlalchemy import create_engine

import pandas as pd
import os

DB_CONNECTION = 'snowflake_stg'  # Connection to the DB
SCHEMA = 'staging'
TARGET_TABLE = 'stg_client'  # Target table to store data trasformed
JOIN_TABLE = 'stg_offre'
DATABASE = 'STG'

INPUT_FILE = "/data/src_data/src_client.csv"  # Source CSV file name
REJECT_FILE = "/data/rejected/reject_stg_client.csv"
CSV_SEPARATOR = ";"  # Separator in the CSV files

hook = SnowflakeHook(snowflake_conn_id=DB_CONNECTION)
connection_uri = hook.get_uri()
engine = create_engine(connection_uri)


@task
def src_to_stg_client():

    file_path = os.path.dirname(__file__) + INPUT_FILE

    # Load CSV data to a DataFrame
    df = pd.read_csv(file_path, sep=CSV_SEPARATOR)

    # Exclude rows with id < 0 or num is null
    df = df[(df['Id_Client'] >= 0) & (df['Numero'].notna())]

    # Capitalize nom
    df['Nom_Client'] = df['Nom_Client'].str.upper()

    # Capitalize the first letter of prenom
    df['Prenom_Client'] = df['Prenom_Client'].str.capitalize()

    # Replace missing values in 'Date_abonnement' with "01/01/2002"
    df['Date_abonnement'].fillna("01/01/2002", inplace=True)
    df['Date_abonnement'] = pd.to_datetime(
        df['Date_abonnement'], format="%d/%m/%Y").dt.strftime('%Y-%m-%d')

    # Load data from SQL table into a DataFrame
    hook.get_conn().cursor().execute(f'USE DATABASE {DATABASE}')
    sql_df = pd.read_sql_table(JOIN_TABLE, engine)

    # Filter rows where offer column value is not present in SQL table
    df = df[df['Id_Offre'].isin(sql_df['Id_Offre'])]

    # Checking the format of the Numero column
    for index, row in df.iterrows():
        if not pd.Series([row['Numero']]).str.match(r'^06\d{8}$').any():
            print(
                f"Alerte : Le numéro {row['Numero']} n'est pas au format attendu (06xxxxxxxx).")

    # Copy df to a Snowflake table
    df.to_sql(TARGET_TABLE, engine, schema=SCHEMA,
              if_exists='append', index=False)
