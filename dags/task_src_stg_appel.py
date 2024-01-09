from airflow.decorators import task

from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from sqlalchemy import create_engine

import pandas as pd
import os

DB_CONNECTION = 'snowflake_stg'  # Connection to the DB
SCHEMA = 'staging'
TARGET_TABLE = 'stg_appel'  # Target table to store data trasformed
JOIN_TABLE_CLIENT = "stg_client"
JOIN_TABLE_DIRECTION = "stg_direction"
JOIN_TABLE_PRODUIT = "stg_produit"
JOIN_TABLE_DISTANCE = "stg_distance"

INPUT_FILE = "/data/src_data/src_appel.csv"  # Source CSV file name
REJECT_FILE = "/data/rejected/reject_stg_appel.csv"
CSV_SEPARATOR = ";"  # Separator in the CSV files

hook = SnowflakeHook(snowflake_conn_id=DB_CONNECTION)
connection_uri = hook.get_uri()
engine = create_engine(connection_uri)


@task
def src_to_stg_appel():
    
    file_path = os.path.dirname(__file__) + INPUT_FILE

    # Load CSV data to a DataFrame
    df = pd.read_csv(file_path, sep=CSV_SEPARATOR)

    # Load data from SQL table client into a DataFrame
    client_df = pd.read_sql_table(JOIN_TABLE_CLIENT, engine)

    # Load data from SQL table direction into a DataFrame
    dir_df = pd.read_sql_table(JOIN_TABLE_DIRECTION, engine)

    # Load data from SQL table produit into a DataFrame
    prod_df = pd.read_sql_table(JOIN_TABLE_PRODUIT, engine)

    # Load data from SQL table distance into a DataFrame
    dis_df = pd.read_sql_table(JOIN_TABLE_DISTANCE, engine)
    
    # Reject data
    df_reject_null = df[df['Id_Client'].isna() | (df['Date_appel'].isna()) | (
        df['Id_Direction'].isna()) | (df['Id_Distance'].isna())]
    df_reject_cli = df[~df['Id_Client'].isin(client_df['Id_Client'])]
    df_reject_dir = df[~df['Id_Direction'].isin(dir_df['Id_Direction'])]
    df_reject_prod = df[(df['Id_Produit'].notna()) & ~df['Id_Produit'].isin(prod_df['Id_Produit'])]
    df_reject_dis = df[~df['Id_Distance'].isin(dis_df['Id_Distance'])]
    
    df['Date_appel'] = pd.to_datetime(df['Date_appel'], format="%d/%m/%Y").dt.strftime('%Y-%m-%d')
    
    # Conctenate all rejected dataframes
    reject_frames = [df_reject_null, df_reject_cli, df_reject_dir, df_reject_prod, df_reject_dis]
    rejected_df = pd.concat(reject_frames).drop_duplicates()

    # Filter rows with missing client id, call date, direction id, or distance id
    df = df[df['Id_Client'].notna() & (df['Date_appel'].notna()) & (
        df['Id_Direction'].notna()) & (df['Id_Distance'].notna())]
    
    # Filter rows where client id is not present in SQL table client
    df = df[df['Id_Client'].isin(client_df['Id_Client'])]

    # Filter rows where direction id is not present in SQL table direction
    df = df[df['Id_Direction'].isin(dir_df['Id_Direction'])]

    # Filter rows where product id is set but not present in SQL table produit
    df = df[(df['Id_Produit'].isna()) | df['Id_Produit'].isin(prod_df['Id_Produit'])]

    # Filter rows where distance id is not present in SQL table distance
    df = df[df['Id_Distance'].isin(dis_df['Id_Distance'])]

    # Replace missing call time with '12:00'
    df['Heure_appel'].fillna("12:00", inplace=True)

    # Replace negative call durations with 0
    df['Duree'] = df['Duree'].apply(lambda x: max(0, x))

    # Checking the format of the num column
    for index, row in df.iterrows():
        if not pd.Series([row['No_appelant']]).str.match(r'^(\d{10}|\+\d{1,19})$').any():
            print(
                f"Alerte : Le numéro {row['No_appelant']} n'est pas au format attendu.")
        if not pd.Series([row['No_appele']]).str.match(r'^(\d{10}|\+\d{1,19})$').any():
            print(
                f"Alerte : Le numéro {row['No_appele']} n'est pas au format attendu.")

    # Save rejected data to a CSV file
    rejected_df.to_csv(os.path.dirname(__file__) +
                        REJECT_FILE, sep=CSV_SEPARATOR, index=False)

    # Copy df to a Snowflake table
    df.to_sql(TARGET_TABLE, engine, schema=SCHEMA, if_exists='append', index=False)
