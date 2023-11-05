from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import os
from sqlalchemy import create_engine

# For the function: copy_df_to_sql(dataframe)
DB_HOST = "host.docker.internal"
DB_NAME = "stg"
DB_PORT = "5435"
DB_USER = "postgres"
DB_PASS = "postgres"
JOIN_TABLE = "stg_offre"
conn_string = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(conn_string)

# For PostgresOperator
DB_CONNECTION = 'postgres_dev'  # Connextion to the DB
TARGET_TABLE = 'stg_client'  # Target table to store data trasformed
TABLE_SCHEMA = "sql/stg_client_schema.sql"

# Source CSV file
INPUT_FILE = "/data/src_data/src_client.csv"  # File name
CSV_SEPARATOR = ";"  # Separator in the CSV file

# The dag


@dag(
    schedule_interval="0 0 * * *",  # Déclencher à minuit chaque jour
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        'owner': 'boufenny',
        "retries": 2,  # If a task fails, it will retry 2 times.
    },
    tags=["src", "stg", "csv", "sql", "client", "offre"],
)
def etl_src_stg_client_dag():
    # Task taht reads a CSV file
    # Set null values for Desc_distance to "Non renseigné"
    @task
    def filter_data_from_csv(file_path, id, nom, prenom, num, offre, date_abo, sep=CSV_SEPARATOR):
        # Load CSV data to a DataFrame
        df = pd.read_csv(file_path, sep=sep)

        # Exclude rows with id < 0 or num is null
        df = df[(df[id] >= 0) & (df[num].notna())]

        # Capitalize nom
        df[nom] = df[nom].str.upper()

        # Capitalize the first letter of prenom
        df[prenom] = df[prenom].str.capitalize()

        # Replace missing values in date_abo with "01/01/2002"
        df[date_abo].fillna("01/01/2002", inplace=True)

        # Load data from SQL table into a DataFrame
        sql_df = pd.read_sql_table(JOIN_TABLE, engine)

        # Filter rows where offer column value is not present in SQL table
        df = df[df[offre].isin(sql_df[offre.lower()])]

        # Checking the format of the num column
        for index, row in df.iterrows():
            if not pd.Series([row[num]]).str.match(r'^06\d{8}$').any():
                print(
                    f"Alerte : Le numéro {row[num]} n'est pas au format attendu (06xxxxxxxx).")

        return df

    # Read the CSV file using the previous function
    csv_file_path = os.path.dirname(__file__) + INPUT_FILE
    # Task: <csv_to_df_task>
    csv_to_df_task = filter_data_from_csv(csv_file_path, 'Id_Client', 'Nom_Client',
                                          'Prenom_Client', 'Numero', 'Id_Offre', 'Date_abonnement')

    # Copy transformed dataframe to SQL table
    # Task: <copy_df_to_sql>
    @task
    def copy_df_to_sql(dataframe):
        conn_string = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(conn_string)
        dataframe.to_sql(TARGET_TABLE, engine,
                         if_exists='replace', index=False)

    # Create variable to store the task
    load_stg_distance = copy_df_to_sql(csv_to_df_task)

    # Create the SQL table
    # Task: <create_sql_table_task>
    create_sql_table_task = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id=DB_CONNECTION,
        sql=TABLE_SCHEMA
    )

    # Tasks order
    csv_to_df_task >> create_sql_table_task >> load_stg_distance


# Create the DAG object
stg_client = etl_src_stg_client_dag()
