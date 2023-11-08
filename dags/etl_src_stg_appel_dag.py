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
JOIN_TABLE_CLIENT = "stg_client"
JOIN_TABLE_DIRECTION = "stg_direction"
JOIN_TABLE_PRODUIT = "stg_produit"
JOIN_TABLE_DISTANCE = "stg_distance"
conn_string = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(conn_string)

# For PostgresOperator
DB_CONNECTION = 'postgres_dev'  # Connextion to the DB
TARGET_TABLE = 'stg_appel'  # Target table to store data trasformed
TABLE_SCHEMA = "sql/stg_appel_schema.sql"

# Source CSV file
INPUT_FILE = "/data/src_data/src_appel.csv"  # File name
CSV_SEPARATOR = ";"  # Separator in the CSV file

# The dag


@dag(
    schedule_interval="0 0 * * *",  # Déclencher à minuit chaque jour
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        'owner': 'boufenny',
        "retries": 0,  # If a task fails, it will retry 2 times.
    },
    tags=["src", "stg", "csv", "sql", "client",
          "produit", "direction", "distance"]
)
def etl_src_stg_appel_dag():
    # Task taht reads a CSV file
    # Set null values for Desc_distance to "Non renseigné"
    @task
    def filter_data_from_csv(file_path, cli, date, heure, num_aplnt, num_aple, dir, prod, dis, dur, sep=CSV_SEPARATOR):
        # Load CSV data to a DataFrame
        df = pd.read_csv(file_path, sep=sep)

        # Exclude rows with missing client id, call date, direction id, or distance id
        df = df[df[cli].notna() & (df[date].notna()) & (
            df[dir].notna()) & (df[dis].notna())]

        # Load data from SQL table client into a DataFrame
        client_df = pd.read_sql_table(JOIN_TABLE_CLIENT, engine)

        # Load data from SQL table direction into a DataFrame
        dir_df = pd.read_sql_table(JOIN_TABLE_DIRECTION, engine)

        # Load data from SQL table produit into a DataFrame
        prod_df = pd.read_sql_table(JOIN_TABLE_PRODUIT, engine)

        # Load data from SQL table distance into a DataFrame
        dis_df = pd.read_sql_table(JOIN_TABLE_DISTANCE, engine)

        # Filter rows where client id is not present in SQL table client
        df = df[df[cli].isin(client_df[cli])]

        # Filter rows where direction id is not present in SQL table direction
        df = df[df[dir].isin(dir_df[dir])]

        # Filter rows where product id is set but not present in SQL table produit
        df = df[(df[prod].isna()) | df[prod].isin(prod_df[prod.lower()])]

        # Filter rows where distance id is not present in SQL table distance
        df = df[df[dis].isin(dis_df[dis])]

        # Replace missing call time with '12:00'
        df[heure].fillna("12/00", inplace=True)

        # Replace negative call durations with 0
        df[dur] = df[dur].apply(lambda x: max(0, x))

        # Checking the format of the num column
        for index, row in df.iterrows():
            if not pd.Series([row[num_aplnt]]).str.match(r'^(\d{10}|\+\d{1,19})$').any():
                print(
                    f"Alerte : Le numéro {row[num_aplnt]} n'est pas au format attendu.")
            if not pd.Series([row[num_aple]]).str.match(r'^(\d{10}|\+\d{1,19})$').any():
                print(
                    f"Alerte : Le numéro {row[num_aple]} n'est pas au format attendu.")

        return df

    # Read the CSV file using the previous function
    csv_file_path = os.path.dirname(__file__) + INPUT_FILE
    # Task: <csv_to_df_task>
    csv_to_df_task = filter_data_from_csv(csv_file_path, 'Id_Client', 'Date_appel', 'Heure_appel',
                                          'No_appelant', 'No_appele', 'Id_Direction', 'Id_Produit', 'Id_Distance', 'Duree')

    # Copy transformed dataframe to SQL table
    # Task: <copy_df_to_sql>
    @task
    def copy_df_to_sql(dataframe):
        conn_string = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(conn_string)
        dataframe.to_sql(TARGET_TABLE, engine,
                         if_exists='replace', index=False)

    # Create variable to store the task
    load_stg_appel = copy_df_to_sql(csv_to_df_task)

    # Create the SQL table
    # Task: <create_sql_table_task>
    create_sql_table_task = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id=DB_CONNECTION,
        sql=TABLE_SCHEMA
    )

    # Tasks order
    csv_to_df_task >> create_sql_table_task >> load_stg_appel


# Create the DAG object
stg_client = etl_src_stg_appel_dag()
