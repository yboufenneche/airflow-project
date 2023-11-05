from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd
import os

DB_CONNECTION = 'postgres_dev'
# Source csv filename
INPUT_FILE = "/data/src_data/src_distance.csv"
CSV_SEPARATOR = ";"
TARGET_TABLE = 'stg_distance'


@dag(
    schedule_interval="0 0 * * *",  # Déclencher à minuit chaque jour
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        'owner': 'boufenny',
        "retries": 2,  # If a task fails, it will retry 2 times.
    },
    tags=["example"],
)
def etl_src_stg_distance_dag():
    # Function to read a CSV file
    # Set null values for Desc_distance to "Non renseigné"
    @task
    def read_csv_task(file_path, id, lib, sep=CSV_SEPARATOR):
        df = pd.read_csv(file_path, sep=sep)
        # Exclude rows with Id_Distance < 0
        df = df[df[id] >= 0]
        # Replace missing values (NaN) for Lib_Distance with "Non renseigné"
        df[lib].fillna("Non renseigné", inplace=True)
        return df

    # Read the CSV file using the previous function
    csv_file_path = os.path.dirname(__file__) + INPUT_FILE
    df_task = read_csv_task(csv_file_path, 'Id_Distance', 'Lib_Distance')

    # Show the data read from CSV
    @task
    def print_dataframe(dataframe):
        print(dataframe)

    # Utiliser la tâche pour afficher les données
    print_task = print_dataframe(df_task)

    # Create the SQL table
    create_t_task = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id=DB_CONNECTION,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            Id_Distance INT,
            Lib_Distance VARCHAR(50),
            Desc_Distance VARCHAR(50),
            CONSTRAINT SRC_REFDISTANCE_PK PRIMARY KEY (Id_Distance)
            );
        """
    )

    # Insert data into SQL table
    insert_t_task = PostgresOperator(
        task_id='insert_into_table',
        postgres_conn_id=DB_CONNECTION,
        sql=f"""
            INSERT INTO {TARGET_TABLE} (Id_Distance, Lib_Distance, Desc_Distance)
            VALUES (1, 'GMS Local', 'Appel entre portables sur le réseau Orange');
        """
    )

    # Truncate SQL table
    truncate_t_task = PostgresOperator(
        task_id='delete_data_from_table',
        postgres_conn_id=DB_CONNECTION,
        sql=f"""
            DELETE FROM {TARGET_TABLE};
        """
    )

    # Tasks order
    df_task >> print_task >> create_t_task >> truncate_t_task >> insert_t_task

# Create the DAG object
stg_distance = etl_src_stg_distance_dag()
