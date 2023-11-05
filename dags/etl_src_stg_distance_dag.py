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
# For PostgresOperator
DB_CONNECTION = 'postgres_dev'  # Connextion to the DB
TARGET_TABLE = 'stg_distance'  # Target table to store data trasformed
# Source CSV file
INPUT_FILE = "/data/src_data/src_distance.csv"  # File name
CSV_SEPARATOR = ";"  # Separator in the CSV file


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
    # Task taht reads a CSV file
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
    csv_to_df_task = read_csv_task(
        csv_file_path, 'Id_Distance', 'Lib_Distance')

    # Copy transformed data to SQL table
    @task
    def copy_df_to_sql(dataframe):
        conn_string = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(conn_string)
        dataframe.to_sql(TARGET_TABLE, engine,
                         if_exists='replace', index=False)

    # Create variable to store the task
    load_stg_distance = copy_df_to_sql(csv_to_df_task)

    # Create the SQL table
    create_sql_table_task = PostgresOperator(
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

    # Truncate SQL table
    truncate_sql_table_task = PostgresOperator(
        task_id='delete_data_from_table',
        postgres_conn_id=DB_CONNECTION,
        sql=f"""
            DELETE FROM {TARGET_TABLE};
        """
    )

    # Tasks order
    csv_to_df_task >> create_sql_table_task >> truncate_sql_table_task >> load_stg_distance


# Create the DAG object
stg_distance = etl_src_stg_distance_dag()
