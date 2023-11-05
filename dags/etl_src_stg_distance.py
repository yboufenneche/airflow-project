

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
from io import StringIO
import os

# Source csv filename
INPUT_FILE_NAME = "/data/src_data/src_direction.csv"

target_table_name = 'stg_distance'

# PostgreSQL connection parameters
postgres_conn_id = "postgres_connection"
table_name = "stg_direction"

# Airflow DAG
dag = DAG('csv_to_postgres', start_date=datetime(
    2023, 11, 4), schedule_interval=None)

# ETL function


def etl_csv_to_postgres():
    # Extract
    # Full file name (path and filename)
    csv_file_path = os.path.dirname(__file__) + INPUT_FILE_NAME
    df = pd.read_csv(csv_file_path)

    # Transform
    # You can perform data transformations here using Pandas

    # Load
    # Set up a connection to your PostgreSQL database
    conn = psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password='postgres',
        host='172.20.0.2', # docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' CONTAINER_ID
        port=5432
    )

    # Infer the table schema from the CSV
    schema = ", ".join(f"{column} VARCHAR" for column in df.columns)

    # Create the PostgreSQL table
    create_table_query = f"CREATE TABLE IF NOT EXISTS {target_table_name} ({schema})"
    cur.execute(create_table_query)

    # Insert data into the table
    for row in df.itertuples(index=False):
        insert_query = f"INSERT INTO {target_table_name} VALUES {tuple(row)}"
        cur.execute(insert_query)

    conn.commit()
    conn.close()

# Define Airflow tasks
extract_and_transform = PythonOperator(
    task_id='extract_and_transform',
    python_callable=process_csv,
    dag=dag,
)

load_to_postgres = PostgresOperator(
    task_id='load_to_postgres',
    postgres_conn_id=postgres_conn_id,
    sql=f"COPY {target_table_name} FROM '{csv_file_path}' CSV HEADER;",
    dag=dag,
)

# Set task dependencies
extract_and_transform >> load_to_postgres

