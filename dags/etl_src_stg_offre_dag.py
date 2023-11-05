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
conn_string = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(conn_string)

# For PostgresOperator
DB_CONNECTION = 'postgres_dev'  # Connextion to the DB
SOURCE_TABLE = 'src_offre'  # Source table to storing input data
TARGET_TABLE = 'stg_offre'  # Target table to store data trasformed

# The dag
@dag(
    schedule_interval="0 0 * * *",  # Déclencher à minuit chaque jour
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        'owner': 'boufenny',
        "retries": 2,  # If a task fails, it will retry 2 times.
    },
    tags=["offre", "src", "stg"],
)
def etl_src_stg_offre_dag():
    # Load a dataframe from SQL table and do filtering and transformations
    # Return a dataframe filtered and transformed
    @task
    def filter_data_from_sql():
        # Load dataframe from source table
        query = f"SELECT * FROM {SOURCE_TABLE}"
        df = pd.read_sql(query, con=engine)

        # Filter and transform dataframe
        df = df[df['id_offre'] >= 0]
        df['lib_offre'] = df['lib_offre'].str.upper()
        df['desc_offre'] = df['desc_offre'].str[:30]
        df['desc_offre'].fillna("Inconnu", inplace=True)

        return df
    # The task: <filter_data_task>
    filter_data_task = filter_data_from_sql()

    # Insert filtered and transformed data into target table
    @task
    def load_filtered_data_to_sql(dataframe):
        dataframe.to_sql(name=TARGET_TABLE, con=engine,
                         if_exists="replace", index=False)

    # The task: <load_filtered_data_task>
    load_filtered_data_task = load_filtered_data_to_sql(filter_data_task)

    # Create source SQL table
    # The task: <create_src_sql_table_task>
    create_src_sql_table_task = PostgresOperator(
        task_id='create_source_postgres_table',
        postgres_conn_id=DB_CONNECTION,
        sql="sql/src_offre_schema.sql"
    )

    # Create target SQL table
    # The task: <create_stg_sql_table_task>
    create_stg_sql_table_task = PostgresOperator(
        task_id='create_target_postgres_table',
        postgres_conn_id=DB_CONNECTION,
        sql="sql/stg_offre_schema.sql"
    )

    # Insert data to source SQL table
    # The task: <fill_src_sql_table_task>
    fill_src_sql_table_task = PostgresOperator(
        task_id='fill_src_postgres_table',
        postgres_conn_id=DB_CONNECTION,
        sql="data/src_data/src_offre_data.sql"
    )

    # Task execution order
    create_src_sql_table_task >> fill_src_sql_table_task >> filter_data_task >> create_stg_sql_table_task >> load_filtered_data_task


stg_offre = etl_src_stg_offre_dag()
