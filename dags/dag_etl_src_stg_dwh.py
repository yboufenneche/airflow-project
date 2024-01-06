from datetime import datetime, timedelta
from airflow.decorators import dag
from task_src_stg_distance import src_to_stg_distance
from task_src_stg_direction import src_to_stg_direction
from task_src_stg_offre import src_to_stg_offre
from task_src_stg_client import src_to_stg_client
from task_src_stg_produit import merge_produit
from task_src_stg_appel import src_to_stg_appel

# The dag
@dag(
    schedule_interval="0 0 * * *",  # Trigger each day at midnight
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        'owner': 'boufenny',
        "retries": 0,  # If a task fails, there is no retry.
    },
    tags=["src", "stg", "dwh", "etl"],
)
def dag_etl_src_stg_dwh():
    src_to_stg_offre() >> src_to_stg_distance() >> src_to_stg_direction() >> src_to_stg_client() >> merge_produit >> src_to_stg_appel()

my_dag = dag_etl_src_stg_dwh()
