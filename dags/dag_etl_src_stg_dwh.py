from datetime import datetime, timedelta
from airflow.decorators import dag
from jobs_src_stg.task_src_stg_distance import src_to_stg_distance
from jobs_src_stg.task_src_stg_direction import src_to_stg_direction
from jobs_src_stg.task_src_stg_offre import src_to_stg_offre
from jobs_src_stg.task_src_stg_client import src_to_stg_client
from jobs_src_stg.task_src_stg_produit import merge_stg_produit
from jobs_src_stg.task_src_stg_appel import src_to_stg_appel
from jobs_stg_dwh.task_stg_dwh_direction import copy_dwh_direction
from jobs_stg_dwh.task_stg_dwh_produit import copy_dwh_produit
from jobs_stg_dwh.task_stg_dwh_distance import merge_dwh_distance
from jobs_stg_dwh.task_stg_dwh_offre import merge_dwh_offre

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
    src_to_stg_offre() >> src_to_stg_distance() >> src_to_stg_direction() >> src_to_stg_client(
    ) >> merge_stg_produit >> src_to_stg_appel() >> copy_dwh_direction >> copy_dwh_produit >> merge_dwh_distance >> merge_dwh_offre


my_dag = dag_etl_src_stg_dwh()
