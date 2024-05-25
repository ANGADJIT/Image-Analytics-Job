from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import task
from plugins.operators.api_modeling_operator import APIModelingOperator
from plugins.models.user_model import Users

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'USER_ENTRY_DAG',
    default_args=default_args,
    description='A simple HTTP operator example',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    max_active_runs=1,
    catchup=False
) as dag:
    
    pass
