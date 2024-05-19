from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import task
from plugins import APIModelingOperator, User

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'example_http_operator',
    default_args=default_args,
    description='A simple HTTP operator example',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    max_active_runs=1,
    catchup=False
) as dag:

    @task
    def print_response(**kwargs):
        print('HELLO RESPONSE..', kwargs['ti'].xcom_pull('demo_task'))

    t1 = APIModelingOperator(
        method='GET',
        url='https://randomuser.me/api/?results=1&nat=IN',
        task_id='demo_task',
        model=User)

    t1 >> print_response()

dag.test()