from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import task
from plugins.operators.api_modeling_operator import APIModelingOperator
from plugins.models.user_model import Users
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable

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

    api = APIModelingOperator(
        method='GET',
        url='https://randomuser.me/api/?results=10&nat=IN',
        task_id='demo_task',
        model=Users)

    @task
    def make_insert_query(users: dict):
        columns: list[str] = None

        for user in users:
            if columns is None:
                columns = list(user.keys())

            values: str = ''.join([f'{t},' for t in list(user.values())])

            return f"""INSER INTO {Variable.get('USER_TABLE_NAME')} {columns} VALUES({values})"""

    api = APIModelingOperator(
        task_id='fetch_users',
        method='GET',
        model=Users,
        url='https://randomuser.me/api/?results=10&nat=IN'
    )

    sql = make_insert_query()

    user_entry = PostgresOperator(postgres_conn_id=Variable.get('POSTGRES_DB'),
                                  task_id='db_entry', sql='')

    user_entry
