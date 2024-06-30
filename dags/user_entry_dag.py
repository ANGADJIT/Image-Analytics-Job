from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import task
from plugins.operators.api_modeling_operator import APIModelingOperator
from plugins.models.user_model import Users
from airflow.models import Variable
from plugins.hooks.custom_postgres_hook import CustomPostgresHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'USER_ENTRY_DAG',
    default_args=default_args,
    description='''
        This dag will responsible for inserting random users into our Users Table
    ''',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    max_active_runs=1,
    catchup=False
) as dag:

    def insert_users(**kwargs) -> None:
        users: list[dict] = kwargs['task_instance'].xcom_pull('fetch_users')

        conn_id: str = Variable.get('USER_DB_CONN_ID')
        table_name: str = Variable.get('USER_TABLE_NAME')

        custom_postgres_hook = CustomPostgresHook(
            conn_id=conn_id)
        custom_postgres_hook.insert_json(records=users, table_name=table_name)

    api = APIModelingOperator(
        task_id='fetch_users',
        method='GET',
        model=Users,
        url='https://randomuser.me/api/?results=10&nat=IN'
    )

    # --------------- Insert User Job Flow ---------------

    api >> insert_users()
