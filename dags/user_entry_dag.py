from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import task
from plugins.operators.api_modeling_operator import APIModelingOperator
from plugins.models.user_model import Users
from airflow.models import Variable
from plugins.hooks.custom_postgres_hook import CustomPostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
}

with DAG(
    'USER_ENTRY_DAG',
    default_args=default_args,
    description='''
        This dag will responsible for inserting random users into our Users Table
    ''',
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2024, 1, 1),
    max_active_runs=1,
    catchup=False
) as dag:

    @task
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
        url=f'https://randomuser.me/api/?results={Variable.get("RESULTS")}&nat={Variable.get("NAT")}'
    )

    # Trigger Emotion Extraction DAG After User Entry Dag
    trigger_emotions_extraction_service = TriggerDagRunOperator(
        task_id='trigger_emotions_extraction_service',
        trigger_dag_id='USER_PROFILE_EMOTION_EXTRACTION',
        wait_for_completion=False
    )

    # --------------- Insert User Job Flow ---------------

    api >> insert_users() >> trigger_emotions_extraction_service
