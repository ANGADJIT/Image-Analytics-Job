from datetime import datetime, timedelta
from airflow.decorators import task
from airflow import DAG
from airflow.models import Variable
from plugins.hooks.custom_postgres_hook import CustomPostgresHook
from plugins.operators.emotion_extraction_operator import EmotionDetectionOperator
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'USER_PROFILE_EMOTION_EXTRACTION',
    default_args=default_args,
    description='''
        This dag will responsible for analyzing user profile to extract emotions model the reponse and to put into 
        the emotions table
    ''',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    max_active_runs=1,
    catchup=False
) as dag:

    @task
    def get_users_profile_info():
        # Get Connection Id and make connection
        conn_id: str = Variable.get('USER_DB_CONN_ID')
        postgres_hook = CustomPostgresHook(conn_id=conn_id)

        # Get Records of users for processing
        table_name: str = Variable.get('USER_TABLE_NAME')

        fields: list[dict] = ['id', 'profile_url']
        records: list[dict] = postgres_hook.get_records_as_dict(
            fields=fields, table_name=table_name)

        return records

    @task
    def filter_processed_users(user_info: list[dict]):

        def set_processed_users(processed_users: list[str]):
            processed_users_json_str = json.dumps(processed_users)

            Variable.set('PROCESSED_USERS', processed_users_json_str)

        filtered_users: list[dict] = []
        new_processed_users: list[str] = []

        try:
            processed_users_json_str: str = Variable.get('PROCESSED_USERS')
            processed_users: list[str] = json.loads(processed_users_json_str)

            for user in user_info:
                if user['id'] not in processed_users:
                    new_processed_users.append(user['id'])
                    filtered_users.append(user)

            # Extend previous list with new one
            processed_users.extend(new_processed_users)
            set_processed_users(processed_users=processed_users)

            return filtered_users

        except KeyError:
            for user in user_info:
                new_processed_users.append(user['id'])

            # Add new list to Variables
            set_processed_users(processed_users=new_processed_users)

            return user_info

    @task
    def detect_emotions(user_info: list[dict]):
        #! Executing operator Inside Task is not recommended [THIS CASE IS DIFFERENT]
        return EmotionDetectionOperator(
            task_id='GET_EMOTIONS',
            user_info=user_info
        ).execute({})

    @task
    def insert_emotions_data(emotions: list[dict]):
        # Make connection and insert emotions data in db
        conn_id: str = Variable.get('USER_DB_CONN_ID')
        user_emotions_table: str = Variable.get('USER_EMOTIONS_TABLE_NAME')

        postgres_hook = CustomPostgresHook(conn_id=conn_id)
        postgres_hook.insert_json(
            records=emotions, table_name=user_emotions_table)

    # ------------------User Profile Emotion Extraction Job Flow----------------

    user_info = get_users_profile_info()
    # Filter processed users
    unprocessed_users: list[dict] = filter_processed_users(user_info=user_info)

    emotions = detect_emotions(user_info=unprocessed_users)

    user_info >> unprocessed_users >> emotions >> insert_emotions_data(
        emotions=emotions)