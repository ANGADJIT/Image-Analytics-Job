from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from deepface import DeepFace


class EmotionDetectionOperator(BaseOperator):

    def __init__(self, task_id: str, user_info: list[dict], **kwargs):
        super.__init__(**kwargs)

        self.__user_info: list[dict] = user_info

    def execute(self, context: Context) -> list[dict]:
        emotions: list[dict] = []

        for info in self.__user_info:
            pass
