from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from deepface import DeepFace
from plugins.models.user_info_model import UserInfo
from plugins.models.emotion_model import Emotion
from PIL import Image
import numpy as np
import requests
from io import BytesIO


class EmotionDetectionOperator(BaseOperator):

    def __init__(self, user_info: list[dict], **kwargs):
        super().__init__(**kwargs)

        # Model User Info Model
        self.__user_info: list[UserInfo] = [
            UserInfo(**info) for info in user_info]

    def __get_image_from_url(self, url: str):
        try:
            # Get Image
            response = requests.get(url)

            # Make a PIL image from the response
            image = Image.open(BytesIO(response.content))

            # Convert PIL image to Np Array
            return np.array(image)
        except:
            return None

    def execute(self, context: Context) -> list[dict]:
        emotions: list[dict] = []

        for info in self.__user_info:

            # Get url and create np image
            url = str(info.profile_url)
            profile = self.__get_image_from_url(url=url)

            if profile is None:
                continue

            # Extract Emotions from Image
            try:
                print('Started Analyzing for user', info.user_id)
                emotion: dict = DeepFace.analyze(img_path=profile)
                print('Completed Analytics for user', info.user_id)
            except:
                emotion = None

            if emotion is not None:
                # Model the emotions
                modeled_emotions = Emotion(**emotion[0])

                # Add user ID
                modeled_emotions = modeled_emotions.model_dump()
                modeled_emotions['user_id'] = info.user_id

                # Add to list
                emotions.append(modeled_emotions)

        return emotions
