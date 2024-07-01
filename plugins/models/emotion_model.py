from pydantic import BaseModel, Field


class Emotion(BaseModel):
    emotion: str = Field(alias='dominant_emotion')
    race: str = Field(alias='dominant_race')
    age: int
    gender: str = Field(alias='dominant_gender')

