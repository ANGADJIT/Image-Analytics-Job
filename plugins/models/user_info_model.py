from pydantic import BaseModel, Field,HttpUrl


class UserInfo(BaseModel):
    user_id: str = Field(alias='id')
    profile_url: HttpUrl
