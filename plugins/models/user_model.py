from pydantic import BaseModel, EmailStr, AnyHttpUrl


class Picture(BaseModel):
    large: AnyHttpUrl


class Result(BaseModel):
    email: EmailStr
    picture: Picture


class User(BaseModel):
    results: list[Result]
