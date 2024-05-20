from typing import Any, Callable, Dict, Set
from pydantic import BaseModel, EmailStr, AnyHttpUrl, Field
from pydantic_core import PydanticUndefined


class Picture(BaseModel):
    large: AnyHttpUrl


class Street(BaseModel):
    name: str
    number: int


class Location(BaseModel):
    street: Street
    city: str
    state: str
    country: str


class Login(BaseModel):
    username: str


class Result(BaseModel):
    email: EmailStr
    picture: Picture
    location: Location = Field(init=False)
    login: Login


class Users(BaseModel):
    results: list[Result]

    def json(self, *, include: Set[int] | Set[str] | Dict[int, Any] | Dict[str, Any] | None = None, exclude: Set[int] | Set[str] | Dict[int, Any] | Dict[str, Any] | None = None, by_alias: bool = False, exclude_unset: bool = False, exclude_defaults: bool = False, exclude_none: bool = False, encoder: Callable[[Any], Any] | None = ..., models_as_dict: bool = ..., **dumps_kwargs: Any):

        records: list[Dict] = []

        for result in self.results:
            json: dict = {}

            json['email'] = result.email
            json['profile_url'] = str(result.picture.large)

            location = result.location
            json['address'] = f'{location.street.number} {location.street.name},{location.city} {location.state},{location.country}'

            json['username'] = result.login.username

            records.append(json)

        return records
