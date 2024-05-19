from typing import Any
from airflow.models.baseoperator import BaseOperator
from urllib.parse import urlparse
from airflow.utils.context import Context
from requests import Response
import requests
from pydantic import BaseModel


class APIModelingOperator(BaseOperator):
    '''
        APIModelingOperator Will allow you to model the response which uses pydantic model's to model the response

        Params:
            method: HTTP Method ['GET', 'PUT', 'POST', 'PATCH', 'DELETE']
            url: Api endpoint(Full API URL)
            model: Model for Response modeling and validation
            request_params(Optional): extra API params like auth,Headers ETC
            custom_parser(Optional):  By default uses json parse but you give you own custom parser like for xml,yaml etc

        Returns:
            dict: modeled and validated response from pyndatic model
    '''

    def __init__(self, method: str, url: str, model: BaseModel, request_params: dict = {}, custom_parser=None, **kwargs):
        super().__init__(**kwargs)

        # Init HTTP Method
        if method in ['GET', 'PUT', 'POST', 'PATCH', 'DELETE']:
            self.__method = method
        else:
            raise ValueError(f'Invalid HTTP Method got {method}')

        # Init URL and Extra ARGS
        try:
            urlparse(url=url)

            self.__url: str = url
        except ValueError as e:
            raise e

        if isinstance(kwargs, dict):
            self.__extra_params: dict = kwargs

            # remove task id
            self.__extra_params.pop('task_id')
        else:
            raise ValueError(
                f'Invalid KWARGS required dict getting {type(kwargs)}')

        # Assign custom parser
        if custom_parser is not None and not callable(custom_parser):
            raise Exception(
                f'Custom parser should a callable got {type(custom_parser)}')

        self.__custom_parser = custom_parser

        # Assign model
        if not issubclass(model, BaseModel):
            raise ValueError(
                f'APIModelingOperator works only with pydantic models but got {type(model)}')
        else:
            self.__model = model

        # Assign request params
        if not isinstance(request_params, dict):
            raise ValueError(
                f'Requests params should be type of <dict> got {type(request_params)}')

        self.__request_params: dict = request_params

    def execute(self, context: Context) -> Any:
        response: Response = requests.request(
            method=self.__method, url=self.__url, **self.__request_params)

        if self.__custom_parser is not None:
            body: str = response.text

            parsed_body: dict = self.__custom_parser(body)

        else:
            parsed_body: dict = response.json()

        # Model the body
        return self.__model(**parsed_body).json()
