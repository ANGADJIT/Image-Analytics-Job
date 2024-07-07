from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException
import psycopg2


class CustomPostgresHook(BaseHook):
    '''
        Custom Postgres Hook for handling JSON data insertion into a PostgreSQL table.
        This hook provides methods to insert JSON data into a specified PostgreSQL table. 

        Attributes:
            conn_id (str): Connection ID for the PostgreSQL database.
    '''

    def __init__(self, conn_id: str, logger_name: str | None = None):
        super().__init__(logger_name)

        self.__conn_id: str = conn_id
        self.__init_connection()

    def __init_connection(self):
        try:
            connection = self.get_connection(conn_id=self.__conn_id)
            self.__conn = psycopg2.connect(
                host=connection.host, database=connection.schema, user=connection.login, password=connection.password)
            self.__cursor = self.__conn.cursor()

        except AirflowNotFoundException as e:
            raise Exception(e)

    def insert_json(self, records: list[dict], table_name: str):
        '''
            Inserts JSON data directly into a specified table.

            Attributes:
                records (list[dict]): A list of dictionaries representing JSON records to be inserted.
                table_name (str): The name of the table where the records will be inserted.

            Args:
                records (list[dict]): The JSON records to be inserted.
                table_name (str): The name of the table for insertion.

            Returns:
                None
        '''

        if len(records) > 0 and isinstance(records[0], dict):
            columns: tuple[str] = None

            for record in records:
                if columns is None:
                    columns = tuple(records[0].keys())
                    columns_query = str(columns).replace("'", '')

                # Create values here
                values: tuple = tuple([record[col] for col in columns])

                INSERT_QUERY: str = f'''INSERT INTO {table_name} {columns_query} VALUES {values}'''

                # Execute Query with values
                self.__cursor.execute(INSERT_QUERY)

            self.__conn.commit()

        else:
            raise Exception('Empty records or invalid record type')

    def get_records_as_dict(self, fields: list[str], table_name: str) -> list[dict]:
        # Make Select Query
        SELECT_QUERY: str = f"SELECT {','.join(fields)} FROM {table_name}"

        # Execute Query Here
        self.__cursor.execute(SELECT_QUERY)

        # Get columns names
        columns: list[str] = [
            column.name for column in self.__cursor.description]

        # Map values with columns names and create dictionary
        records: list[tuple] = self.__cursor.fetchall()
        dict_records: list[dict] = []

        for record in records:
            # Map record and add to dict records
            dict_record: dict = dict(zip(columns, record))
            dict_records.append(dict_record)

        return dict_records

    def __del__(self):
        self.__conn.close()
