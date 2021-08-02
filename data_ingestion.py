from __future__ import annotations
from typing import Callable
import psycopg2
import configparser
import pyathena
import pandas as pd


class DataBase:

    engine: str
    conn_func: Callable[[any], "Connection"]

    def __init__(self, **kwargs):
        # TODO: self.credentials: 'Credentials' = None # To be evaluated instead of kwargs
        # TODO: Explore the possibility of property with setter/getter
        self.conn: 'Connection' = self.get_conn_object(**kwargs)

    @classmethod
    def get_conn_object(cls, **kwargs) -> "Connection":
        conn = cls.conn_func(**kwgars)
        return conn

    @classmethod
    def query(cls, query: str, parameters: list) -> pd.DataFrame:

        # Read the sql file, if necessary
        if '.sql' in query:
            with open(query, 'r', encoding='utf_8') as sql_file:
                query_str = sql_file.read()
        else:
            query_str = query
        # Assign the parameters to the query
        if parameters is not None:
            if isinstance(parameters, list):
                parameters = ["'" + "', '".join([str(y) for y in x]) + "'" if isinstance(x, list) else x
                              for x in parameters]
                query_str = query_str.format(*parameters)
            else:
                raise ValueError("'parameters' should be a list object")

        # Execute and store the results in a DataFrame
        df_query = pd.read_sql(query_str, con=cls.conn)
        return df_query

        # TODO: define if it will an abstact method
    def connect_with_config(self, config_file: str):
        parser = configparser.ConfigParser()
        parser.read(config_file)
        pass


class Postgres(DataBase):

    engine = 'PostgreSQL'
    conn_func = psycopg2.connect


    def __init__(self, hostname: str, port: str, dbname: str,
                 username: str, password: str):
        # super().__init__(conn=self.connect()) # Override instead?
        pass


class Athena(DataBase):

    engine = 'AWS Athena'
    conn_func = pyathena.connect



