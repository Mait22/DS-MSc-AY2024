import psycopg2
from psycopg2 import sql
import psycopg2.extras as extras
import pandas as pd
import numpy as np
from dagster import IOManager, InputContext, OutputContext
from contextlib import contextmanager
from ..sql_snipets.pg_sql_snipets import *
from ..custom_functions.log_matcher_util import *

@contextmanager
def connect_psql(config):
    try:
        yield psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"]
        )
    except (Exception) as e:
        print(f"DW Postgs connection error: {e}")


class PgIOManagerLx(IOManager):
    """
    Creates extended I/O class for InfraLX level crossing logs database
    """

    def __init__(self, config):
        self._config = config

    def handle_output(self, context):
        """
        Not implemented
        """
        pass

    def load_input(self, context):
        """
        Not implemented
        """
        pass

    def load_bronze_data(self, continue_from_id: int):
        """
        
        """
        try:
            with connect_psql(self._config) as conn:
                conn.autocommit = True
                with conn.cursor() as cursor:

                    cursor.execute(query_raw_data(continue_from_id))
                    df = pd.DataFrame(cursor.fetchall())
                    
        except (Exception) as e:
            conn.rollback()
            cursor.close() 
            conn.close()
            return False
        
        conn.close()
        return df


   