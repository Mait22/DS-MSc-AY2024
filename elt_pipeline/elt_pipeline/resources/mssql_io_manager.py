from dagster import IOManager, OutputContext, InputContext
import pyodbc
import pandas as pd

from contextlib import contextmanager
from datetime import datetime
from typing import Union
from ..sql_snipets.ms_sql_snipets import generate_ms_dim_query

@contextmanager
def connect_mssql(config):
    """
    Yields and manages connection to MS SQL server
    """

    connectionString = f"""DRIVER={{ODBC Driver 18 for SQL Server}};
                        SERVER={config.get("mssql_server")};
                        DATABASE={config.get("mssql_database")};
                        UID={config.get("mssql_username")};
                        PWD={config.get("mssql_password")};
                        TrustServerCertificate=yes;
                        Trusted_Connection=no;Encrypt=yes"""
    
    conn = pyodbc.connect(connectionString) 

    try:
        yield conn
    except Exception as e:
        raise e


class MSSQLIOManager(IOManager):
    """
    Extend built in Dagster base class IOManager
    """
    def __init__(self, config):
        self._config = config

    def handle_output(self, context: "OutputContext"):
        """
        Not implemented
        """
        pass

    def load_input(self, context: "InputContext"):
        """
        Not implemented
        """
        pass

    def get_fin_dims(self, reg_code, invoice_number):
        """
        
        """
        try:
            with connect_mssql(self._config) as conn:
                sql_query = generate_ms_dim_query(reg_code, invoice_number)
                res = pd.read_sql(sql_query, conn)

                return res

        except Exception as e:
            raise e