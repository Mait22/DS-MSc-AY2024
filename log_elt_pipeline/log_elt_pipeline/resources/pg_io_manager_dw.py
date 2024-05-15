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
            password=config["password"],
        )
    except (Exception) as e:
        print(f"DW Postgs connection error: {e}")



class PgIOManagerDw(IOManager):
    """

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

    def load_data_from_bronze(self, lc_name: str, max_id: int):
        """
        
        """
        try:
            with connect_psql(self._config) as conn:
                conn.autocommit = True
                with conn.cursor() as cursor:

                    cursor.execute(query_data_from_bronze(lc_name, max_id))
                    df = pd.DataFrame(cursor.fetchall(), columns=["MainLogEventId", "LcName", "MainLogEventStartTime", "MainLogEventEndTime", "MainLogEventName", "StartErrorTypeName", "EndErrorTypeName", "created_at", "updated_at"])

        except (Exception) as e:
            conn.rollback()
            cursor.close() 
            conn.close()
            return False
        
        conn.close()
        return df
    

    def set_up_dw_log_data_landing(self):
        """
        
        """
        try:
            with connect_psql(self._config) as conn:
                conn.autocommit = True
                with conn.cursor() as cursor:

                    cursor.execute(create_pg_schema("level_crossing_logs"))

                    cursor.execute(create_updated_at_trigger("level_crossing_logs"))

                    cursor.execute(create_log_bronze_table("level_crossing_logs", "source_log_data"))

                    cursor.execute(create_matched_events_table("level_crossing_logs", "matched_log_data"))

                    cursor.execute(create_alert_criteria_table("level_crossing_logs", "criteria_vals"))

        except (Exception) as e:
            conn.rollback()
            cursor.close() 
            conn.close()
            return False
        
        conn.close()
        return True
    
    def get_max_loaded_id_val(self, schema: str, table: str, id_col: str):
        """
        
        """
        try:
            with connect_psql(self._config) as conn:
                conn.autocommit = True
                with conn.cursor() as cursor:

                    cursor.execute(f"""select max("{id_col}") from "{schema}"."{table}" """)
                    res = cursor.fetchall()

        except (Exception) as e:
            conn.rollback()
            cursor.close() 
            conn.close()
            return False
        
        conn.close()
        return res

    def upload_df_to_dw(self, 
                        df_to_upload: pd.DataFrame,
                        df_columns_to_upload: list,
                        db_columns: list,
                        schema: str,
                        table: str,
                        db_key_col: str):
            """
            
            """
            value_tuples = [tuple(x) for x in df_to_upload[df_columns_to_upload].to_numpy()]

            db_cols_list = ','.join(list(db_columns))
            insert_query = """
            insert into %s."%s"(%s) values %%s
            on conflict ("%s")
                do update set
                (%s)
                = (%s);
            """ % (schema, table, db_cols_list, db_key_col, db_cols_list, ','.join(list(["EXCLUDED." + cn for cn in db_columns])))

            try:
                with connect_psql(self._config) as conn:
                    conn.autocommit = True
                    with conn.cursor() as cursor:
                            try: 
                                extras.execute_values(cursor, insert_query, value_tuples) 
                                conn.commit() 
                            except (Exception, psycopg2.DatabaseError) as error:
                                conn.rollback() 
                                cursor.close() 
                                conn.close()
                                return error
            except (Exception) as e:
                conn.rollback()
                cursor.close() 
                conn.close()
                return value_tuples
            
            conn.close()
            return True