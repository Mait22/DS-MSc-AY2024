import psycopg2
from psycopg2 import sql
import psycopg2.extras as extras
import pandas as pd
import numpy as np
from dagster import IOManager, InputContext, OutputContext
from contextlib import contextmanager
from ..sql_snipets.pg_sql_snipets import *
from ..custom_functions.e_invoice_fetching_util import *

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



class PgIOManager(IOManager):
    """
    DW interfacing I/O manager class by extending base I/O class.
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

    def set_up_dw_xml_landing(self):
        """
        Create landing schema, tables, procedures and triggers tables in temp Postgres DW
        """
        try:
            with connect_psql(self._config) as conn:
                conn.autocommit = True
                with conn.cursor() as cursor:

                    cursor.execute(create_pg_xml_schema("invoice_xmls_sc"))

                    cursor.execute(create_updated_at_trigger("invoice_xmls_sc"))

                    cursor.execute(create_pg_account_dim_table("invoice_xmls_sc","dim_account"))

                    cursor.execute(create_pg_dim_table("invoice_xmls_sc","dim_department","Department"))

                    cursor.execute(create_pg_dim_table("invoice_xmls_sc","dim_main_asset","MainAsset"))

                    cursor.execute(create_pg_dim_table("invoice_xmls_sc","dim_cost_center","CostCenter"))

                    cursor.execute(create_pg_dim_table("invoice_xmls_sc","dim_project","Project"))

                    cursor.execute(create_pg_dim_table("invoice_xmls_sc","dim_contract","Contract"))

                    cursor.execute(create_pg_xml_fact_table("invoice_xmls_sc","fact_invoice_xml"))

                    cursor.execute(create_pg_xml_mn_table(
                        schema_name = "invoice_xmls_sc",
                        first_table = "fact_invoice_xml",
                        second_table = "dim_account",
                        first_table_pk = "SurrogateKey",
                        second_table_pk = "MainAccountId",
                        first_table_pk_type = "varchar",
                        second_table_pk_type = "varchar"
                    ))

                    cursor.execute(create_pg_xml_mn_table(
                        schema_name = "invoice_xmls_sc",
                        first_table = "fact_invoice_xml",
                        second_table = "dim_contract",
                        first_table_pk = "SurrogateKey",
                        second_table_pk = "ContractId",
                        first_table_pk_type = "varchar",
                        second_table_pk_type = "bigint"
                    ))

                    cursor.execute(create_pg_xml_mn_table(
                        schema_name = "invoice_xmls_sc",
                        first_table = "fact_invoice_xml",
                        second_table = "dim_cost_center",
                        first_table_pk = "SurrogateKey",
                        second_table_pk = "CostCenterId",
                        first_table_pk_type = "varchar",
                        second_table_pk_type = "bigint"
                    ))

                    cursor.execute(create_pg_xml_mn_table(
                        schema_name = "invoice_xmls_sc",
                        first_table = "fact_invoice_xml",
                        second_table = "dim_department",
                        first_table_pk = "SurrogateKey",
                        second_table_pk = "DepartmentId",
                        first_table_pk_type = "varchar",
                        second_table_pk_type = "bigint"
                    ))

                    cursor.execute(create_pg_xml_mn_table(
                        schema_name = "invoice_xmls_sc",
                        first_table = "fact_invoice_xml",
                        second_table = "dim_main_asset",
                        first_table_pk = "SurrogateKey",
                        second_table_pk = "MainAssetId",
                        first_table_pk_type = "varchar",
                        second_table_pk_type = "bigint"
                    ))

                    cursor.execute(create_pg_xml_mn_table(
                        schema_name = "invoice_xmls_sc",
                        first_table = "fact_invoice_xml",
                        second_table = "dim_project",
                        first_table_pk = "SurrogateKey",
                        second_table_pk = "ProjectId",
                        first_table_pk_type = "varchar",
                        second_table_pk_type = "bigint"
                    ))

        except (Exception) as e:
            conn.rollback()
            cursor.close() 
            conn.close()
            return False
        
        conn.close()
        return True

    def upload_df_to_dw(self, 
                        df_to_upload: pd.DataFrame,
                        df_columns_to_upload: list,
                        db_columns: list,
                        schema: str,
                        table: str,
                        db_key_col: str):
            """
            Upsert data to specified Postgers DW table from Pandas DF 
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
                return e
            
            conn.close()
            return True