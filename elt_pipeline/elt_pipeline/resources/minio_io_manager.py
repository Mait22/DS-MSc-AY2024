from dagster import IOManager, OutputContext, InputContext
from minio import Minio
import pandas as pd

from contextlib import contextmanager
from datetime import datetime
from typing import Union
import os
from io import BytesIO
import time
from typing import List, Callable
from ..custom_functions.e_invoice_fetching_util import *


@contextmanager
def connect_minio(config):
    """
    Yields and manages connection to MINIO S3
    """
    client = Minio(
        endpoint=config.get("endpoint_url"),
        access_key=config.get("minio_access_key"),
        secret_key=config.get("minio_secret_key"), 
        secure = False, # No SSL TO-DO: Traefik reverse proxy
        cert_check=False # No SSL TO-DO: Traefik reverse proxy
    )
    try:
        yield client
    except Exception as e:
        raise e


# Make bucket if not exists

class MinIOIOManager(IOManager):
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


    def make_bucket(self, bucket_name: str):
        """
        Create bucket
        """
        try:
            with connect_minio(self._config) as client:

                found = client.bucket_exists(bucket_name)
                if not found:
                    client.make_bucket(bucket_name)
                    return True
                else:
                    return False

        except Exception as e:
            raise e


    def get_bucket_list(self):
        """
        Get bucket list of the client instance specified in the config
        """
        try:
            with connect_minio(self._config) as client:
                return [bucket.name for bucket in client.list_buckets()]
        except Exception as e:
            raise e
        

    def insert_df_rows_as_json(self, inputDf: pd.DataFrame, json_id_col: str, bucket_name: str):
        """
        Insert Pandas DataFrame as JSON files (1 record - > 1 file) to specified S3 bucket 
        """
        try:
            with connect_minio(self._config) as client:

                added_data_hits = []
                inputDf = inputDf.reset_index()

                for rn, row in inputDf.iterrows():
                    try:
                        data = inputDf.iloc[[rn]].to_json(orient="records").encode("utf-8")
                        client.put_object(
                            bucket_name,
                            f"{row[json_id_col]}.json",
                            data=BytesIO(data),
                            length=len(data),
                            content_type='application/csv'
                        )
                        added_data_hits.append(1)
                    except:
                        added_data_hits.append(0)

                return added_data_hits
                
        except Exception as e:
            raise e
        

    def insert_df_as_json(self, inputDFs: List, bucket_name: str):
        """
        Insert Pandas DataFrame as JSON file (1 DF - > 1 JSON file) to specified S3 bucket 
        """
        try:
            with connect_minio(self._config) as client:

                added_data_hits = []

                for fn_Df in inputDFs:
                    try:
                        data = fn_Df[list(fn_Df.keys())[0]].reset_index().to_json(orient="records").encode("utf-8")
                        client.put_object(
                            bucket_name,
                            f"{list(fn_Df.keys())[0]}.json",
                            data=BytesIO(data),
                            length=len(data),
                            content_type='application/csv'
                        )
                        added_data_hits.append(1)
                    except Exception as e:
                        added_data_hits.append(e)

                return added_data_hits
                
        except Exception as e:
            raise e


    def list_obj_in_bucket(self, bucket: str):
        """
        Get list of objects in specified bucket -> List literal
        """
        try:
            with connect_minio(self._config) as client:

                if client.bucket_exists(bucket):
                    objects = []
                    obj_gen = client.list_objects(bucket)
                    for obj in obj_gen:
                        objects.append(obj.object_name)
                    return objects
                else:
                    return []

        except Exception as e:
            raise e


    def download_handle_obj(self, bucket: str, file: str, handler_func: Callable):
        """
        Download specified file from specified bucket. 
        If file not present fail and yield FileNotFound errot.  
        Savee file to Dagster worker node temp file
        """
        try:
            with connect_minio(self._config) as client:

                if client.bucket_exists(bucket):

                    client.fget_object(bucket, file, f"/tmp/{file}")
                    df_xml = handler_func(f"/tmp/{file}")

                    return df_xml

                else:
                    return None

        except Exception as e:
            raise e


    def delete_file_from_bucket(self, bucket: str, file: str):
        """
        Delete specified file from specified bucket. 
        If file not present fail and yield FileNotFound errot.  
        """
        try:
            with connect_minio(self._config) as client:

                if client.bucket_exists(bucket):

                    client.remove_object(bucket, file)
                    return 1
                    
                else:
                    return None

        except Exception as e:
            raise e