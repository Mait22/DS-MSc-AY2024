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
import pysftp
from datetime import datetime, timedelta


@contextmanager
def connect_sftp(config):
    """
    Yields and manages connection to on-cluster SFTP. 
    Whitelisted to localhost.
    """
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None 

    client = pysftp.Connection(
        username=config.get("sftp_user"),
        password=config.get("sftp_pass"),
        host='10.100.35.229',
        port=2222,
        cnopts=cnopts
    )

    try:
        yield client
    except Exception as e:
        raise e

## Extend Dagster inbuilt I/O manager class
class SFTPManager(IOManager):
    """
    I/O class to connect with SFTP resource.
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


    def get_files_in_sftp(self, 
                          time_delta_in_days: int = 3, 
                          size_limit_in_bytes: int = 1_048_576 * 4):
        """
        Get files in in SFTP drive
        """
        try:
            with connect_sftp(self._config) as sftp:
                files = sftp.listdir_attr("./upload/")
                files_ls = []

                ini_time_for_now = datetime.now()
                after = ini_time_for_now - timedelta(days = time_delta_in_days)
                before = ini_time_for_now - timedelta(days = 0)

                for f in files:
                    if (datetime.fromtimestamp(f.st_mtime) >= after and datetime.fromtimestamp(f.st_mtime) <= before and f.st_size < size_limit_in_bytes):
                        files_ls.append(f.filename)
                return files_ls

        except Exception as e:
            raise e
        

    def run_xml_validity_check(self, filename: str, val_check_handler_func: Callable):
        """
        Check if serialized storage blob is valid XML
        File is !!!not!!! stored to Dagster worker node tmp folder - parsed and checked in-memory.  
        """
        try:
            with connect_sftp(self._config) as sftp:
                try:
                    with sftp.open("./upload/" + filename) as to_test:
                        test_res = val_check_handler_func(to_test)
                        return test_res
                except:
                    return None

        except Exception as e:
            raise e