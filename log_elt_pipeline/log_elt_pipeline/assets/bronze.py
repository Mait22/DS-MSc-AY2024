from dagster import asset, AssetIn, AssetExecutionContext
from datetime import datetime
import pandas as pd
from typing import List, Dict
from ..custom_functions.log_matcher_util import *
import time
import os
from distutils.util import strtobool


## Levelcrossing log data fetching
@asset(group_name="extract_log_data", 
       key_prefix=["bronze"],
       required_resource_keys={"pg_io_manager_dw"}
       ) 
def set_up_dw_landing(context: AssetExecutionContext) -> bool:
    """
    Creates Postgres DW landing tables and trigges. Skips the step at DB level if the tables already exist. 
    If 
    """
    res = context.resources.pg_io_manager_dw.set_up_dw_log_data_landing()
    if bool(strtobool(os.getenv('LOGGING_ON'))):
        context.log.info(res)
    return res

@asset(group_name="extract_log_data", 
       key_prefix=["bronze"],
       required_resource_keys={"pg_io_manager","pg_io_manager_dw"},
       ins={
       "landing_done": AssetIn(key="set_up_dw_landing")
       },
       ) 
def load_raw_data(context: AssetExecutionContext, 
                  landing_done: bool) -> bool:
    """
    Loads raw data from level crossing log database. 
    """
    # Source data is ingested in batches - it shows the row count of one ingestion batch
    LOAD_INCREMENT = 500

    # Source database columns to ingest, columns refere to table returned by the data 
    # extraction query
    DB_COLS = ['"LcName"',
               '"MainLogEventId"',
               '"MainLogEventStartTime"',
               '"MainLogEventEndTime"',
               '"MainLogEventName"',
               '"StartErrorTypeName"',
               '"EndErrorTypeName"']
    
    # Check if DW landing tables are set
    if not landing_done:
        return False 
    
    # Get alreadly loaded log data max ID - log data is immutable 
    # and surce log entries are appended by incrementing ID 
    current_max_id = context.resources.pg_io_manager_dw.get_max_loaded_id_val("level_crossing_logs", 
                                                                              "source_log_data", 
                                                                              "MainLogEventId")
    if current_max_id[0][0] is None:
        current_max_id = 0

    if bool(strtobool(os.getenv('LOGGING_ON'))):
        context.log.info(current_max_id)

    # Load new data
    to_load = context.resources.pg_io_manager.load_bronze_data(current_max_id)
    to_load = to_load.reset_index()
    if bool(strtobool(os.getenv('LOGGING_ON'))):
        context.log.info(to_load)

    # If nothing to load return false
    if to_load.shape[0] == 0:
        return False
    
    # For logging per increment successes
    load_results = []

    # Incremental loading from source to DW staging (bronze) layer
    if to_load.shape[0] >= LOAD_INCREMENT:
       for i in range(to_load.shape[0] % LOAD_INCREMENT):

              load_result = context.resources.pg_io_manager_dw.upload_df_to_dw(to_load.iloc[i*LOAD_INCREMENT:((i+1)*LOAD_INCREMENT)], 
                                                                      [0,1,2,3,5,8,11], # Postgres I/O class returns data without column names, return DF is column-named by col idx
                                                                      DB_COLS,
                                                                      "level_crossing_logs",
                                                                      "source_log_data",
                                                                      "MainLogEventId")
              
              load_results.append(load_result)
       
       reminder_start = (to_load.shape[0] % LOAD_INCREMENT) * LOAD_INCREMENT
       load_result = context.resources.pg_io_manager_dw.upload_df_to_dw(to_load.iloc[reminder_start:to_load.shape[0]], 
                                                               [0,1,2,3,5,8,11], # Postgres I/O class returns data without column names, return DF is column-named by col idx
                                                               DB_COLS,
                                                               "level_crossing_logs",
                                                               "source_log_data",
                                                               "MainLogEventId")
              
       load_results.append(load_result)

    # If new dataset is smaller than load increment
    else:
       load_result = context.resources.pg_io_manager_dw.upload_df_to_dw(to_load, 
                                                               [0,1,2,3,5,8,11], # Postgres I/O class returns data without column names, return DF is column-named by col idx
                                                               DB_COLS,
                                                               "level_crossing_logs",
                                                               "source_log_data",
                                                               "MainLogEventId")
              
       load_results.append(load_result)

    if bool(strtobool(os.getenv('LOGGING_ON'))):
        context.log.info(load_results)
    return True


@asset(group_name="extract_log_data", 
       key_prefix=["bronze"],
       required_resource_keys={"pg_io_manager_dw"},
       ins={
       "loading_done": AssetIn(key="load_raw_data")
       },
       ) 
def get_data_to_analyze(context: AssetExecutionContext, loading_done: bool) -> Dict:
    """
    Load data from Postgre's DW landing table. 
    Create output dict -> {"LC_name": "Staging data in DF"}
    """

    if not loading_done:
        if bool(strtobool(os.getenv('LOGGING_ON'))):
            context.log.info("Previous step failed")
        return False
    
    # LC-s included
    LC_TO_INCLUDE = ['Kulli', 'Ruusa', 'Auvere', 'Nõo', 'Irvala', 'Vägeva', 'Sangaste', 'Lehtse', 'Lagedi', 'Soldina', 'Orava', 'Mustjõe', 'Poldri', 'Kohtla', 'Elva', 'Parila', 'Kesk-kaar', 'Kuru', 'Tambre', 'Keeni', 'Näki', 
                   'Betooni', 'Rakvere', 'Aegviidu', 'Moe', 'Aiamaa', 'Põlva', 'Mullavere', 'Palupera', 'Sonda', 'Ülejõe', 'Kalme', 'Holvandi', 'Tabivere', 'Sootaga', 'Jäneda', 'Tamsalu', 
                   'Oru', 'Lemmatsi', 'Ilumetsa', 'Kiviõli', 'Kadrina', 'Kehra', 'Sompa', 'Sordi', 'Taevaskoja', 'Mägiste', 'Tapa', 'Aardla', 'Püssi', 'Kiidjärve', 'Kabala', 
                   'Kalevi', 'Tiksoja', 'Imastu', 'Puka', 'Mõneku', 'Kärkna', 'Ropka', 'Peedu', 'Aruküla', 'Raasiku']
    
    load_results = {}

    for lc in LC_TO_INCLUDE:
        load_result = context.resources.pg_io_manager_dw.load_data_from_bronze(lc, 0)
        if bool(strtobool(os.getenv('LOGGING_ON'))):
            context.log.info(load_result)
        load_results[lc] = load_result

    if bool(strtobool(os.getenv('LOGGING_ON'))):
        context.log.info(load_results)

    return load_results


@asset(group_name="extract_log_data", 
       key_prefix=["bronze"],
       required_resource_keys={"pg_io_manager_dw"},
       ins={
       "raw_log_data": AssetIn(key="get_data_to_analyze")
       },
       ) 
def match_events(context: AssetExecutionContext, 
                 raw_log_data: Dict) -> Dict:
    """
    Create train event passing matchings - enriches input DFs with matching columns
    """

    if False in [lambda x: True if isinstance(x, pd.DataFrame) else False for x in raw_log_data.values()]:
        if bool(strtobool(os.getenv('LOGGING_ON'))):
            context.log.info("Previous step failed")
            context.log.info(raw_log_data)
        return {}
    
    pat_match_results = {}

    for lc in raw_log_data.keys():
        try:
            pat_match_result = log_pattern_matcher(raw_log_data[lc],
                                                    "MainLogEventName",
                                                    "MainLogEventId",
                                                    lc)
            if bool(strtobool(os.getenv('LOGGING_ON'))) and bool(strtobool(os.getenv('SENSITIVE_LOGGING_ON'))):
                context.log.info(pat_match_result)
            pat_match_results[lc] = pat_match_result["filtered_events"]

        except:
            pat_match_results[lc] = False

    if bool(strtobool(os.getenv('LOGGING_ON'))) and bool(strtobool(os.getenv('SENSITIVE_LOGGING_ON'))):
        context.log.info(pat_match_results)

    return pat_match_results


@asset(group_name="extract_log_data", 
       key_prefix=["bronze"],
       required_resource_keys={"pg_io_manager_dw"},
       ins={
       "matched_events": AssetIn(key="match_events")
       },
       ) 
def load_matched_events_to_dw(context: AssetExecutionContext, 
                              matched_events: Dict
                              ) -> Dict:
    """
    Load matching results (original df + train passing events matching columns) to output Postgres DW table. 
    """

    load_results = {}

    for lc in matched_events.keys():
        try:
            res =  matched_events[lc].dropna(subset=['MatchedEventId'])
      
            converted__delta_series = res["MatchedEventDuration"].apply(lambda x : x.seconds)
            res["MatchedEventDuration"] = converted__delta_series

            if bool(strtobool(os.getenv('LOGGING_ON'))):
                context.log.info(res)

            load_result = context.resources.pg_io_manager_dw.upload_df_to_dw(res, 
                                                                    ["LcName",
                                                                     "MainLogEventStartTime",
                                                                     "MainLogEventId",
                                                                     "MatchedEventId",
                                                                     "MatchedEventDuration",
                                                                     "MatchedEventSignature"],
                                                                    ['"LcName"',
                                                                     '"MainLogEventStartTime"',
                                                                     '"MainLogEventId"',
                                                                     '"MatchedEventId"',
                                                                     '"MatchedEventDuration"',
                                                                     '"MatchedEventSignature"'],
                                                                    "level_crossing_logs",
                                                                    "matched_log_data",
                                                                    "MainLogEventId")
            load_results[lc] = load_result

        except:
            load_results[lc] = False

    if bool(strtobool(os.getenv('LOGGING_ON'))):
        context.log.info(load_results)

    return load_results