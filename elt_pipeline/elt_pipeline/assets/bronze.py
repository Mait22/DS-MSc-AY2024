from dagster import asset, AssetIn, AssetExecutionContext
from datetime import datetime
import pandas as pd
from typing import List, Dict
from ..custom_functions.e_invoice_fetching_util import *
import time
import re
import os
from distutils.util import strtobool

## E-invoice XML fetching assets
@asset(group_name="extract_purchase_e_invoices", 
       key_prefix=["bronze"],
       required_resource_keys={"minio_io_manager","sftp_io_manager"}
       ) 
def get_files_to_parse(context: AssetExecutionContext) -> List:
    """
    Step 1. - Get all files from SFTP; 
    Step 2. - Check if already parsed; 

    Return -  List of files to parse -> ["str", "str", ...] 
    """
    logging_on = bool(strtobool(os.getenv('LOGGING_ON')))

    GET_FILES_FROM_LAST_X_DAYS = 3 # Look only files from SFTP from last X days.  
    FILE_SIZE_LIMIT_MB = 4 # To discard large non-XML attachments like Excel files, dwg and image files etc.

    # Get previously checked files list
    parsed_files = context.resources.minio_io_manager.list_obj_in_bucket('d365-xml-check-logs')
    parsed_files = [re.split("[ _| .]", x)[0] for x in parsed_files]
    if logging_on:
        context.log.info(f"Already parsed files: {parsed_files}")
        context.log.info(f"Number of already parsed files: {len(parsed_files)}")

    # Get list of available files from SFTP
    available_files = context.resources.sftp_io_manager.get_files_in_sftp(time_delta_in_days=GET_FILES_FROM_LAST_X_DAYS, 
                                                                          size_limit_in_bytes= 1_048_576 * FILE_SIZE_LIMIT_MB)
    available_files = [re.split("[ _| .]", x)[0] for x in available_files]
    if logging_on:
        context.log.info(f"Available files: {available_files}")
        context.log.info(f"Number of available files: {len(available_files)}")

    # Get list of files that haven't been parsed via set difference
    files_to_parse = list(set(available_files) - set(parsed_files))
    if logging_on:
        context.log.info(f"Files to parse: {files_to_parse}")
        context.log.info(f"Number of files to parse: {len(files_to_parse)}")

    return files_to_parse


@asset(group_name="extract_purchase_e_invoices", 
       key_prefix=["bronze"],
       required_resource_keys={"minio_io_manager","sftp_io_manager"},
       ins={
       "files_to_parse": AssetIn(key="get_files_to_parse")
       },
       ) 
def get_new_XMLs(context: AssetExecutionContext, files_to_parse: List) -> List:
    """
    Step 1. - Check if valid XML file
    Step 2. - Log SFTP checks -> filename_timestamp.json
    Step 3. - Log valid files -> filename_timestamp.json
    Step 4. - Log errenouse files -> filename_timestamp.json

    Return - Valid XML-s in parsed form -> [{filename:parsed_obj_serialized_to_str}]
    """
    logging_on = bool(strtobool(os.getenv('LOGGING_ON')))

    if logging_on:
        context.log.info(files_to_parse)

    # Checking files
    valid_xmls = []

    for f_p in files_to_parse:
        file_check = context.resources.sftp_io_manager.run_xml_validity_check(f_p, check_if_valid_xml)
        parse_log_entry = {"file_name": f_p + "_" + str(time.time()).replace('.',''), "date_time": datetime.now()}

        # Add XML-file check log entry for a given file to a Minio S3 bucket  
        _ = context.resources.minio_io_manager.insert_df_rows_as_json(pd.DataFrame(parse_log_entry, index=[0]), 
                                                                             'file_name', 
                                                                             'd365-xml-check-logs')
        
        
        if file_check is not None:
            valid_xmls.append({f_p: file_check})
            # Add "is valid" XML-file check log entry for a given file to a Minio S3 bucket
            _ = context.resources.minio_io_manager.insert_df_rows_as_json(pd.DataFrame(parse_log_entry, index=[0]), 
                                                                             'file_name', 
                                                                             'd365-valid-xmls')
    
    if logging_on:
        context.log.info(f"Added {len(files_to_parse)} XML check log entries")
        context.log.info(f"Added {len(valid_xmls)} valid XML log entries")
    
    return valid_xmls


@asset(group_name="extract_purchase_e_invoices", 
       key_prefix=["bronze"],
       required_resource_keys={"minio_io_manager"},
       ins={
       "valid_e_invoice_XMLs": AssetIn(key="get_new_XMLs")
       },
       ) 
def XML_to_results_df(context: AssetExecutionContext, 
                      valid_e_invoice_XMLs: List) -> List:
    """
    Step 1. - Parse valid XML-s (str to etree obejct)
    Step 2. - Run fetching (etree XML to df)

    Return - [{XML_file:e-invoice rows in Pandas dataframe}]
    """
    logging_on = bool(strtobool(os.getenv('LOGGING_ON')))
    sensitive_logging_on = bool(strtobool(os.getenv('SENSITIVE_LOGGING_ON')))
    
    return_dfs = []

    for el in valid_e_invoice_XMLs:
        try:
            r = xml_to_df(el)
            #r = suggest_to_clean(r) # Experimental part of XML-fetching util function, currently not turned on
            r = generate_surrogate_key(r)
            return_dfs.append({list(el.keys())[0]: r})
        except Exception as e:
            parse_log_entry = {"file_name": list(el.keys())[0] + "_" + str(time.time()).replace('.',''), "date_time": datetime.now()}
            # If XML to df step throws an error - it is logged to Minio S3 bucket "xml-to-df-errors".
            # Those entries should be handled by a separate error-correction data pipeline - currently not implemented.
            # Future re-runs will not re-generate or re-fetch the error-producing XML to df step as given XML is already logged in "d365-xml-check-logs" 
            # (See previous pipeline step: get_new_XMLs).   
            _ = context.resources.minio_io_manager.insert_df_rows_as_json(pd.DataFrame(parse_log_entry, index=[0]), 
                                                                                'file_name', 
                                                                                'xml-to-df-errors')
    if logging_on and sensitive_logging_on:
        context.log.info(return_dfs)

    return return_dfs


@asset(group_name="extract_purchase_e_invoices", 
       key_prefix=["bronze"],
       required_resource_keys={"minio_io_manager"},
       ins={
       "result_DFs": AssetIn(key="XML_to_results_df")
       },
       ) 
def result_DFs_to_S3(context: AssetExecutionContext, 
                    result_DFs: List) -> List:
    """
    Step 1. - Get result DFs, individual lines of invoice as rows of DF
    Step 2. - Write DF to S3 bucket as a JSON file, 

    Return - Successful inserts
    """
    logging_on = bool(strtobool(os.getenv('LOGGING_ON')))
    sensitive_logging_on = bool(strtobool(os.getenv('SENSITIVE_LOGGING_ON')))

    # Log step source 
    if logging_on and sensitive_logging_on:
        context.log.info(result_DFs)

    # Insert purchase XML-s rows to Minio S3
    added_log_entries = context.resources.minio_io_manager.insert_df_as_json(result_DFs, 'invoice-jason-landing')

    # Log successful invoice XML to JSON Minio S3 insertions
    if logging_on:
        context.log.info(added_log_entries)

    return added_log_entries


@asset(group_name="extract_purchase_e_invoices", 
       key_prefix=["bronze"],
       required_resource_keys={"pg_io_manager"},
       ins={
       "result_DFs": AssetIn(key="XML_to_results_df")
       },
       ) 
def set_up_dw_xml_landing(context: AssetExecutionContext, 
                          result_DFs: List) -> List:
    """
    Create landing tables, if not present

    Return: if successful return [{XML_file:e-invoice rows in Pandas dataframe}] from XML_to_results_df, otherwise return []  
    """
    logging_on = bool(strtobool(os.getenv('LOGGING_ON')))

    # Set up landing in temp Postgres DW, this step is omitted on DB side 
    # if required tables, procedures and triggers are already present  
    setup_complete = context.resources.pg_io_manager.set_up_dw_xml_landing()
    
    if setup_complete:
        if logging_on:
            context.log.info(result_DFs)
        return result_DFs

    # In case of error in setting up landing in Postgres DW return empty list
    return []


@asset(group_name="extract_purchase_e_invoices", 
       key_prefix=["bronze"],
       required_resource_keys={"pg_io_manager"},
       ins={
       "result_DFs": AssetIn(key="set_up_dw_xml_landing")
       },
       ) 
def load_fact_table_to_dw(context: AssetExecutionContext, 
                          result_DFs: List) -> List:
    """
    Upsert fact table

    Return: [success/failure as bool by XML-file]
    """
    logging_on = bool(strtobool(os.getenv('LOGGING_ON')))
    sensitive_logging_on = bool(strtobool(os.getenv('SENSITIVE_LOGGING_ON')))

    # To pass successful inserts to DW fact table to the next pipeline step
    loaded_xml_dfs = []

    # For logging
    load_successes = []
 
    for i in range(len(result_DFs)):
        
        # Get given XML's DF from List[dict{}] data structure
        dict_i = result_DFs[i]
        df = dict_i[list(dict_i.keys())[0]]

        if logging_on and sensitive_logging_on:
            context.log.info(df)

        # Write given XML's DF to Postgres DW fact table
        load_i = context.resources.pg_io_manager.upload_df_to_dw(df, 
                                                                    # Source DF columns
                                                                    ["SurrogateKey",
                                                                     "Description", 
                                                                     "SellerName",
                                                                     "RegNumber",
                                                                     "InvoiceNumber",
                                                                     "InvoiceDate",
                                                                     "Amount",
                                                                     "ItemUnit",
                                                                     "AddRate",
                                                                     "ItemPrice",
                                                                     "ItemTotal",
                                                                     "SellerProductId",
                                                                     "EAN",
                                                                     "Filename"], 
                                                                    
                                                                    # DB columns
                                                                    ['"SurrogateKey"',
                                                                    '"Description"',
                                                                    '"SellerName"',
                                                                    '"RegNumber"',
                                                                    '"InvoiceNumber"',
                                                                    '"InvoiceDate"',
                                                                    '"Amount"',
                                                                    '"ItemUnit"',
                                                                    '"AddRate"',
                                                                    '"ItemPrice"',
                                                                    '"ItemTotal"',
                                                                    '"SellerProductId"',
                                                                    '"EAN"',
                                                                    '"Filename"'],

                                                                    "invoice_xmls_sc", # Schema
                                                                    "fact_invoice_xml", # Table name
                                                                    "SurrogateKey") # Key col
        # Add write success to log output
        load_successes.append(load_i)

        # If load successful - add XML's {filename: DF} entry to return dict
        if load_i:
            loaded_xml_dfs.append(dict_i)
    
    if logging_on:
        context.log.info(load_successes)

    return loaded_xml_dfs


@asset(group_name="extract_purchase_e_invoices", 
       key_prefix=["bronze"],
       required_resource_keys={"mssql_io_manager"},
       ins={
       "result_DFs": AssetIn(key="load_fact_table_to_dw")
       },
       ) 
def get_dims_to_load(context: AssetExecutionContext, 
                    result_DFs: List) -> Dict:
    """
    Query dimensions related to invoice from D365's MS SQL database based on seller reg number and invoice id.

    Return: {xml_id: dim DF}
    """
    logging_on = bool(strtobool(os.getenv('LOGGING_ON')))
    sensitive_logging_on = bool(strtobool(os.getenv('SENSITIVE_LOGGING_ON')))

    seller_regs = []
    invoice_nrs = []
    xml_ids = []
    res = {}

    # Generate return DF template and XML-ids (filename), seller reg codes and invoice nr (from XML) iterator lists 
    for i in range(len(result_DFs)):
        df = result_DFs[i]
        xml_ids.append(list(df.keys())[0])

        df = df[list(df.keys())[0]]
        seller_regs.append(df['RegNumber'].unique())
        invoice_nrs.append(df['InvoiceNumber'].unique()),

        # Return DF
        res[xml_ids[i]] = {'data': df, 
                           'account': None, 
                           'contract': None, 
                           'cost_center': None, 
                           'department': None, 
                           'main_asset': None,
                           'dim_project': None
                            }

    # Falatten the iterator lists
    seller_regs = [el for li in seller_regs for el in li]
    invoice_nrs = [el for li in invoice_nrs for el in li]
    
    # Sanity check
    assert len(seller_regs) == len(invoice_nrs) == len(xml_ids)
    
    ## Get dimension data from D365 MS SQL DB based on seller reg nr and invoice nr
    for xmlid, regnr, invoicenr in zip(xml_ids, seller_regs, invoice_nrs):
        try:
            dim_res = context.resources.mssql_io_manager.get_fin_dims(regnr, invoicenr)
            dim_query_success = True
        except Exception as e:
            # Exceptions are always logged
            context.log.info(e)
            dim_query_success = False

        # In case of fetching error
        if not dim_query_success:
            res[xmlid]['account'] = "Error"
            res[xmlid]['contract'] = "Error"
            res[xmlid]['cost_center'] = "Error"
            res[xmlid]['department'] = "Error"
            res[xmlid]['main_asset'] = "Error"
            res[xmlid]['dim_project'] = "Error"
        else:
            # Add query output to results DF
            res[xmlid]['account'] = dim_res[["MAINACCOUNTID", "NAME"]]
            res[xmlid]['contract'] = dim_res[["LEPING", "LEPINGVALUE", "ContractDesc"]]
            res[xmlid]['cost_center'] = dim_res[["KULUKOHT", "KULUKOHTVALUE", "CostCenterName"]]
            res[xmlid]['department'] = dim_res[["OSAKOND", "OSAKONDVALUE","DepartmentName"]]
            res[xmlid]['main_asset'] = dim_res[["POHIVARA", "POHIVARAVALUE", "MainAssetName"]]
            res[xmlid]['dim_project'] = dim_res[["PROJEKT", "PROJEKTVALUE", "ProjectName"]]

            # Non-coded no-matches
            res[xmlid]['account'].loc[res[xmlid]['account']["MAINACCOUNTID"] == 0, ["Name"]] = "Missing"
            res[xmlid]['contract'].loc[res[xmlid]['contract']["LEPING"] == 0, ["LEPINGVALUE", "ContractDesc"]] = "Missing"
            res[xmlid]['cost_center'].loc[res[xmlid]['cost_center']["KULUKOHT"] == 0, ["KULUKOHTVALUE", "CostCenterName"]] = "Missing"
            res[xmlid]['department'].loc[res[xmlid]['department']["OSAKOND"] == 0, ["OSAKONDVALUE","DepartmentName"]] = "Missing"
            res[xmlid]['main_asset'].loc[res[xmlid]['main_asset']["POHIVARA"] == 0, ["POHIVARAVALUE", "MainAssetName"]] = "Missing"
            res[xmlid]['dim_project'].loc[res[xmlid]['dim_project']["PROJEKT"] == 0, ["PROJEKTVALUE", "ProjectName"]] = "Missing"

            # Remove dublicates
            for dimtable, col in zip(
                ['account', 'contract', 'cost_center', 'department', 'main_asset', 'dim_project'], 
                ['MAINACCOUNTID','LEPING','KULUKOHT','OSAKOND','POHIVARA', 'PROJEKT']):
                res[xmlid][dimtable] =  res[xmlid][dimtable].drop_duplicates(subset=[col])
            
            # MS SQL to Pandas gives wrong type for IDm update it to int
            for dimtable, col in zip(
                ['contract', 'cost_center', 'department', 'main_asset', 'dim_project'], 
                ['LEPING','KULUKOHT','OSAKOND','POHIVARA', 'PROJEKT']):
                res[xmlid][dimtable][col] = res[xmlid][dimtable][col].astype(int)

    if logging_on and sensitive_logging_on:
        context.log.info(res)

    return res


@asset(group_name="extract_purchase_e_invoices", 
       key_prefix=["bronze"],
       required_resource_keys={"pg_io_manager"},
       ins={
       "dim_DFs": AssetIn(key="get_dims_to_load")
       },
       ) 
def load_dim_tables_to_dw(context: AssetExecutionContext, 
                         dim_DFs: Dict) -> Dict:
    """
    Upsert to dim tables in Postgres DW

    Return: Pass through get_dims_to_load step input  
    """
    logging_on = bool(strtobool(os.getenv('LOGGING_ON')))
    sensitive_logging_on = bool(strtobool(os.getenv('SENSITIVE_LOGGING_ON')))

    xmls_dims = list(dim_DFs.keys())

    # For logging
    load_successes = {}
    load_successes['department'] = []
    load_successes['contract'] = []
    load_successes['main_asset'] = []
    load_successes['dim_project'] = []
    load_successes['cost_center'] = []
    load_successes['account'] = []

    for xml in xmls_dims:

        ## Department dim table upsert to DW
        df_to_load = dim_DFs[xml]['department']
        
        load_i = context.resources.pg_io_manager.upload_df_to_dw(df_to_load, 
                                                                    ["OSAKOND",
                                                                     "OSAKONDVALUE", 
                                                                     "DepartmentName"
                                                                    ], 
                                                                    
                                                                    ['"DepartmentId"',
                                                                    '"DepartmentValue"',
                                                                    '"DepartmentName"'
                                                                    ],

                                                                    "invoice_xmls_sc",
                                                                    "dim_department",
                                                                    "DepartmentId")
        load_successes['department'].append(load_i)

        ## Contract dim table upsert to DW
        df_to_load = dim_DFs[xml]['contract']
        
        load_i = context.resources.pg_io_manager.upload_df_to_dw(df_to_load, 
                                                                    ["LEPING",
                                                                     "LEPINGVALUE", 
                                                                     "ContractDesc"
                                                                    ], 
                                                                    
                                                                    ['"ContractId"',
                                                                    '"ContractValue"',
                                                                    '"ContractName"'
                                                                    ],

                                                                    "invoice_xmls_sc",
                                                                    "dim_contract",
                                                                    "ContractId")
        load_successes['contract'].append(load_i)

        ## Main asset dim table upsert to DW
        df_to_load = dim_DFs[xml]['main_asset']
        
        load_i = context.resources.pg_io_manager.upload_df_to_dw(df_to_load, 
                                                                    ["POHIVARA",
                                                                     "POHIVARAVALUE", 
                                                                     "MainAssetName"
                                                                    ], 
                                                                    
                                                                    ['"MainAssetId"',
                                                                    '"MainAssetValue"',
                                                                    '"MainAssetName"'
                                                                    ],

                                                                    "invoice_xmls_sc",
                                                                    "dim_main_asset",
                                                                    "MainAssetId")
        load_successes['main_asset'].append(load_i)

        ## Project dim table upsert to DW
        df_to_load = dim_DFs[xml]['dim_project']
        
        load_i = context.resources.pg_io_manager.upload_df_to_dw(df_to_load, 
                                                                    ["PROJEKT",
                                                                     "PROJEKTVALUE", 
                                                                     "ProjectName"
                                                                    ], 
                                                                    
                                                                    ['"ProjectId"',
                                                                    '"ProjectValue"',
                                                                    '"ProjectName"'
                                                                    ],

                                                                    "invoice_xmls_sc",
                                                                    "dim_project",
                                                                    "ProjectId")
        load_successes['dim_project'].append(load_i)

        ## Cost center dim table upsert to DW
        df_to_load = dim_DFs[xml]['cost_center']
        
        load_i = context.resources.pg_io_manager.upload_df_to_dw(df_to_load, 
                                                                    ["KULUKOHT",
                                                                     "KULUKOHTVALUE", 
                                                                     "CostCenterName"
                                                                    ], 
                                                                    
                                                                    ['"CostCenterId"',
                                                                    '"CostCenterValue"',
                                                                    '"CostCenterName"'
                                                                    ],

                                                                    "invoice_xmls_sc",
                                                                    "dim_cost_center",
                                                                    "CostCenterId")
        load_successes['cost_center'].append(load_i)

        ## Account dim table upsert to DW
        df_to_load = dim_DFs[xml]['account']
        
        load_i = context.resources.pg_io_manager.upload_df_to_dw(df_to_load, 
                                                                    ["MAINACCOUNTID",
                                                                     "NAME"
                                                                    ], 
                                                                    
                                                                    ['"MainAccountId"',
                                                                    '"AccountName"',
                                                                    ],

                                                                    "invoice_xmls_sc",
                                                                    "dim_account",
                                                                    "MainAccountId")
        load_successes['account'].append(load_i)

    if logging_on and sensitive_logging_on:
        context.log.info(load_successes)
    return dim_DFs


@asset(group_name="extract_purchase_e_invoices", 
       key_prefix=["bronze"],
       required_resource_keys={"pg_io_manager"},
       ins={
       "dim_DFs": AssetIn(key="load_dim_tables_to_dw")
       },
       ) 
def load_dim__fact_rels_to_dw(context: AssetExecutionContext, 
                              dim_DFs: Dict) -> Dict:
    """
    Upsert dim - fact table n-m relation tables

    Return: {dimname: [success/failure as bool by XML-file]} 
    """
    logging_on = bool(strtobool(os.getenv('LOGGING_ON')))

    xmls_dims = list(dim_DFs.keys())

    # For logging
    load_successes = {}
    load_successes['department'] = []
    load_successes['contract'] = []
    load_successes['main_asset'] = []
    load_successes['project'] = []
    load_successes['cost_center'] = []
    load_successes['account'] = []

    for xml in xmls_dims:

        ## Department dim and fact table n-m rels
        for _, row in dim_DFs[xml]['department'].iterrows(): 

            df_to_load = dim_DFs[xml]['data']
            df_to_load['OSAKOND'] = row['OSAKOND']
            df_to_load = df_to_load[["SurrogateKey", "OSAKOND"]]
            ## Generate n-m rel table surrogate key
            df_to_load["nm_id"] = df_to_load["SurrogateKey"].astype(str) + "_" + df_to_load["OSAKOND"].astype(str)

            load_i = context.resources.pg_io_manager.upload_df_to_dw(df_to_load, 
                                                                        ["nm_id",
                                                                        "SurrogateKey",
                                                                        "OSAKOND", 
                                                                        ], 
                                                                        
                                                                        ['"SurrogateKey"',
                                                                        '"FactTableSurrogateKey"',
                                                                        '"DimTableDepartmentId"'
                                                                        ],

                                                                        "invoice_xmls_sc",
                                                                        "fact_invoice_xml_x_dim_department",
                                                                        "SurrogateKey")
            load_successes['department'].append(load_i)

        ## Contract dim and fact table n-m rels
        for _, row in dim_DFs[xml]['contract'].iterrows(): 

            df_to_load = dim_DFs[xml]['data']
            df_to_load['LEPING'] = row['LEPING']
            df_to_load = df_to_load[["SurrogateKey", "LEPING"]]

            ## Generate n-m rel table surrogate key
            df_to_load["nm_id"] = df_to_load["SurrogateKey"].astype(str) + "_" + df_to_load["LEPING"].astype(str)

            load_i = context.resources.pg_io_manager.upload_df_to_dw(df_to_load, 
                                                                        ["nm_id",
                                                                        "SurrogateKey",
                                                                        "LEPING", 
                                                                        ], 
                                                                        
                                                                        ['"SurrogateKey"',
                                                                        '"FactTableSurrogateKey"',
                                                                        '"DimTableContractId"'
                                                                        ],

                                                                        "invoice_xmls_sc",
                                                                        "fact_invoice_xml_x_dim_contract",
                                                                        "SurrogateKey")
            load_successes['contract'].append(load_i)

        ## Main asset dim and fact table n-m rels
        for _, row in dim_DFs[xml]['main_asset'].iterrows(): 

            df_to_load = dim_DFs[xml]['data']
            df_to_load['POHIVARA'] = row['POHIVARA']
            df_to_load = df_to_load[["SurrogateKey", "POHIVARA"]]

            ## Generate n-m rel table surrogate key
            df_to_load["nm_id"] = df_to_load["SurrogateKey"].astype(str) + "_" + df_to_load["POHIVARA"].astype(str)

            load_i = context.resources.pg_io_manager.upload_df_to_dw(df_to_load, 
                                                                        ["nm_id",
                                                                        "SurrogateKey",
                                                                        "POHIVARA", 
                                                                        ], 
                                                                        
                                                                        ['"SurrogateKey"',
                                                                        '"FactTableSurrogateKey"',
                                                                        '"DimTableMainAssetId"'
                                                                        ],

                                                                        "invoice_xmls_sc",
                                                                        "fact_invoice_xml_x_dim_main_asset",
                                                                        "SurrogateKey")
            load_successes['main_asset'].append(load_i)

        ## Project dim and fact table n-m rels
        for _, row in dim_DFs[xml]['dim_project'].iterrows(): 

            df_to_load = dim_DFs[xml]['data']
            df_to_load['PROJEKT'] = row['PROJEKT']
            df_to_load = df_to_load[["SurrogateKey", "PROJEKT"]]

            ## Generate n-m rel table surrogate key
            df_to_load["nm_id"] = df_to_load["SurrogateKey"].astype(str) + "_" + df_to_load["PROJEKT"].astype(str)

            load_i = context.resources.pg_io_manager.upload_df_to_dw(df_to_load, 
                                                                        ["nm_id",
                                                                        "SurrogateKey",
                                                                        "PROJEKT", 
                                                                        ], 
                                                                        
                                                                        ['"SurrogateKey"',
                                                                        '"FactTableSurrogateKey"',
                                                                        '"DimTableProjectId"'
                                                                        ],

                                                                        "invoice_xmls_sc",
                                                                        "fact_invoice_xml_x_dim_project",
                                                                        "SurrogateKey")
            load_successes['project'].append(load_i)

        ## Cost center dim and fact table n-m rels
        for _, row in dim_DFs[xml]['cost_center'].iterrows(): 

            df_to_load = dim_DFs[xml]['data']
            df_to_load['KULUKOHT'] = row['KULUKOHT']
            df_to_load = df_to_load[["SurrogateKey", "KULUKOHT"]]

            ## Generate n-m rel table surrogate key
            df_to_load["nm_id"] = df_to_load["SurrogateKey"].astype(str) + "_" + df_to_load["KULUKOHT"].astype(str)

            load_i = context.resources.pg_io_manager.upload_df_to_dw(df_to_load, 
                                                                        ["nm_id",
                                                                        "SurrogateKey",
                                                                        "KULUKOHT", 
                                                                        ], 
                                                                        
                                                                        ['"SurrogateKey"',
                                                                        '"FactTableSurrogateKey"',
                                                                        '"DimTableCostCenterId"'
                                                                        ],

                                                                        "invoice_xmls_sc",
                                                                        "fact_invoice_xml_x_dim_cost_center",
                                                                        "SurrogateKey")
            load_successes['cost_center'].append(load_i)

        ## Account dim and fact table n-m rels
        for _, row in dim_DFs[xml]['account'].iterrows(): 

            df_to_load = dim_DFs[xml]['data']
            df_to_load['MAINACCOUNTID'] = row['MAINACCOUNTID']
            df_to_load = df_to_load[["SurrogateKey", "MAINACCOUNTID"]]
            
            ## Generate n-m rel table surrogate key
            df_to_load["nm_id"] = df_to_load["SurrogateKey"].astype(str) + "_" + df_to_load["MAINACCOUNTID"].astype(str)

            load_i = context.resources.pg_io_manager.upload_df_to_dw(df_to_load, 
                                                                        ["nm_id",
                                                                        "SurrogateKey",
                                                                        "MAINACCOUNTID", 
                                                                        ], 
                                                                        
                                                                        ['"SurrogateKey"',
                                                                        '"FactTableSurrogateKey"',
                                                                        '"DimTableMainAccountId"'
                                                                        ],

                                                                        "invoice_xmls_sc",
                                                                        "fact_invoice_xml_x_dim_account",
                                                                        "SurrogateKey")
            load_successes['account'].append(load_i)

    if logging_on:
        context.log.info(load_successes)

    return {}