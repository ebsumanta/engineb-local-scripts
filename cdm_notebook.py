# Databricks notebook source
import io
import os
import json
import numpy as np
import pandas as pd
import requests
import zipfile
import shutil
import math
import yaml
import re

from datetime import datetime
from dateutil.parser import parse
from abc import ABC, abstractmethod

# COMMAND ----------

# status_update_url = os.getenv("STATUS_UPDATE_URL", "http://localhost:7072/graphql")
status_update_url= 'https://app-engb-uks-staging-inteng-docker-api.azurewebsites.net/graphql'

local = True
standard_mapping = "true" if local else ""

storage_account_name = ""
container_name = ""
folder_path = ""
ingestion_id = ""
custom_erp = ""

storage_url = ""

INPUT_CONTAINER_PATH = ""
ERROR_CONTAINER_PATH = ""
OUTPUT_CONTAINER_PATH = ""
OUTPUT_DQS_CONTAINER_PATH = ""
ERP_CONFIG_PATH = ""
STANDARD_MAPPING_FILEPATH = ""

MAPPING_FILE_NAME = "mapping_file.json"

dbfs_input_container_path = ""
dbfs_error_container_path = ""
dbfs_output_container_path = ""
dbfs_dqs_output_container_path = ""
dbfs_standard_mapping_path = ""

working_dir = ""
temp_path = ""

# COMMAND ----------

# MAGIC %run /IntegrationEngine01/utilities/storageAccess

# COMMAND ----------

conversion_errors = list()


class CDMConversionException(Exception):
    def init(self, message="Error occured in process of CDM Conversion"):
        super().__init__(self.message)


# COMMAND ----------


def format_cmd_conversion_error(errors):
    fd = {
        "graphql_request": "communicating Node API Service",
        "fetch_groupings": "fetching grouping information",
        "send_job_status": "send job completion status",
        "read_erp_config_file": "reading erp configuration from storage",
        "read_mapping_file": "reading cdm mapping configuration from storage",
        "retrive_unique_fields": "fetching unqiue input erp fields from mapping configuration",
        "retrive_report_names": "fetching all report type with mapping configuration",
        "remove_all_null_rows": "removing null rows from the input file",
        "read_csv_utf": "reading csv file using utf-8 encoding format",
        "get_encoders": "fetching list of encoding formats supported ",
        "dataframe_creator": "reading csv file using different format",
        "read_csv_unicode": "no more in use",
        "read_csv_ignore": "no more in use",
        "fetch_csv_data": "no more in use",
        "create_reprot_file_field_map": "fetching all report type with mapping configuration",
        "apply_column_operation": "finalizing row exclusion rule",
        "apply_row_exlcusion": "filtering data using exclusion-row rule",
        "read_csv_data": "reading input csv file data and aplying row-exclusion rule",
        "check_is_curency_field": "identifying data type as currency e.g. $10,000.00, 10,00,00,000.00",
        "check_is_number": "identifying data type as numeric",
        "check_is_string": "identifying data type as string",
        "check_is_date": " identifying data type as datetime",
        "check_is_boolean": "identifying data type as bolean",
        "clean_up_false_data": "eliminating bad data from the input file based on data type",
        "validate_file_data": "validating input file data (data type check)",
        "detect_total_sub_total_rows": "removing last row if it contains toal or subtotal information",
        "fill_input_data": "fill input data based on file and field configured in CDM Mapping Configuration",
        "calculate_sum": "calculating Sum",
        "calculate_diff": "calculating Diiference",
        "calculate_product": "calculating Product",
        "concat_string": "concating strings",
        "identify_debit_credit": "fetching debit/credit indicator (normal)",
        "range_based_debit_credit": "fetching debit/credit indicator (range based)",
        "adjust_fields_data": "adjusting data after CDM conversion of individual fields",
        "generate_data_type_error_report": "generating error report for Data Type issues",
        "create_report_content": "Converting input file to CDM Format",
        "create_zip_file": "creating zip files of the CDM file(s)",
        "adjust_standard_mapping": "removing unused file configuration from Standard Mapping",
        "insert_erp_file_config": "accounting erp configuration in the mapping file",
        "main": "",
        "remove_unwanted_rows": "picking rows only which are configured in mapping",
        "evaluate_grouping": "Creates Grouping Data Quality Scorecard",
    }
    errors_keys = list()
    for error in errors:
        errors_keys += list(error.keys())
    errors_keys = list(set(errors_keys))
    key_included_errors = list()
    for error in errors:
        for error_key in errors_keys:
            if error_key not in error.keys():
                error[error_key] = None
        error["method_description"] = fd.get(error["failed_function"], "n/a")
        key_included_errors.append(error)
    error_df = pd.DataFrame(key_included_errors)
    static_columns = [
        "method_description",
        "failed_function",
        "called_method",
        "message",
    ]
    property_columns = list(set(list(error_df.columns)) - set(static_columns))
    property_columns.sort()
    error_df = error_df[static_columns + property_columns]
    return error_df.iloc[::-1]


# COMMAND ----------


def clean_outputs():
    cleanup_path = [
        dbfs_error_container_path,
        dbfs_output_container_path,
        dbfs_dqs_output_container_path,
        dbfs_gdqs_output_container_path,
        temp_path,
    ]
    for folder_path in cleanup_path:
        for file_object in os.listdir(folder_path):
            file_object_path = os.path.join(folder_path, file_object)
            if os.path.isfile(file_object_path) or os.path.islink(file_object_path):
                os.unlink(file_object_path)
            else:
                shutil.rmtree(file_object_path)


# COMMAND ----------

if not local:
    dbutils.widgets.text("storage_account_name", "", "")
    storage_account_name = dbutils.widgets.get("storage_account_name")

    dbutils.widgets.text("containerName", "", "")
    container_name = dbutils.widgets.get("containerName")

    dbutils.widgets.text("folderPath", "", "")
    folder_path = dbutils.widgets.get("folderPath")

    dbutils.widgets.text("ingestion_id", "", "")
    ingestion_id = dbutils.widgets.get("ingestion_id")

    dbutils.widgets.text("standard_mapping", "", "")
    standard_mapping = dbutils.widgets.get("standard_mapping")

    dbutils.widgets.text("custom_erp", "", "")
    custom_erp = dbutils.widgets.get("custom_erp")

    storage_url = (
        f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net"
    )
    INPUT_CONTAINER_PATH = f"{storage_url}/{folder_path}/input/"
    ERROR_CONTAINER_PATH = f"{storage_url}/{folder_path}/error/"
    OUTPUT_CONTAINER_PATH = f"{storage_url}/{folder_path}/share/zip/"
    OUTPUT_DQS_CONTAINER_PATH = f"{storage_url}/{folder_path}/share/dqs/"
    OUTPUT_GDQS_CONTAINER_PATH = f"{storage_url}/{folder_path}/share/gdqs/"
    ERP_CONFIG_PATH = (
        f"{storage_url}/mandatory-fields-erp/"
        if custom_erp == "true"
        else f"abfss://mandatory-fields-erp@{storage_account_name}.dfs.core.windows.net/"
    )

    standard_mapping_root, standard_mapping_file = (
        standard_mapping.split("/")
        if len(str(standard_mapping).split("/")) > 1
        else (None, None)
    )
    STANDARD_MAPPING_FILEPATH = (
        f"abfss://{standard_mapping_root}@{storage_account_name}.dfs.core.windows.net/{standard_mapping_file}"
        if standard_mapping
        else None
    )

    dbfs_input_container_path = f"/FileStore/custom-mapping-file/{folder_path}/input/"
    dbfs_error_container_path = f"/FileStore/custom-mapping-file/{folder_path}/error/"
    dbfs_output_container_path = (
        f"/FileStore/custom-mapping-file/{folder_path}/share/zip/"
    )
    dbfs_dqs_output_container_path = (
        f"/FileStore/custom-mapping-file/{folder_path}/share/dqs/"
    )
    dbfs_gdqs_output_container_path = (
        f"/FileStore/custom-mapping-file/{folder_path}/share/gdqs/"
    )
    dbfs_standard_mapping_path = (
        f"/FileStore/custom-mapping-file/{folder_path}/input/{MAPPING_FILE_NAME}"
    )

    inputfile = INPUT_CONTAINER_PATH + MAPPING_FILE_NAME
    working_dir = f"/FileStore/custom-mapping-file/{folder_path}"
    temp_path = f"/FileStore/custom-mapping-file/{ingestion_id}/temp_output/"

    dbutils.fs.mkdirs(dbfs_input_container_path)
    dbutils.fs.mkdirs(dbfs_error_container_path)
    dbutils.fs.mkdirs(dbfs_output_container_path)
    dbutils.fs.mkdirs(dbfs_dqs_output_container_path)
    dbutils.fs.mkdirs(dbfs_gdqs_output_container_path)
    dbutils.fs.mkdirs(temp_path)

    dbutils.fs.cp(INPUT_CONTAINER_PATH, dbfs_input_container_path, recurse=True)
    if STANDARD_MAPPING_FILEPATH:
        custom_mapping = [
            file_info.name
            for file_info in dbutils.fs.ls(dbfs_input_container_path)
            if file_info.name == MAPPING_FILE_NAME
        ]
        if len(custom_mapping) == 0:
            print("Custom Mapping not found, fetching Standard Mapping")
            print(dbfs_standard_mapping_path)
            dbutils.fs.cp(
                STANDARD_MAPPING_FILEPATH, dbfs_standard_mapping_path, recurse=True
            )
        else:
            print("Custom Mapping is present, ignoring Standard Mapping")
            standard_mapping = "null"
else:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))).replace(
        "/notebooks/IntegrationEngine01", ""
    )
    BASE_DIR = '.'
    ingestion_id = "clgwcb7o22419601plhanzkpeq"
    MAPPING_FILE_NAME = "mapping_file.json"
    dbfs_input_container_path = f"{BASE_DIR}/container/input/"
    dbfs_error_container_path = f"{BASE_DIR}/container/error/"
    dbfs_output_container_path = f"{BASE_DIR}/container/share/zip/"
    dbfs_dqs_output_container_path = f"{BASE_DIR}/container/share/dqs/"
    dbfs_gdqs_output_container_path = f"{BASE_DIR}/container/share/gdqs/"
    dbfs_standard_mapping_path = f"{BASE_DIR}/container/input/{MAPPING_FILE_NAME}"
    temp_path = f"{BASE_DIR}/container/temp_output/"
    #clean_outputs()


# COMMAND ----------


def copy_data():
    try:
        dbutils.fs.cp(dbfs_output_container_path, OUTPUT_CONTAINER_PATH, recurse=True)
        dbutils.fs.cp(dbfs_error_container_path, ERROR_CONTAINER_PATH, recurse=True)
        dbutils.fs.cp(
            dbfs_dqs_output_container_path, OUTPUT_DQS_CONTAINER_PATH, recurse=True
        )
        dbutils.fs.cp(dbfs_input_container_path, INPUT_CONTAINER_PATH, recurse=True)
        dbutils.fs.cp(dbfs_gdqs_output_container_path, OUTPUT_GDQS_CONTAINER_PATH, recurse=True)
        print("Data copied to Storage")
    except Exception as e:
        error_data = {
            "failed_function": "copy_data",
            "called_method": None,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


# COMMAND ----------


def graphql_request(req_body):
    try:
        response = requests.post(
            status_update_url,
            data=json.dumps(req_body),
            headers={"Content-Type": "application/json"},
        )
        if response.status_code == 200:
            return response.json()
        else:
            return None
    except Exception as e:
        error_data = {
            "failed_function": "graphql_request",
            "called_method": None,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


def fetch_groupings():
    try:
        req_body = {
            "query": "query NominalCodeMappings($ingestionId: String!) { nominalCodeMappings(ingestionId: $ingestionId) {metadata {groupingName version lastUpdatedBy} grouping {nominalCode description account accountSubtype fsCaption accountName}}}",
            "variables": {"ingestionId": ingestion_id},
        }
        data = graphql_request(req_body)
        if data:
            return None if "errors" in data.keys() else data
        return None
    except CDMConversionException as cdm_e:
        error_info = json.loads(str(cdm_e))
        conversion_errors.append(error_info)
        error_data = {
            "failed_function": "fetch_groupings",
            "called_method": error_info["failed_function"],
            "ingestion_id": ingestion_id,
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)
    except Exception as e:
        error_data = {
            "failed_function": "graphql_request",
            "called_method": None,
            "ingestion_id": ingestion_id,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


def send_job_status(status):
    try:
        request_body = {
            "query": "mutation update($dataIngestionId: String!, $status: DataIngestionStatus!){updateDataIngestionStatus(dataIngestionId: $dataIngestionId, status: $status){id}}",
            "variables": {"dataIngestionId": ingestion_id, "status": status},
        }
        response = requests.post(
            status_update_url,
            data=json.dumps(request_body),
            headers={"Content-Type": "application/json"},
        )
        if response.status_code == 200:
            print("Job Status Updated")
        else:
            print("Could not update Job Status")
    except CDMConversionException as cdm_e:
        error_info = json.loads(str(cdm_e))
        conversion_errors.append(error_info)
        error_data = {
            "failed_function": "send_job_status",
            "called_method": error_info["failed_function"],
            "ingestion_id": ingestion_id,
            "status": status,
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)
    except Exception as e:
        error_data = {
            "failed_function": "send_job_status",
            "called_method": None,
            "ingestion_id": ingestion_id,
            "status": status,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


# COMMAND ----------


def adjust_filepath(filepath):
    try:
        if str(filepath).startswith("/dbfs"):
            return filepath
        return filepath if local else f"/dbfs{str(filepath)}"
    except Exception as e:
        error_data = {
            "failed_function": "send_job_status",
            "called_method": None,
            "file_path": filepath,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


# COMMAND ----------


def read_erp_config_file(filepath):
    try:
        with open(adjust_filepath(filepath), "r") as f:
            config_dict = yaml.safe_load(f)
        return config_dict
    except Exception as e:
        error_data = {
            "failed_function": "read_erp_config_file",
            "called_method": None,
            "file_path": filepath,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


# COMMAND ----------


def read_mapping_file():
    """Reads the mapping file and returns the mapping"""
    try:
        with open(
            adjust_filepath(f"{dbfs_input_container_path}{MAPPING_FILE_NAME}")
        ) as f:
            mapping = json.load(f)
        return mapping
    except Exception as e:
        error_data = {
            "failed_function": "read_mapping_file",
            "called_method": None,
            "file_path": f"{dbfs_input_container_path}{MAPPING_FILE_NAME}",
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


# COMMAND ----------


def retrive_unique_fields(fields):
    try:
        unique_fields = list()
        for _, data in enumerate(fields):
            field_list = list(
                filter(lambda x: x["field_name"] == data["field_name"], unique_fields)
            )
            if len(field_list) == 0:
                unique_fields.append(data)

        return unique_fields
    except Exception as e:
        error_data = {
            "failed_function": "retrive_unique_fields",
            "called_method": None,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


# COMMAND ----------


def retrive_report_names(mapping):
    try:
        report_names = []
        for cdm_map in mapping:
            report_names.append(cdm_map["report_type"])
        return list(set(report_names))
    except Exception as e:
        error_data = {
            "failed_function": "retrive_report_names",
            "called_method": None,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


# COMMAND ----------


def remove_all_null_rows(df):
    """
    This function removes all the rows with null values
    """
    try:
        null_row_indexes = df.isnull().all(1)
        df = df[~null_row_indexes].reset_index(drop=True)
        return df
    except Exception as e:
        error_data = {
            "failed_function": "remove_all_null_rows",
            "called_method": None,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


# COMMAND ----------

# Test file encoding - by Sumanta (Test Mode)
def get_encoders():
    return [
        "utf-8",
        "iso-8859-1",
        "windows-1251",
        "windows-1252",
        "shiftjis",
        "gb2312-1980",
        "gb2312-80",
        "eucjp",
        "iso-8859-9",
    ]


# changes for Data encoding and formation  - by Sumanta (Test Mode)
def dataframe_creator(content, skiprows=0, nrows=0):
    encodingList = get_encoders()
    df = None
    en = ""

    for item in encodingList:
        try:
            if str(type(content)) == "<class '_io.BytesIO'>":
                en = item
                if nrows:
                    df = pd.read_csv(
                        content,
                        encoding=item,
                        engine="python",
                        skiprows=skiprows,
                        nrows=nrows,
                    )
                else:
                    df = pd.read_csv(
                        content, encoding=item, engine="python", skiprows=skiprows
                    )
                break
            elif str(type(content)) == "<class 'str'>":
                en = item
                x = content
                if nrows:
                    df = pd.read_csv(
                        x,
                        encoding=item,
                        engine="python",
                        skiprows=skiprows,
                        nrows=nrows,
                    )
                else:
                    df = pd.read_csv(
                        x, encoding=item, engine="python", skiprows=skiprows
                    )
                break
            else:
                en = item
                x = io.StringIO(content.decode(item))
                if nrows:
                    df = pd.read_csv(
                        x,
                        encoding=item,
                        engine="python",
                        skiprows=skiprows,
                        nrows=nrows,
                    )
                else:
                    df = pd.read_csv(
                        x, encoding=item, engine="python", skiprows=skiprows
                    )
                break
        except Exception as ex:
            continue
    if isinstance(df, pd.DataFrame):
        return df
    else:
        error_data = {
            "failed_function": "dataframe_creator",
            "called_method": None,
            "message": "Unable to read CSV File withany of the supported encoding formats",
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


def fetch_csv_data(file_path, encoding="utf-8", start_row=0):
    try:
        data = None
        data = dataframe_creator(adjust_filepath(f"{file_path}"), start_row)
        data = (
            data.replace(" ", np.nan).replace("BLANK", np.nan).replace("NULL", np.nan)
        )
        data = remove_all_null_rows(data)
        data.columns = [col.replace("Unnamed: ", "NO_NAME") for col in data.columns]
        return data
    except CDMConversionException as cdm_e:
        error_info = json.loads(str(cdm_e))
        conversion_errors.append(error_info)
        error_data = {
            "failed_function": "fetch_csv_data",
            "called_method": error_info["failed_function"],
            "file_path": file_path,
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)
    except Exception as e:
        error_data = {
            "failed_function": "fetch_csv_data",
            "called_method": None,
            "file_path": file_path,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


# COMMAND ----------


def create_reprot_file_field_map(cdm_fields):
    """
    This function creates a dictionary of the fields in the report file
    """
    try:
        reports = retrive_report_names(cdm_fields)
        mapping_struct = list()
        for report in reports:
            report_info = dict()
            report_cdm_fields = list(
                filter(lambda x: x["report_type"] == report, cdm_fields)
            )
            report_info["report_name"] = report
            report_info["mapping"] = report_cdm_fields

            files = list()
            for cdm_field in report_cdm_fields:
                for erp_field in cdm_field["erp_fields"]:
                    files.append(
                        {
                            "file_name": erp_field["file_name"],
                            "field_name": erp_field["field_name"],
                            "data_type": cdm_field["data_type"],
                        }
                    )
            file_mapping = list(set(list(map(lambda x: x["file_name"], files))))
            file_info = dict()
            for file in file_mapping:
                file_fields = list(filter(lambda x: x["file_name"] == file, files))
                file_fields = list(
                    map(
                        lambda x: {
                            "field_name": x["field_name"],
                            "data_type": x["data_type"],
                        },
                        file_fields,
                    )
                )
                file_info[file] = {
                    "data": None,
                    "fields": retrive_unique_fields(file_fields),
                }
            report_info["file_info"] = file_info
            mapping_struct.append(report_info)
        return mapping_struct
    except CDMConversionException as cdm_e:
        error_info = json.loads(str(cdm_e))
        conversion_errors.append(error_info)
        error_data = {
            "func": "create_reprot_file_field_map",
            "called_method": error_info["failed_function"],
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)
    except Exception as e:
        error_data = {
            "func": "create_reprot_file_field_map",
            "called_method": None,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


# COMMAND ----------


def apply_column_operation(row, operator, total_columns):
    try:
        final_value = row[0]
        for i in range(1, total_columns):
            if operator == "or":
                final_value = final_value or row[i]
            elif operator == "and":
                final_value = final_value and row[i]
            else:
                final_value = False
        return final_value
    except Exception as e:
        error_data = {
            "failed_function": "apply_column_operation",
            "called_method": None,
            "row_info": row,
            "operator": operator,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


def apply_row_exlcusion(data, exclusion):
    try:
        exclusion_values = list()
        rules = exclusion["rules"]
        operation = exclusion["aggr"] if len(rules) > 1 else "or"
        for rule in rules:
            column_name = list(data.columns)[rule["column"] - 1]
            column_to_apply = data[[column_name]]
            contains_null = rule["contains"] == "NULL"
            contains_not_null = rule["contains"] == "NOT_NULL"
            if contains_null:
                values = column_to_apply.isnull().all(axis=1)
            elif contains_not_null:
                values = column_to_apply.notnull().all(axis=1)
            else:
                values = column_to_apply.apply(
                    lambda x: rule["contains"].lower() in str(x[column_name]).lower(),
                    axis=1,
                )
            exclusion_values.append(values)
        exclusions = pd.concat(exclusion_values, axis=1)
        rows_to_exclude = exclusions.apply(
            lambda x: apply_column_operation(x, operation, exclusions.shape[1])
            if exclusions.shape[1] > 1
            else x[0],
            axis=1,
        )
        return data[~rows_to_exclude].reset_index(drop=True)
    except CDMConversionException as cdm_e:
        error_info = json.loads(str(cdm_e))
        conversion_errors.append(error_info)
        error_data = {
            "failed_function": "apply_row_exlcusion",
            "called_method": error_info["failed_function"],
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)
    except Exception as e:
        error_data = {
            "failed_function": "apply_row_exlcusion",
            "called_method": None,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


# COMMAND ----------


def read_csv_data(file_name, file_template, encoding_format):
    """
    This function reads the csv data and returns the dataframe
    """
    try:
        df = fetch_csv_data(
            f"{dbfs_input_container_path}{file_name}",
            encoding=encoding_format,
            start_row=file_template["start_row"] - 1,
        )
        if isinstance(df, pd.DataFrame) and file_template.get("exclusion", None):
            df = apply_row_exlcusion(df, file_template.get("exclusion", None))
        return df
    except CDMConversionException as cdm_e:
        error_info = json.loads(str(cdm_e))
        conversion_errors.append(error_info)
        error_data = {
            "failed_function": "read_csv_data",
            "called_method": error_info["failed_function"],
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)
    except Exception as e:
        error_data = {
            "failed_function": "read_csv_data",
            "called_method": None,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


# COMMAND ----------


def check_is_curency_field(value):
    """
    This function checks if the field is a currency field
    """
    number = ""
    valid = False
    str_value = str(value)
    braces = f"{str_value[0]}{str_value[-1:]}"
    init_val = 1
    
    # regular expression to check if it contails any alpha char or not
    if bool(re.match('^[a-zA-Z0-9]+$', value)):
        return (None, valid)
    
    if braces == "()" or ("(" in str_value and ")" in str_value):
        init_val = -1
    for index in range(len(str(value))):
        if str(value)[index] in [
            "0",
            "1",
            "2",
            "3",
            "4",
            "5",
            "6",
            "7",
            "8",
            "9",
            ".",
            "-",
        ]:
            number += str(value)[index]
    try:
        number = float(number) * init_val
        valid = True
    except Exception as e:
        valid = False

    return (number if number != "" else None, valid)


# COMMAND ----------


def check_is_number(value):
    """
    This function checks if the value is a number
    """
    valid = False
    converted_value = None
    try:
        if isinstance(value, str):
            if value.isnumeric():
                valid = True
                converted_value = float(value)
            elif check_is_curency_field(value)[1]:
                valid = True
                converted_value = check_is_curency_field(value)[0]
            else:
                valid = False
        elif isinstance(value, (int, float)):
            valid = True
            converted_value = float(value)
        else:
            converted_value = float(value)
            valid = True
    except:
        valid = False

    return (converted_value, valid)


# COMMAND ----------


def check_is_string(value):
    valid = False
    converted_value = None
    try:
        if value == np.nan:
            valid = True
            converted_value = ""
        else:
            valid = True
            converted_value = str(value) if str(value) != "nan" else ""
    except Exception as e:
        valid = False
    return (converted_value, valid)


# COMMAND ----------


def check_is_date(value, fuzzy=False):
    valid = False
    if value == "nan":
        return ("", True)
    try:
        parse(value, fuzzy=fuzzy)
        valid = True
    except ValueError as e:
        valid = False
    return (value, valid)


# COMMAND ----------


def check_is_boolean(value):
    converted_value = None
    valid = False
    if isinstance(value, bool):
        valid = True
        converted_value = value
    else:
        if isinstance(value, str):
            if value.lower() == "true" or value.lower() == "false":
                valid = True
                converted_value = bool(value)
            else:
                valid = False

    return (converted_value, valid)


# COMMAND ----------


def clean_up_false_data(data, fields):
    try:
        field_names = list(map(lambda x: x["name"], fields))
        validity_colmns = list(set(list(data.columns)) - set(field_names))
        check_df = data[validity_colmns]

        bad_data = check_df.isin([False])
        bad_data = bad_data.reset_index(drop=False)
        bad_data.columns = ["index"] + [
            name[: -1 * len("_type_valid")]
            for name in bad_data.columns
            if name != "index"
        ]
        bad_row_dict = dict()
        for col in list(bad_data.columns):
            if col not in ["index", ""]:
                col_data = bad_data[bad_data[col] == True][["index", col]][
                    "index"
                ].values
                if len(col_data) > 0:
                    col_data_type = list(filter(lambda x: x["name"] == col, fields))[0][
                        "data_type"
                    ]
                    for idx in col_data:

                        if idx not in bad_row_dict:
                            bad_row_dict[idx] = f"{col} ({col_data_type})"
                        else:
                            bad_row_dict[idx] += "|" + f"{col} ({col_data_type})"

        check_df = check_df[check_df == True]
        valid_df = check_df.dropna()

        row_eliminated = list(set(data.index) - set(valid_df.index))
        row_eliminated.sort()
        valid_data = data[field_names]
        valid_data = valid_data.drop(row_eliminated).reset_index(drop=True)
        return (
            valid_data,
            bad_row_dict,
        )
    except Exception as e:
        error_data = {
            "failed_function": "clean_up_false_data",
            "called_method": None,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


# COMMAND ----------


def validate_file_data(file_data, file_fields):
    try:
        type_check_df = pd.DataFrame()
        fields = list()
        for field in file_fields:
            field_name = field["field_name"]
            column_data = list(file_data[field_name].values)
            if field["data_type"] in ["decimal", "number", "integer"]:
                type_info = [check_is_number(x) for x in column_data]
            elif field["data_type"] in ["string"]:
                type_info = [check_is_string(x) for x in column_data]
            elif field["data_type"] in ["date", "datetime"]:
                type_info = [check_is_date(str(x)) for x in column_data]
            elif field["data_type"] in ["boolean"]:
                type_info = [check_is_boolean(x) for x in column_data]
            else:
                type_info = [(x, False) for x in column_data]
            fields.append({"name": field_name, "data_type": field["data_type"]})
            type_check_df[field_name] = list(map(lambda x: x[0], type_info))
            type_check_df[f"{field_name}_type_valid"] = list(
                map(lambda x: x[1], type_info)
            )

        _, row_eliminated = clean_up_false_data(type_check_df, fields)

        return (type_check_df, row_eliminated)
    except CDMConversionException as cdm_e:
        error_info = json.loads(str(cdm_e))
        conversion_errors.append(error_info)
        error_data = {
            "failed_function": "validate_file_data",
            "called_method": error_info["failed_function"],
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)
    except Exception as e:
        error_data = {
            "failed_function": "validate_file_data",
            "called_method": None,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


# COMMAND ----------


def detect_total_sub_total_rows(data):
    try:
        last_row_first_column = str(data[[list(data.columns)[0]]].values[-1]).lower()
        last_row_second_column = str(data[[list(data.columns)[1]]].values[-1]).lower()
        if "total" in last_row_first_column or "total" in last_row_second_column:
            data = data[0 : data.shape[0] - 1]
        return data
    except Exception as e:
        error_data = {
            "failed_function": "detect_total_sub_total_rows",
            "called_method": None,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


# COMMAND ----------


def remove_unwanted_rows(data: pd.DataFrame, columns: list) -> pd.DataFrame:
    try:
        df = data[columns]
        df = remove_all_null_rows(df)
        return df
    except Exception as e:
        error_data = {
            "failed_function": "remove_unwanted_rows",
            "called_method": None,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


def fill_input_data(mapping_struct):
    """
    This function fills the input data for the report
    """
    try:
        info_with_data = list()
        for report in mapping_struct:
            info = {
                "report_name": report["report_name"],
                "mapping": report["mapping"],
                "file_info": {},
            }
            for file_name, file_data in report["file_info"].items():
                file_info_with_data = dict()
                data = read_csv_data(
                    file_name, file_data["template"], file_data["encoding_format"]
                )
                columns_required = list(
                    map(
                        lambda field_info: field_info["field_name"], file_data["fields"]
                    )
                )
                #                 data = detect_total_sub_total_rows(data)
                data = remove_unwanted_rows(data, columns_required)
                valid_data, bad_rows = validate_file_data(data, file_data["fields"])
                file_info_with_data["fields"] = file_data["fields"]
                file_info_with_data["data"] = valid_data
                file_info_with_data["bad_rows"] = bad_rows
                info["file_info"][file_name] = file_info_with_data
            info_with_data.append(info)

        return info_with_data
    except CDMConversionException as cdm_e:
        error_info = json.loads(str(cdm_e))
        conversion_errors.append(error_info)
        error_data = {
            "failed_function": "fill_input_data",
            "called_method": error_info["failed_function"],
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)
    except Exception as e:
        error_data = {
            "failed_function": "fill_input_data",
            "called_method": None,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


# COMMAND ----------


def calculate_sum(numbers):
    try:
        result = 0
        for _, num in enumerate(numbers):
            num_value, is_valid = (
                check_is_number(num)
                if num != ""
                else (
                    0,
                    True,
                )
            )
            if is_valid:
                result = result + num_value
            else:
                error_data = {
                    "failed_function": "calculate_sum",
                    "called_method": None,
                    "values": num,
                    "message": "Could not use Addition operation",
                }
                error_info = json.dumps(error_data)
                raise CDMConversionException(error_info)
        return result
    except Exception as e:
        error_data = {
            "failed_function": "calculate_sum",
            "called_method": None,
            "values": ", ".join(numbers),
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


def calculate_diff(numbers):
    try:

        result = 0
        for idx, num in enumerate(numbers):
            num_value, is_valid = (
                check_is_number(num)
                if num != ""
                else (
                    0,
                    True,
                )
            )
            if is_valid:
                if idx == 0:
                    result = num_value
                else:
                    result = result - num_value
            else:
                error_data = {
                    "failed_function": "calculate_diff",
                    "called_method": None,
                    "values": num,
                    "message": "Could not use subtraction operation",
                }
                error_info = json.dumps(error_data)
                raise CDMConversionException(error_info)
        return result
    except Exception as e:
        error_data = {
            "failed_function": "calculate_diff",
            "called_method": None,
            "values": ", ".join(numbers),
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


def calculate_product(numbers):
    try:
        result = 1
        for _, num in enumerate(numbers):
            num_value, is_valid = (
                check_is_number(num)
                if num != ""
                else (
                    0,
                    True,
                )
            )
            if is_valid:
                result = result * num_value
            else:
                error_data = {
                    "failed_function": "calculate_product",
                    "called_method": None,
                    "values": num,
                    "message": "Could not calculate product",
                }
                error_info = json.dumps(error_data)
                raise CDMConversionException(error_info)
        return result
    except Exception as e:
        error_data = {
            "failed_function": "calculate_product",
            "called_method": None,
            "values": ", ".join(numbers),
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


def concat_string(strings):
    try:
        result = strings[0]
        for idx, st in enumerate(strings):
            if idx > 0:
                result = f"{result}-{st}"
        return result
    except Exception as e:
        error_data = {
            "failed_function": "concat_string",
            "called_method": None,
            "values": strings,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


def identify_debit_credit(debit_value, credit_value):
    try:
        debit = (
            check_is_number(debit_value)[0] if check_is_number(debit_value)[1] else None
        )
        credit = (
            check_is_number(credit_value)[0]
            if check_is_number(credit_value)[1]
            else None
        )
        if debit == None and credit != None:
            return "C"
        elif debit != None and credit == None:
            return "D"
        elif debit != None and credit != None:
            return "D" if debit > credit else "C"
        else:
            return None
    except Exception as e:
        error_data = {
            "failed_function": "identify_debit_credit",
            "called_method": None,
            "debit_amount": debit_value,
            "credit_amount": credit_value,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


def range_based_debit_credit(account_no, amount, range_details, debit_ind='D', credit_ind='C'):
    result = {
            'status': True,
            'output': None,
            'error_message': None,
            'error_value': None
        }
    try:
        reference_value, ref_valid = check_is_number(account_no)
        amount_value, amt_valid = check_is_number(amount)

        if ref_valid and amt_valid:
            for rng in range_details:
                if reference_value >= rng["start"] and reference_value <= rng["end"]:
                    if rng["negatives"] == "Debit":
                        result['output'] = debit_ind if amount_value < 0 else credit_ind
                    elif rng["negatives"] == "Credit":
                        result['output'] = credit_ind if amount_value < 0 else debit_ind

            if not result['output']:
                result['status'] = False
                result['error_message'] = "Could not determine DC Indicator. Reference not in range"
                result['error_value'] = f"A/C No - {account_no}, Amount - {amount}"
        elif not ref_valid:
            result['status'] = False
            result['error_message'] = "A/C No should be numeric"
            result['error_value'] = account_no
        elif not amt_valid:
            result['status'] = False
            result['error_message'] = "Amount value should be numeric"
            result['error_value'] = amount
    except Exception as e:
        result['status'] = False
        result['error_message'] = str(e)
        result['error_value'] = f"A/C No: {account_no} Amount: {amount}"
    return result


# COMMAND ----------

START_CH = "<"
END_CH = ">"
SEPERATOR = "_._"


def extract_field_from_exp(start_ch, end_ch, expression, to_int=False):
    fields_used = list()
    for index, ch in enumerate(expression):
        field_id = ""
        if ch == start_ch:
            start = index
            continue
        if ch == end_ch:
            field_id = expression[start + 1 : index]
            if to_int:
                if field_id.isdigit():
                    try:
                        fields_used.append(int(field_id))
                    except:
                        fields_used.append(None)
                else:
                    fields_used.append(None)
            else:
                fields_used.append(field_id)
            continue

    return list(set(fields_used))


def calculate_expression(mapping_object, file_data):
    exp = mapping_object["expression"].replace(" ", "")
    try:
        assert exp != ''
    except:
        return {
            "status": False,
            "message": "No Expression to evaluate",
            "failed_exp": None,
            "bad_row": None,
            "data": [np.nan],
        }

    exp = exp.replace("^", "**")
    fields_used = extract_field_from_exp(
        start_ch=START_CH, end_ch=END_CH, expression=exp, to_int=True
    )
    erp_id_mapping = dict()

    files_used = list()
    for id in fields_used:
        field_info = list(
            filter(lambda x: x["order"] == id, mapping_object["erp_fields"])
        )[0]
        field_string = f"{field_info['file_name']}{SEPERATOR}{field_info['field_name']}"
        erp_id_mapping[f"<{id}>"] = f"<{field_string}>"
        files_used.append(field_info["file_name"])

    files_used = list(set(files_used))

    try:
        assert len(files_used) == 1
    except:
        return {
            "status": False,
            "message": "Multiple files used for the conversion.",
            "failed_exp": None,
            "bad_row": None,
            "data": [np.nan],
        }

    converted_exp = exp
    for key, value in erp_id_mapping.items():
        converted_exp = converted_exp.replace(key, value)

    erp_fields = extract_field_from_exp(
        start_ch=START_CH, end_ch=END_CH, expression=converted_exp, to_int=False
    )

    output_value = list()

    for idx, row in file_data[files_used[0]]["data"].iterrows():
        data_type_issue = False

        fields = list(map(lambda operrand: operrand.split(SEPERATOR)[1], erp_fields))
        exp_to_solve = converted_exp
        for field in fields:
            num_value, status = check_is_number(str(row[field]))
            if not status:
                if row[field] == "" or str(row[field]) == "nan":
                    exp_to_solve = exp_to_solve.replace(
                        f"<{files_used[0]}{SEPERATOR}{field}>", "0"
                    )
                else:
                    data_type_issue = True
                    exp_to_solve = exp_to_solve.replace(
                        f"<{files_used[0]}{SEPERATOR}{field}>", str(row[field])
                    )
            else:
                exp_to_solve = exp_to_solve.replace(
                    f"<{files_used[0]}{SEPERATOR}{field}>", str(num_value)
                )
        if data_type_issue:
            return {
                "status": False,
                "message": "Expresion contains non numeric operand(s)",
                "failed_exp": exp_to_solve,
                "bad_row": idx + 1,
                "data": [np.nan],
            }
        try:
            cal = eval(exp_to_solve)
            result = round(cal, 4)
            output_value.append(result)
        except Exception as e:
            return {
                "status": False,
                "message": f"Failed to solve the expression. {str(e)}",
                "failed_exp": exp_to_solve,
                "bad_row": idx + 1,
                "data": [np.nan],
            }
    return {
        "status": True,
        "message": "",
        "failed_exp": None,
        "bad_row": None,
        "data": output_value,
    }


# COMMAND ----------


def adjust_fields_data(fields):
    try:
        max_length = max([len(data) for field, data in fields.items()])
        adjusted_fields = dict()
        for field, data in fields.items():
            if len(data) == 1 and str(data[0]) != "nan":
                adjusted_fields[field] = data
            elif len(data) < max_length and len(data) > 1:
                number_of_nans = max_length - len(data)
                adjusted_fields[field] = (
                    data + [np.nan] * number_of_nans if number_of_nans > 0 else data
                )
            elif len(data) == 1:
                adjusted_fields[field] = [np.nan for _ in range(max_length)]
            else:
                adjusted_fields[field] = data
        return adjusted_fields
    except Exception as e:
        error_data = {
            "failed_function": "adjust_fields_data",
            "called_method": None,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


# COMMAND ----------


def generate_data_type_error_report(reports):
    try:
        type_error = list()
        for report in reports:
            report_type = report["report_name"]
            process_report = True
            for file_name, details in report["file_info"].items():
                invalid_rows = list(
                    map(lambda row: row + 1, details["bad_rows"].keys())
                )
                if len(invalid_rows) > 0:
                    print(
                        f"{file_name} for {report_type} has invalid {len(invalid_rows)} rows, the output will not be generated"
                    )
                    process_report = False and process_report
                    for row_num in invalid_rows:
                        type_error.append(
                            {
                                "report_name": report_type,
                                "cdm_field": None,
                                "file_name": file_name,
                                "field_name": details["bad_rows"][row_num - 1],
                                "data_row": row_num,
                                "error": f"Invalid Data Type",
                            }
                        )
            report["process"] = process_report

        processable_reports = list(filter(lambda report: report["process"], mappings))
        return processable_reports, type_error
    except Exception as e:
        error_data = {
            "failed_function": "generate_data_type_error_report",
            "called_method": None,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


# COMMAND ----------


def create_report_content(mappings):
    """
    This function creates the intermediate files for the report
    """
    try:
        intermediate_files = list()
        errors = list()
        for mapping in mappings:
            report_name = mapping["report_name"]
            input_files = mapping["file_info"]
            cdm_mapping = mapping["mapping"]
            report_data = {
                "report_type": report_name,
            }
            fields = dict()
            cdm_order_fields = [cdm_map["cdm_field"] for cdm_map in cdm_mapping]
            for cdm_map in cdm_mapping:
                cdm_field_name = cdm_map["cdm_field"]
                if cdm_map["map_type"] == "dummy":
                    if cdm_field_name not in fields.keys():
                        fields[cdm_field_name] = [np.nan]

                if cdm_map["map_type"] == "direct":
                    if len(cdm_map["erp_fields"]) > 0:
                        file_name = cdm_map["erp_fields"][0]["file_name"]
                        erp_field_name = cdm_map["erp_fields"][0]["field_name"]
                        cdm_data_type = cdm_map["data_type"]
                        fields[cdm_field_name] = list(
                            input_files[file_name]["data"][erp_field_name].values
                        )
                        if cdm_map["data_type"] == "decimal":
                            fields[cdm_field_name] = [
                                float(check_is_number(val)[0])
                                if check_is_number(val)[1]
                                else None
                                for val in fields[cdm_field_name]
                            ]
                        if cdm_map["data_type"] == "integer":
                            fields[cdm_field_name] = [
                                int(check_is_number(val)[0])
                                if check_is_number(val)[1]
                                else None
                                for val in fields[cdm_field_name]
                            ]
                    else:
                        fields[cdm_field_name] = [np.nan]

                if cdm_map["map_type"] == "date-mask":
                    file_name = cdm_map["erp_fields"][0]["file_name"]
                    erp_field_name = cdm_map["erp_fields"][0]["field_name"]
                    inp_data = list(
                        input_files[file_name]["data"][erp_field_name].values
                    )
                    output_data = list()
                    for idx, data in enumerate(inp_data):
                        try:
                            if data:
                                date_string = datetime.strptime(
                                    data, cdm_map["date_format"]
                                )
                                output_value = date_string.strftime("%Y-%m-%dT%H:%M:%S")
                                output_value = f"{output_value}Z"
                            else:
                                output_value = None
                        except:
                            # the value is not in the format specified
                            errors.append(
                                {
                                    "report_name": report_name,
                                    "file_name": file_name,
                                    "field_name": erp_field_name,
                                    "data_row": idx,
                                    "error": f"The value {data} is not in the proper Date Format",
                                }
                            )
                            output_value = np.nan
                        finally:
                            output_data.append(output_value)
                    fields[cdm_field_name] = output_data

                if cdm_map["map_type"] == "calculated":

                    if cdm_map["operation"] == "MC":
                        calculation = calculate_expression(cdm_map, input_files)
                        file_name = cdm_map["erp_fields"][0]["file_name"]
                        if calculation.get("status") == False:
                            errors.append(
                                {
                                    "report_name": report_name,
                                    "cdm_field": cdm_field_name,
                                    "file_name": file_name,
                                    "field_name": None,
                                    "data_row": calculation.get("bad_row"),
                                    "error": f"{calculation.get('message')}. {calculation.get('failed_exp')} ",
                                }
                            )

                        fields[cdm_field_name] = calculation.get("data")

                    if cdm_map["operation"] == "Journal Line Number":
                        inp_data = list(
                            input_files[file_name]["data"][erp_field_name].values
                        )
                        output_data = list()
                        counter = dict()
                        for jid in inp_data:
                            if jid in counter:
                                counter[jid] += 1
                            else:
                                counter[jid] = 1
                            output_data.append(counter[jid])

                        fields[cdm_field_name] = output_data

                    def remove_inside_get_outside(account, config):
                        open_char = config["opening_char"]
                        close_char = config["closing_char"]
                        start = False
                        remove_str = ""
                        for char in account:
                            if char == open_char:
                                start = True
                            if start:
                                if (
                                    char in [open_char, close_char]
                                    or char.isnumeric() == True
                                ):
                                    remove_str += char
                            if char == close_char:
                                start = False
                                break
                        return account.replace(remove_str, "")

                    if cdm_map["operation"] == "SGLAN":
                        file_name = cdm_map["erp_fields"][0]["file_name"]
                        erp_field_name = cdm_map["erp_fields"][0]["field_name"]
                        extract_split_fields = cdm_map["extract_split_fields"]
                        inp_data = list(
                            input_files[file_name]["data"][erp_field_name].values
                        )
                        output_data = list()
                        for account in inp_data:
                            if extract_split_fields["opening_char"] in account:
                                splited_account = account.split(
                                    extract_split_fields["opening_char"]
                                )
                                if extract_split_fields["inside_outside"] == "outside":
                                    if "-" in account:
                                        output_data.append(
                                            "".join(
                                                splited_account[
                                                    0 : (len(splited_account) - 1)
                                                ]
                                            )
                                        )
                                    else:
                                        output_data.append(
                                            remove_inside_get_outside(
                                                account, extract_split_fields
                                            )
                                        )
                                else:
                                    if "-" in account:
                                        output_data.append(account.split("-")[-1])
                                    else:
                                        current_pos = 1
                                        v = splited_account[(current_pos * -1)].replace(
                                            extract_split_fields["closing_char"], ""
                                        )
                                        flag = v.isnumeric()
                                        while flag == False and current_pos < len(
                                            splited_account
                                        ):
                                            current_pos = current_pos + 1
                                            v = splited_account[
                                                (current_pos * -1)
                                            ].replace(
                                                extract_split_fields["closing_char"], ""
                                            )
                                            flag = (v.strip()).isnumeric()
                                        output_data.append(v)
                        print("output_data: ", output_data)
                        fields[cdm_field_name] = output_data

                    if cdm_map["operation"] in ["ABS", "SPLIT", "DCI1"]:
                        file_name = cdm_map["erp_fields"][0]["file_name"]
                        erp_field_name = cdm_map["erp_fields"][0]["field_name"]
                        inp_data = list(
                            input_files[file_name]["data"][erp_field_name].values
                        )
                        output_data = list()
                        extra_data = list()
                        extra_required = (
                            True
                            if cdm_map["operation"] == "SPLIT"
                            and cdm_map.get("extra", "-1") != "-1"
                            else False
                        )
                        for idx, data in enumerate(inp_data):
                            if cdm_map["operation"] == "ABS":
                                try:
                                    numeric_value = (
                                        float(check_is_number(data)[0])
                                        if check_is_number(data)[1]
                                        else 0
                                    )
                                    output_value = abs(numeric_value)
                                except:
                                    # error to be reported as the data is not numeric to convert to float/int for abs function
                                    errors.append(
                                        {
                                            "report_name": report_name,
                                            "cdm_field": cdm_field_name,
                                            "file_name": file_name,
                                            "field_name": erp_field_name,
                                            "data_row": idx,
                                            "error": f"The value {data} is not numeric to convert to float/int for absolute function",
                                        }
                                    )
                                    output_value = np.nan
                            elif cdm_map["operation"] == "DCI1":
                                try:
                                    numeric_value, valid = check_is_number(data)
                                    if valid:
                                        output_value = (
                                            "D" if numeric_value >= 0 else "C"
                                        )
                                    else:
                                        errors.append(
                                            {
                                                "report_name": report_name,
                                                "cdm_field": cdm_field_name,
                                                "file_name": file_name,
                                                "field_name": erp_field_name,
                                                "data_row": idx,
                                                "error": f"The value {data} is not numeric to convert to float/int for DbCrInd1 function",
                                            }
                                        )
                                        output_value = np.nan
                                except:
                                    # error to be reported as the data is not numeric to convert to float/int for abs function
                                    errors.append(
                                        {
                                            "report_name": report_name,
                                            "cdm_field": cdm_field_name,
                                            "file_name": file_name,
                                            "field_name": erp_field_name,
                                            "data_row": idx,
                                            "error": f"The value {data} is not numeric to convert to float/int for DbCrInd1 function",
                                        }
                                    )
                                    output_value = np.nan
                            else:
                                length_of_data = len(str(data))
                                char_length = int(cdm_map["characters"])
                                if length_of_data == 0:
                                    output_value = ""
                                    if extra_required:
                                        extra_data.append("")
                                elif length_of_data >= char_length:
                                    output_value = str(data)[0:char_length]
                                    if extra_required:
                                        extra_data.append(str(data)[char_length:])
                                else:
                                    # error to be reported as characters to split is more than the length of the string
                                    errors.append(
                                        {
                                            "report_name": report_name,
                                            "cdm_field": cdm_field_name,
                                            "file_name": file_name,
                                            "field_name": erp_field_name,
                                            "data_row": idx,
                                            "error": f"Could not split the value {data} for {cdm_map['characters']} characters",
                                        }
                                    )
                                    output_value = str(data)
                                    if extra_required:
                                        extra_data.append(np.nan)
                            output_data.append(output_value)
                        fields[cdm_field_name] = output_data
                        if extra_required:
                            fields[cdm_map["extra"]] = extra_data

                    if cdm_map["operation"] in ["DIV", "DC", "DCI3"]:
                        if cdm_map["operation"] in ["DCI3"]:
                            ref_field = cdm_map["erp_fields"][0]["field_name"]
                            amt_field = cdm_map["erp_fields"][1]["field_name"]
                            file = (
                                cdm_map["erp_fields"][0]["file_name"]
                                if cdm_map["erp_fields"][0]["file_name"]
                                == cdm_map["erp_fields"][1]["file_name"]
                                else None
                            )
                            if file:
                                dci3_data = input_files[file]["data"][[ref_field, amt_field]]
                                output_data = []
                                for index, row in dci3_data.iterrows():
                                    result = range_based_debit_credit(
                                        row[ref_field],
                                        row[amt_field],
                                        cdm_map["rangeDetails"],
                                    )
                                    if result['status'] == True:
                                        output_data.append(result['output'])
                                    else:
                                        errors.append(
                                            {
                                                "report_name": report_name,
                                                "cdm_field": cdm_field_name,
                                                "file_name": cdm_map["erp_fields"][0]["file_name"],
                                                "field_name": f"{ref_field} | {amt_field}",
                                                "data_row": index,
                                                "error": f"{result['error_message']} | Data: {result['error_value']}",
                                            }
                                        )
                            else:
                                output_data = [np.nan]
                        else:
                            first_file = cdm_map["erp_fields"][0]["file_name"]
                            second_file = cdm_map["erp_fields"][1]["file_name"]
                            first_field = cdm_map["erp_fields"][0]["field_name"]
                            second_field = cdm_map["erp_fields"][1]["field_name"]
                            if first_file == second_file:
                                first_file_data = list(
                                    input_files[first_file]["data"][first_field].values
                                )
                                second_file_data = list(
                                    input_files[second_file]["data"][
                                        second_field
                                    ].values
                                )
                                output_data = list()
                                for idx, data in enumerate(first_file_data):
                                    if cdm_map["operation"] == "DIV":
                                        try:
                                            first_value = float(data)
                                            second_value = float(second_file_data[idx])
                                            output_value = first_value / second_value
                                        except:
                                            # error to be reported as the data is not numeric or division by zero is attempted
                                            errors.append(
                                                {
                                                    "report_name": report_name,
                                                    "cdm_field": cdm_field_name,
                                                    "file_name": file_name,
                                                    "field_name": erp_field_name,
                                                    "data_row": idx,
                                                    "error": f"Could not divide the value {data} by {second_file_data[idx]}",
                                                }
                                            )
                                            output_value = np.nan
                                    else:
                                        try:
                                            first_value = (
                                                check_is_number(str(data))[0]
                                                if check_is_number(str(data))[1]
                                                else 0
                                            )
                                            second_value = (
                                                check_is_number(second_file_data[idx])[
                                                    0
                                                ]
                                                if check_is_number(
                                                    str(second_file_data[idx])
                                                )[1]
                                                else 0
                                            )
                                            output_value = first_value + second_value
                                        except:
                                            # error to be reported as the data is not numeric or division by zero is attempted
                                            errors.append(
                                                {
                                                    "report_name": report_name,
                                                    "cdm_field": cdm_field_name,
                                                    "file_name": file_name,
                                                    "field_name": erp_field_name,
                                                    "data_row": idx,
                                                    "error": f"Could not combine the values {data} and {second_file_data[idx]}",
                                                }
                                            )
                                            output_value = np.nan
                                    output_data.append(output_value)
                            else:
                                # cannot do operation on different files
                                errors.append(
                                    {
                                        "report_name": report_name,
                                        "cdm_field": cdm_field_name,
                                        "file_name": None,
                                        "field_name": None,
                                        "data_row": None,
                                        "error": f"Using fields of different input files",
                                    }
                                )
                                output_data = [np.nan]
                        fields[cdm_field_name] = output_data
                    if cdm_map["operation"] in [
                        "SUM",
                        "DIFF",
                        "PROD",
                        "CONCAT",
                        "DCI2",
                    ]:
                        erp_fields = list(
                            sorted(cdm_map["erp_fields"], key=lambda i: i["order"])
                        )
                        fields_required = [
                            field_info["field_name"] for field_info in erp_fields
                        ]
                        file_name = erp_fields[0]["file_name"]
                        file_data = input_files[file_name]["data"]
                        required_data = file_data[fields_required]
                        required_data = (
                            required_data.fillna("")
                            if cdm_map["operation"] == "CONCAT"
                            else required_data.fillna(0)
                        )
                        records = list(required_data.to_records(index=False))
                        if cdm_map["operation"] == "SUM":
                            output_data = [calculate_sum(record) for record in records]
                        elif cdm_map["operation"] == "DIFF":
                            output_data = [calculate_diff(record) for record in records]
                        elif cdm_map["operation"] == "PROD":
                            output_data = [
                                calculate_product(record) for record in records
                            ]
                        elif cdm_map["operation"] == "DCI2":
                            output_data = list()
                            for idx, values in enumerate(records):
                                ind = identify_debit_credit(values[0], values[1])
                                if ind:
                                    output_data.append(ind)
                                else:
                                    errors.append(
                                        {
                                            "report_name": report_name,
                                            "cdm_field": cdm_field_name,
                                            "file_name": file_name,
                                            "field_name": "",
                                            "data_row": idx,
                                            "error": f"Could not determine Debit/Credit Indicator for {str(values)}",
                                        }
                                    )
                                    output_data.append(np.nan)
                        else:
                            output_data = [concat_string(record) for record in records]
                            # print("output_data: ",output_data)
                        fields[cdm_field_name] = output_data
            fields = adjust_fields_data(fields)
            if len(list(fields.keys())) > 0:
                report_data["fields"] = fields
                report_data["field_order"] = cdm_order_fields
                intermediate_files.append(report_data)

        for report in intermediate_files:
            df = pd.DataFrame(report["fields"])
            report["data"] = df[report["field_order"]]

        intermediate_files = [
            {
                "report_type": report["report_type"],
                "data": report["data"],
                "bad_file_data": [
                    {"file_name": file_name, "invalid_rows": details["bad_rows"]}
                    for file_name, details in input_files.items()
                ],
            }
            for report in intermediate_files
        ]

        return intermediate_files, errors
    except CDMConversionException as cdm_e:
        error_info = json.loads(str(cdm_e))
        conversion_errors.append(error_info)
        error_data = {
            "failed_function": "create_report_content",
            "called_method": error_info["failed_function"],
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)
    except Exception as e:
        error_data = {
            "failed_function": "create_report_content",
            "called_method": None,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


# COMMAND ----------


def create_zip_file(file_to_zip):
    try:
        zip_destination = (
            f"file:/tmp/{ingestion_id}/" if not local else f"/tmp/{ingestion_id}/"
        )
        zip_filename = "cdm.zip"
        zip_filepath = f"{zip_destination}{zip_filename}"
        try:
            os.makedirs(os.path.dirname(zip_destination))
        except FileExistsError as e:
            print("Folder already exists")
        file = open(zip_filepath, "w")
        file.close()
        zf = zipfile.ZipFile(zip_filepath, "w", zipfile.ZIP_DEFLATED)
        for filepath in file_to_zip:
            zf.write(filepath, f'/cdm/{filepath.split("/")[-1]}')
        zf.close()
        shutil.move(zip_filepath, adjust_filepath(f"{dbfs_output_container_path}"))
        if not local:
            dbutils.fs.rm(f"{temp_path}", True)
            copy_data()
    except CDMConversionException as cdm_e:
        error_info = json.loads(str(cdm_e))
        conversion_errors.append(error_info)
        error_data = {
            "failed_function": "create_zip_file",
            "called_method": error_info["failed_function"],
            "file_paths": file_to_zip,
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)
    except Exception as e:
        error_data = {
            "failed_function": "create_zip_file",
            "called_method": None,
            "file_paths": file_to_zip,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


# COMMAND ----------


class DQS(ABC):
    def __init__(self, data, entity_type, cdm_fields):
        self.data = data
        self.entity_type = entity_type
        self.cdm = [
            {"name": cdm_field["cdm_field"], "type": cdm_field["data_type"]}
            for cdm_field in cdm_fields
        ]

    def get_precent(self, count, total):
        if total == 0:
            return 0
        else:
            return round((count / total) * 100)

    def get_unique_value_count(self, column_name):
        values = list(self.data[column_name].dropna().unique())
        values = [val for val in values if val != ""]
        return len(values)

    def all_rows_blank(self, columns):
        present = 0
        controls_new_list = list()
        for column in columns:
            controls_new_list.append({column: not self.data[column].isnull().all()})
            present = present + 1 if not self.data[column].isnull().all() else present
        control_check_percentage = self.get_precent(present, len(columns))
        return (
            control_check_percentage,
            controls_new_list,
        )

    @abstractmethod
    def statistical(self):
        pass

    @abstractmethod
    def business_rule(self):
        pass

    def profile(self):
        df = self.data.replace(r"^\s*$", np.nan, regex=True)
        # profile score------ filled rates
        total_values = int(df.shape[1])
        filled_fields = df.columns[df.notna().any()].tolist()
        filled_percent = int(self.get_precent(len(filled_fields), total_values))

        # profile score------ empty fields
        empty_fields = df.columns[df.isna().all()].tolist()
        count_empty_values = len(empty_fields)
        empty_percent = int(self.get_precent(count_empty_values, total_values))

        # profile score------ Text Fields Empty
        text_fields = [
            field["name"]
            for field in list(
                filter(
                    lambda cdm_field: cdm_field["type"] in ["string"]
                    and cdm_field["name"] in list(self.data.columns),
                    self.cdm,
                )
            )
        ]
        text_data = df[text_fields]
        total_text_values = int(text_data.shape[1])
        null_text_fields_column = list(text_data.columns[text_data.isna().any()])
        count_null_text = len(null_text_fields_column)
        null_text_percent = int(self.get_precent(count_null_text, total_text_values))

        # profile score------ number empty fields
        numeric_fields = [
            field["name"]
            for field in list(
                filter(
                    lambda cdm_field: cdm_field["type"]
                    in ["decimal", "number", "integer"]
                    and cdm_field["name"] in list(self.data.columns),
                    self.cdm,
                )
            )
        ]
        numeric_data = df[numeric_fields]
        total_numeric_values = int(numeric_data.shape[1])
        null_numeric_fields_column = list(
            numeric_data.columns[numeric_data.isna().any()]
        )
        count_null_numeric = len(null_numeric_fields_column)
        null_numeric_percent = int(
            self.get_precent(count_null_numeric, total_numeric_values)
        )

        # profile score------ missing fields
        total_records = int(df.shape[0])
        gapped_fields = list()
        for emp_field in filled_fields:
            field_gaps = df[emp_field].isnull().sum()
            if field_gaps < total_records and field_gaps > 0:
                gapped_fields.append(emp_field)
        total_fields = len(filled_fields)
        missing_record_details = list()
        for fld in gapped_fields:
            total_not_null_count = df[fld].count()

            if total_not_null_count > 0:
                missing_record_details.append(
                    {
                        fld: int(
                            self.get_precent(
                                total_not_null_count, len(list(self.data[fld]))
                            )
                        )
                    }
                )
        missing_records_percent = int(
            self.get_precent(total_fields - len(gapped_fields), total_fields)
        )
        return [
            {
                "fieldsFilled": {
                    "percent": filled_percent,
                    "total": total_values,
                    "filled": len(filled_fields),
                    "fields": filled_fields,
                }
            },
            {
                "fieldsEmpty": {
                    "percent": empty_percent,
                    "total": total_values,
                    "empty": count_empty_values,
                    "fields": empty_fields,
                }
            },
            {
                "textFieldsEmpty": {
                    "percent": null_text_percent,
                    "total": total_text_values,
                    "empty": count_null_text,
                    "fields": null_text_fields_column,
                }
            },
            {
                "numericFieldsEmpty": {
                    "percent": null_numeric_percent,
                    "total": total_numeric_values,
                    "empty": count_null_numeric,
                    "fields": null_numeric_fields_column,
                }
            },
            {
                "missingRecords": {
                    "percent": missing_records_percent,
                    "total": total_fields,
                    "empty": len(gapped_fields),
                    "fields": missing_record_details,
                }
            },
        ]


# COMMAND ----------


class GLDQS(DQS):
    def statistical(self):
        # gl-detail-statistical score -total record
        gl_detail_total_record = self.data.shape[0]
        total_fields = self.data.shape[1]

        # gl-detail-statistical score -total amount sum
        gl_detail_total_amount_sum = self.data["amount"].sum(skipna=True)

        # gl-detail-statistical score - sum of local amount
        gl_detail_local_amount_sum = self.data["localAmount"].sum(skipna=True)

        # gl-detail-statistical score - Debits/Credits
        net_amount = self.data.apply(
            lambda x: float(x["amount"]) * -1
            if x["amountCreditDebitIndicator"] == "C"
            else float(x["amount"]),
            axis=1,
        )
        sum_net_amount = net_amount.sum(skipna=True)
        # gl-detail-statistical score - GL Account Code Count
        gl_detail_account_code_count = self.get_unique_value_count("glAccountNumber")
        # gl-detail-statistical score - Number of Business Units
        gl_detail_business_unit_code_count = self.get_unique_value_count(
            "businessUnitCode"
        )

        return [
            {"recordCount": float(round(gl_detail_total_record, 5))},
            {"fieldCount": float(round(total_fields, 5))},
            {"amountSum": float(round(gl_detail_total_amount_sum, 5))},
            {"localAmountSum": float(round(gl_detail_local_amount_sum, 5))},
            {"debitCredits": float(round(sum_net_amount, 5))},
            {"glAccountCodeCount": float(round(gl_detail_account_code_count, 5))},
            {"businessUnitCounts": float(round(gl_detail_business_unit_code_count, 5))},
        ]

    def business_rule(self):
        net_amount = self.data.apply(
            lambda x: x["amount"] * -1
            if x["amountCreditDebitIndicator"] == "C"
            else x["amount"],
            axis=1,
        )
        sum_net_amount = net_amount.sum(skipna=True)
        # GL - Detail Bussiness Rule -------------------- netting
        gl_detail_netting = "pass" if -0.001 <= sum_net_amount <= 0.001 else "fail"
        # GL - Detail Bussiness Rule ----------------- Controls checks:
        columns_to_check = [
            "enteredBy",
            "enteredDateTime",
            "approvedBy",
            "approvedDateTime",
            "lastModifiedBy",
        ]
        control_check_percentage, controls_new_list = self.all_rows_blank(
            columns_to_check
        )

        # GL - Detail Bussiness Rule ----------------- Revarsals:
        columns_to_check = ["reversalIndicator", "reversalJournalId"]
        reversal_percentage, reversals_new_list = self.all_rows_blank(columns_to_check)

        # GL - Detail Bussiness Rule - ---------------- Manual  Journals:
        columns_to_check = ["journalEntryType", "sourceId"]
        manual_journals_percentage, journals_new_list = self.all_rows_blank(
            columns_to_check
        )

        # GL - Detail Bussiness Rule - ---------------- Missing Sequence:
        missing_sequence_data = self.data[["journalId"]]
        try:
            journal_id_int = pd.to_numeric(
                missing_sequence_data["journalId"], errors="coerce"
            )
            if journal_id_int.isnull().sum() == 0:
                missing_seq_data = journal_id_int.diff()
                null_values = list(missing_sequence_data.isna().sum())[0]
                missing_seq_count = int(missing_seq_data[lambda x: x != 1.0].shape[0])
                total_rows = int(missing_sequence_data.shape[0])
                if null_values == total_rows:
                    missing_seq_detail = {
                        "overall": 0,
                        "fields": [
                            {
                                "journalId": False,
                                "show_warning": True,
                                "warning": "Could not locate missing sequences, all values are blank.",
                            }
                        ],
                    }
                else:
                    seq_count = missing_seq_count + null_values - 1
                    miss_seq_prcnt = (
                        ((total_rows - seq_count) / total_rows) * 100
                        if total_rows != 0
                        else 0
                    )
                    missing_seq_detail = {
                        "overall": math.floor(miss_seq_prcnt),
                        "fields": [
                            {
                                "journalId": miss_seq_prcnt == 100,
                                "show_warning": False,
                            }
                        ],
                    }
            else:
                missing_seq_detail = {
                    "overall": 0,
                    "fields": [
                        {
                            "journalId": False,
                            "show_warning": True,
                            "warning": "JournalId does not appear to be a numeric sequence",
                        }
                    ],
                }
        except:
            missing_seq_detail = {
                "overall": 0,
                "fields": [
                    {
                        "journalId": False,
                        "show_warning": True,
                        "warning": "Could not locate missing sequences.",
                    }
                ],
            }
        return [
            {"netting": gl_detail_netting},
            {
                "controlChecks": {
                    "overall": float(control_check_percentage),
                    "fields": list(controls_new_list),
                }
            },
            {
                "reversals": {
                    "overall": float(reversal_percentage),
                    "fields": list(reversals_new_list),
                }
            },
            {
                "manualJournals": {
                    "overall": float(manual_journals_percentage),
                    "fields": list(journals_new_list),
                }
            },
            {"missingSequentialItems": missing_seq_detail},
        ]


# COMMAND ----------


class TBDQS(DQS):
    def statistical(self):

        # TB --statistical score- record count -------
        total_record_trail_bal_df = self.data.shape[0]
        total_fields = self.data.shape[1]
        # TB --statistical score- amountBeginning Sum  -------
        amount_beginning_sum = self.data["amountBeginning"].sum(skipna=True)
        amount_ending_sum = self.data["amountEnding"].sum(skipna=True)
        amount_ending_sum = (
            0
            if amount_ending_sum > -0.000001 and amount_ending_sum < 0.000001
            else amount_ending_sum
        )
        # TB --statistical score- glAccountNumber-------
        gl_account_code_count = self.get_unique_value_count("glAccountNumber")
        # TB --statistical score- Number of Business Units -------
        business_unit_code_count = self.get_unique_value_count("businessUnitCode")
        return [
            {"recordCount": float(round(total_record_trail_bal_df, 5))},
            {"fieldCount": float(total_fields)},
            {"amountBeginingSum": float(round(amount_beginning_sum, 5))},
            {"amountEndingSum": float(round(amount_ending_sum, 5))},
            {"glAccountCodeCount": float(round(gl_account_code_count, 5))},
            {"businessUnitCounts": float(round(business_unit_code_count, 5))},
        ]

    def business_rule(self):
        sum_net_amount = self.data["amountEnding"].sum(skipna=True)
        # TB --business rule score- netting  -------
        return [{"netting": "pass" if -0.001 <= sum_net_amount <= 0.001 else "fail"}]


# COMMAND ----------


class COADQS(DQS):
    def statistical(self):
        coa_total_record = self.data.shape[0]
        total_fields = self.data.shape[1]
        # COA --- statistical score -- glAccountNumber
        coa_gl_account_number_count = self.get_unique_value_count("glAccountNumber")
        # COA --- statistical score -- Number of Business Units
        coa_business_unit_code_count = self.get_unique_value_count("businessUnitCode")
        return [
            {"recordCount": float(coa_total_record)},
            {"fieldCount": float(total_fields)},
            {"glAccountCodeCount": float(coa_gl_account_number_count)},
            {"businessUnitCounts": float(coa_business_unit_code_count)},
        ]

    def business_rule(self):
        return []


# COMMAND ----------

# dbutils.fs.rm(f'{working_dir}',True)
def adjust_standard_mapping(report_file_struct):
    try:
        if local:
            files_from_folder = [
                file_info
                for file_info in os.listdir(dbfs_input_container_path)
                if file_info != MAPPING_FILE_NAME
                and not file_info.endswith(".yml")
                and not file_info.endswith(".DS_Store")
            ]
        else:
            files_from_folder = [
                file_info.name
                for file_info in dbutils.fs.ls(dbfs_input_container_path)
                if file_info.name != MAPPING_FILE_NAME
                and not file_info.name.endswith(".yml")
            ]
        files_from_mapping = list()
        for report in report_file_struct:
            files_from_mapping += list(report["file_info"].keys())
        if len(files_from_folder) > len(files_from_mapping):
            print(
                "Ingestion contains extra files that are not mapped. Untracked files will be ignored"
            )
            return report_file_struct
        elif len(files_from_folder) < len(files_from_mapping):
            print(
                "Few files missing as per the standard mapping. Standard Mapping will be modifed as per the files uploaded"
            )
            new_structure = list()
            for file_name in files_from_folder:
                report = list(
                    filter(
                        lambda x: file_name in x["file_info"].keys(), report_file_struct
                    )
                )
                if len(report) > 0:
                    report = report[0]
                    report_name = report["report_name"]
                    already_exisits = list(
                        filter(lambda x: x["report_name"] == report_name, new_structure)
                    )
                    if len(already_exisits) == 0:
                        new_structure.append(report)
            report_file_struct = new_structure
            if len(report_file_struct) > 0:
                print("Standard Mapping modified")
                return new_structure
            else:
                raise ValueError(
                    "Could not find valid mapping from the Standard Mapping"
                )
        else:
            print("Standard Mapping will applied without any modifications.")
            return report_file_struct
    except CDMConversionException as cdm_e:
        error_info = json.loads(str(cdm_e))
        conversion_errors.append(error_info)
        error_data = {
            "failed_function": "adjust_standard_mapping",
            "called_method": error_info["failed_function"],
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)
    except Exception as e:
        error_data = {
            "failed_function": "adjust_standard_mapping",
            "called_method": None,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


# COMMAND ----------


def insert_erp_file_config(report_file_struct, extract_config):
    try:
        for report in report_file_struct:
            for file_name in list(report["file_info"].keys()):
                config_file_name = file_name[len(report["report_name"]) + 1 :]
                report_config = [
                    rpt_cfg
                    for rpt_cfg in extract_config["reports"]
                    if rpt_cfg["report_type"] == report["report_name"]
                ][0]
                file_config = [
                    file_cfg
                    for file_cfg in report_config["input_files"]
                    if file_cfg["file_name"] == config_file_name
                ][0]
                report["file_info"][file_name]["encoding_format"] = file_config[
                    "encoding_format"
                ]
                report["file_info"][file_name]["template"] = file_config["template"]
        return report_file_struct
    except Exception as e:
        error_data = {
            "failed_function": "insert_erp_file_config",
            "called_method": None,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


# COMMAND ----------


def evaluate_grouping(data):
    try:
        grouping_info = {
            "assets": {
                "search_string": ["asset"],
            },
            "liabilities": {
                "search_string": ["liabilities", "liability"],
            },
            "equity": {
                "search_string": ["equity"],
            },
            "income": {
                "search_string": ["income"],
            },
            "expenditure": {
                "search_string": ["expense"],
            },
        }
        total_accounts = data.glAccountNumber.nunique()
        mapped_accounts = data[data.account.notnull()].glAccountNumber.nunique()
        unique_accounts = data[data.account.notnull()].account.unique()
        for _, value in grouping_info.items():
            search_strings = value["search_string"]
            accounts_to_search = [
                account
                for account in unique_accounts
                for srch in search_strings
                if srch in account.lower()
            ]

            account_data = data[data.account.isin(accounts_to_search)]
            if not account_data.empty:
                amount_total = account_data.amountEnding.sum()
                mapped_acounts = account_data.glAccountNumber.nunique()
            else:
                amount_total = 0
                mapped_acounts = 0

            value["accounts"] = accounts_to_search
            value["total_value"] = amount_total
            value["mapped"] = mapped_acounts

        quality_scorecard = {
            "total_codes": total_accounts,
            "mapped_codes": mapped_accounts,
            "account_detail": grouping_info,
        }

        return quality_scorecard
    except Exception as e:
        error_data = {
            "failed_function": "evaluate_grouping",
            "called_method": None,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


# COMMAND ----------


def trim_account_name(account_name: str):
    name = account_name.strip()
    return name[2:] if name.startswith('- ') else name[1:] if name.startswith('-') else name

def create_grouped_trial_balance(cdm_tb_output):
    try:
        path_to_zip = None

        grouping_response = fetch_groupings()
        grouping_response = (
            grouping_response
            if grouping_response
            else {"data": {"nominalCodeMappings": None}}
        )

        if grouping_response["data"]["nominalCodeMappings"]:
            grouping_data = grouping_response["data"]["nominalCodeMappings"]
            grouping_df = pd.DataFrame(grouping_data["grouping"])

            tb_data = cdm_tb_output["data"].copy(deep=True)
            tb_data['cleaned_glAccountName'] = tb_data.apply(lambda x: trim_account_name(str(x['glAccountName'])),axis=1)
            grouping_df['cleaned_description'] = grouping_df.apply(lambda x: str(x['description']).strip(),axis=1)
            
            print("tb_data: \n", tb_data)
            print("grouping_df: \n", grouping_df)
            print(tb_data[["glAccountNumber", "cleaned_glAccountName","glAccountName"]])
            
            print(grouping_df.dtypes, tb_data.dtypes)
            tb_data['glAccountNumber'].fillna(0).astype(str)
            tb_data['cleaned_glAccountName'].fillna("").astype(str)
            grouping_df['nominalCode'].fillna(0).astype(str)
            grouping_df['cleaned_description'].fillna("").astype(str)
            

            grouped_trialbalance = tb_data.merge(
                grouping_df,
                left_on=["glAccountNumber", "cleaned_glAccountName"],
                right_on=["nominalCode", "cleaned_description"],
                how="left",
            )
            
            grouped_trialbalance = grouped_trialbalance.drop(columns=["nominalCode", "description", "cleaned_description", "cleaned_glAccountName"])
            grouped_trialbalance.drop_duplicates(inplace=True)

            path_to_zip = adjust_filepath(
                f"{temp_path}{cdm_tb_output['report_type']}_grouped.csv"
            )
            print("grouped_trialbalance: \n", grouped_trialbalance)
            grouped_trialbalance.to_csv(path_to_zip, index=False)

            grouping_file_path = adjust_filepath(
                f"{dbfs_gdqs_output_container_path}grouping.json"
            )

            quality_scorecard = evaluate_grouping(grouped_trialbalance)
            grouping_data["quality"] = quality_scorecard

            grouping_df.columns = [
                "accountType" if col == "account" else col
                for col in list(grouping_df.columns)
            ]
            grouping_data["grouping"] = grouping_df.to_dict("records")

            with open(grouping_file_path, "w") as outfile:
                json.dump(grouping_data, outfile)
                print("Grouping created, evaluated and saved!!!")

        return path_to_zip
    except CDMConversionException as cdm_e:
        error_info = json.loads(str(cdm_e))
        conversion_errors.append(error_info)
        error_data = {
            "failed_function": "create_grouped_trial_balance",
            "called_method": error_info["failed_function"],
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)
    except Exception as e:
        error_data = {
            "failed_function": "create_grouped_trial_balance",
            "called_method": None,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


# COMMAND ----------

# dbutils.fs.rm(f'{working_dir}',True)

# COMMAND ----------

file_to_zip = list()
try:
    print("----------Start of JOB----------")
    mapping = read_mapping_file()
    if not mapping:
        print("No mapping file found")
    else:
        print("Mapping file read")
        # combine cdm fields with extra_fields
        cdm_fields = list()
        for mapping_type, mappings in mapping.items():
            for cdm_map in mappings:
                cdm_fields.append(cdm_map)
        erp_id = cdm_fields[0]["erp_Id"]
        config_file_name = f"{erp_id}.yml"
        extract_type = cdm_fields[0]["extract_type"]
        if not local:
            dbutils.fs.cp(
                f"{ERP_CONFIG_PATH}{config_file_name}",
                f"{dbfs_input_container_path}{config_file_name}",
                recurse=True,
            )
        erp_config = read_erp_config_file(
            f"{dbfs_input_container_path}{config_file_name}"
        )

        extract_config = [
            extract
            for extract in erp_config["mappings"]
            if extract["extract"] == extract_type
        ]

        report_file_struct = create_reprot_file_field_map(cdm_fields)

        if standard_mapping:
            report_file_struct = adjust_standard_mapping(report_file_struct)

        report_file_struct = insert_erp_file_config(
            report_file_struct, extract_config[0]
        )
        print("Intermidiate structure created for the mapping")

        mappings = fill_input_data(report_file_struct)
        print("Input files Read Complete")

        reports, type_errors = generate_data_type_error_report(mappings)
        print(
            "Input File Validation is done. Data Type Errors found:", len(type_errors)
        )

        if len(reports) > 0:
            report_files, errors = create_report_content(reports)
            print(
                "CDM File Conversion Complete. Erros found while converting:",
                len(errors),
            )
            errors = type_errors + errors
        else:
            errors = type_errors
            print("Skipped CDM File Conversion. Data Type Errors found")
        print("Total Errors:", len(errors))

        if len(errors) > 0:
            print("Errors found while processing the report")
            error_df = pd.DataFrame(errors)
            error_df.sort_values(by=["report_name", "data_row"], inplace=True)
            error_df.to_csv(
                adjust_filepath(f"{dbfs_error_container_path}errors.csv"), index=False
            )
            file_to_zip.append(
                adjust_filepath(f"{dbfs_error_container_path}errors.csv")
            )
            send_job_status("FAILED")
            print("Custom Mapper CDM Conversion Status is FAILED!!!")
        else:
            score_card_dict = {}
            for inter_files in report_files:
                inter_files["data"].to_csv(
                    adjust_filepath(f"{temp_path}{inter_files['report_type']}.csv"),
                    index=False,
                )
                file_to_zip.append(
                    adjust_filepath(f"{temp_path}{inter_files['report_type']}.csv")
                )
                cdm_fields = list(
                    filter(
                        lambda report: report["report_name"]
                        == inter_files["report_type"],
                        reports,
                    )
                )[0]["mapping"]

                if inter_files["report_type"] == "glDetail":
                    dqs = GLDQS(
                        inter_files["data"], inter_files["report_type"], cdm_fields
                    )
                elif inter_files["report_type"] == "trialBalance":
                    dqs = TBDQS(
                        inter_files["data"], inter_files["report_type"], cdm_fields
                    )

                    grouping_path = create_grouped_trial_balance(inter_files)
                    if grouping_path:
                        file_to_zip.append(grouping_path)

                elif inter_files["report_type"] == "ChartOfAccounts":
                    dqs = COADQS(
                        inter_files["data"], inter_files["report_type"], cdm_fields
                    )
                else:
                    dqs = None
                if dqs:
                    score_card_dict[inter_files["report_type"]] = {
                        "statistical": dqs.statistical(),
                        "business_rule": dqs.business_rule(),
                        "profile": dqs.profile(),
                    }

            if len(score_card_dict.keys()) > 0:
                dqs_file_path = adjust_filepath(
                    f"{dbfs_dqs_output_container_path}dqs.json"
                )
                with open(dqs_file_path, "w") as outfile:
                    json.dump(score_card_dict, outfile)
                    print("DQS Calculated and saved!!!")
            send_job_status("COMPLETED")
            print("Custom Mapper CDM Conversion Status is COMPLETED!!!")
except CDMConversionException as cdm_e:
    print(cdm_e)
    error_info = json.loads(str(cdm_e))
    conversion_errors.append(error_info)
    error_data = {
        "failed_function": "main",
        "called_method": error_info["failed_function"],
    }
    conversion_errors.append(error_data)
    error_df = format_cmd_conversion_error(conversion_errors)
    error_df.to_csv(
        adjust_filepath(f"{dbfs_error_container_path}errors.csv"), index=False
    )
    file_to_zip.append(adjust_filepath(f"{dbfs_error_container_path}errors.csv"))
    send_job_status("FAILED")
    print("Custom Mapper CDM Conversion Status is FAILED!!!")
except Exception as e:
    print("Un-Expected Error Occured", e)
    errors = [{"error": str(e)}]
    error_df = pd.DataFrame(errors)
    error_df.to_csv(
        adjust_filepath(f"{dbfs_error_container_path}errors.csv"), index=False
    )
    file_to_zip.append(adjust_filepath(f"{dbfs_error_container_path}errors.csv"))
    send_job_status("FAILED")
    print("Custom Mapper CDM Conversion Status is FAILED!!!")
finally:
    create_zip_file(file_to_zip)
    if not local:
        dbutils.fs.rm(f"{working_dir}", True)
        print(f"{working_dir}/", "Working Directory Deleted...")
    print("----------End of JOB----------")
