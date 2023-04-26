# cmd 1
from datetime import datetime
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
from zipfile import ZipFile
import shutil

# cmd 2
#%run /IntegrationEngine01/utilities/storageAccess

# cmd 3
# global log array
global_log_list=[]
is_local = True
# register log into the log list
def log(msg):
    global_log_list.append(msg)
def display_log():
    for item in global_log_list:
        print(item,'\n\n')


# cmd 4
# request variable
global_group_id=''
global_container_name=''
global_storage_account_name=''
global_storage_url=''
graph_ql_url=''

# databricks setup variables
input_container_path=''
output_container_path=''
global_ingestion_id=''
hdfs_path=''
unzip_dir=''

# databricks setup - input, storage url, output url
log(f"storage access: granted")
if not is_local:
    # # CDM Ingestion ID
    # dbutils.widgets.text("group_id", "","")
    # global_group_id = dbutils.widgets.get("group_id")
    # log(f"global_group_id: {global_group_id}")

    # # container name to access the zip
    # dbutils.widgets.text("containerName", "","")
    # global_container_name = dbutils.widgets.get("containerName")
    # log(f"global_container_name: {global_container_name}")

    # # storage account name
    # dbutils.widgets.text("storage_account_name", "","")
    # global_storage_account_name = dbutils.widgets.get("storage_account_name")
    # log(f"global_storage_account_name: {global_storage_account_name}")

    # # global data variables for storage, input and putput
    # graph_ql_url = os.getenv('STATUS_UPDATE_URL', 'http://localhost:7072/graphql')
    # log(f"graph_ql_url: {graph_ql_url}")

    # global_storage_url = f'abfss://{global_container_name}@{global_storage_account_name}.dfs.core.windows.net'
    # log(f"global_storage_url: {global_storage_url}")

    # graph_ql_url 
    pass
else:
    global_group_id='clgxw6kzz0069ec1w5dligfvl'
    global_container_name='auditfirma'
    global_storage_account_name='stdemocompanyaccount1 '
    global_storage_url='test'
    graph_ql_url = 'https://app-service-intengb-dev-docker-api.azurewebsites.net/graphql'

# cmd 5
def databricks_setup():
    if is_local:
        if not os.path.exists('grouper/input'):
            os.makedirs('grouper/input')
        if not os.path.exists('grouper/output'):
            os.makedirs('grouper/output')
        input_container_path='grouper/input/'
        output_container_path='grouper/output/'
    else:
        input_container_path = f"{global_storage_url}/data-ingestions/{global_ingestion_id}/input/"
        log(f"input_container_path: {input_container_path}")

        output_container_path = f"{global_storage_url}/data-ingestions/{global_ingestion_id}/grouper/"
        log(f"output_container_path: {output_container_path}")

# cmd 6
# log exporter to container
def export_log_to_container():
    try:
        if is_local:
            with open(f"{output_container_path}{global_group_id}_log.json", 'w') as log_writter_object:
                log_writter_object.write(json.dumps({"log":global_log_list}, indent=4))
        else:
            # log("Exporting log to container")
            # json_object = json.dumps({"log": global_log_list}, indent=4)
            # with open(f"/dbfs{hdfs_path}{global_group_id}_log.json", "w") as outfile:
            #     outfile.write(json_object)
            # dbutils.fs.cp(
            #     f'dbfs:{hdfs_path}{global_group_id}_log.json',
            #     f'{output_container_path}log_{str(datetime.now()).replace(" ","_")}.json',
            #     recurse=True)
            pass
    except Exception as ex:
        log(f"Exception(export_log_to_container): {str(ex)}")

# cmd 7
# copy zip from container to HDFS
def copy_cdm_zip_to_databricks():
    try:
        if is_local:
            log("Creating HDFS fo this analytics ID")
            global hdfs_path
            hdfs_path = f"grouper/hdfs/"
            if not os.path.exists(hdfs_path):
                os.makedirs(hdfs_path)
            log("HDFS directory created")

            log("Copy zip from input container to hdfs")
            print("input_container_path: ", input_container_path)
            #shutil.copytree()
            os.system("copy "+f"./{input_container_path}cdm.zip"+" "+f"{hdfs_path}cdm.zip")
            log("unzip the cdm zip")
            with ZipFile(f'{hdfs_path}cdm.zip', 'r') as zObject:
                zObject.extractall(path=f'{hdfs_path}')
            log("Unzip Completed")

            global unzip_dir
            unzip_dir='cdm'
            log(f"unzip_dir: {unzip_dir}")
        else:
            # # setup DBFS local directory to access the file
            # log("Creating HDFS fo this analytics ID")
            # hdfs_path = f"/FileStore/grouper/{global_group_id}/"
            # dbutils.fs.mkdirs(hdfs_path)
            # log("HDFS directory created")

            # # Sample: data-ingestions/clg0h8yx50064h10l2x2eksgw/share/zip/cdm.zip
            # dbutils.fs.cp(f"{global_storage_url}/data-ingestions/{global_group_id}/share/zip/cdm.zip", hdfs_path, recurse=True)
            # log("Zip file copied")
            
            # log("Unzipping..")
            # # unzip the zip into dbfs
            # with ZipFile(f'/dbfs{hdfs_path}cdm.zip', 'r') as zObject:
            #     zObject.extractall(path=f'/dbfs{hdfs_path}')
            # log("Unzip Completed")

            # unzip_dir = "cdm"
            # log(f"UNZIPED_DIR: {unzip_dir}")
            pass
    except Exception as ex:
        log(f"Exception(copy_cdm_zip_to_databricks): {str(ex)}")

# cmd 8
# copy zip to container from HDFS
def copy_groupped_data_to_continaer(grouped_dataframe):
    try:
        grouped_dataframe.to_csv(f"{hdfs_path}trialBalance_groupped_{global_group_id}.csv",index=False)
        if not is_local:
            dbutils.fs.cp(f"dbfs:{hdfs_path}trialBalance_groupped_{global_group_id}.csv",
                          f"{output_container_path}trialBalance_groupped_{global_group_id}.csv",
                          recurse=True)
    except Exception as ex:
        log(f"Exception(copy_groupped_data_to_continaer): {str(ex)}")

# cmd 9
def fetch_groupings():
    try:
        req_body = {
            "query": "query GetDataIngestionGroupings($dataIngestionGrouperId: String) { getDataIngestionGroupings(dataIngestionGrouperId: $dataIngestionGrouperId) { id dataIngestion { name groupName groupId } grouper{id name}}}",
            "variables": {
                "dataIngestionGrouperId": global_group_id
            }
        }
        response = requests.post(
            graph_ql_url,
            data=json.dumps(req_body),
            headers={"Content-Type": "application/json"},
        )
        if response.status_code == 200:
            data = response.json()
            return None if 'errors' in data.keys() else data
        else:
            return None
    except Exception as e:
        log(f"Exception(fetch_groupings): {str(ex)}")

# cmd 10
def dispatch_status(status=''):
    try:
        req_body = {
            "query": "",
            "variables": {
                "dataIngestionGrouperId": global_group_id,
                "status": status
            }
        }
        response = requests.post(
            graph_ql_url,
            data=json.dumps(req_body),
            headers={"Content-Type": "application/json"},
        )
        if response.status_code == 200:
            print(response.status_code)
            log('GraphQL: Job Status Updated')
        else:
            print(response.status_code)
            log('GraphQL: Could not update Job Status')
    except Exception as ex:
        log(f"Exception(fetch_groupings): {str(ex)}")

# cmd 11
def get_cdm_file_dataframe():
    try:
        print(f"==========={hdfs_path}{unzip_dir}/trialBalance.csv")
        trialBalance_dataframe = pd.read_csv(f"{hdfs_path}{unzip_dir}/trialBalance.csv")
        return trialBalance_dataframe
    except Exception as ex:
        log(f"Exception (get_cdm_file_dataframe): {str(ex)}")

# cmd 12
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
        log(f"Exception: {str(e)}")

# cmd 13
def create_grouped_trial_balance(grouping_data_config):
    try:
        log(f"Reading trial balance csv data")
        cdm_tb_output=get_cdm_file_dataframe()
        log(f"trial balance data received")

        log(f"trying to fetch grouping from GraphQL")
        grouping_response = grouping_data_config
        grouping_response = (
            grouping_response
            if grouping_response
            else {"data": {"nominalCodeMappings": None}}
        )
        log(f"Received response from GraphQL")

        if grouping_response["data"]["nominalCodeMappings"]:
            log(f"Mapping started")
            grouping_data = grouping_response["data"]["nominalCodeMappings"]
            grouping_df = pd.DataFrame(grouping_data["grouping"])

            # for IE-1021
            grouping_df = grouping_df.replace("nan", np.nan)
            log(f"Trying to do left merge")
            grouped_trialbalance = cdm_tb_output["data"].merge(
                grouping_df,
                left_on="glAccountNumber",
                right_on="nominalCode",
                how="left",
            )

            grouped_trialbalance = grouped_trialbalance.drop(columns=["nominalCode"])
            log(f"copying grouping data to container")
            copy_groupped_data_to_continaer(grouped_trialbalance)
            
            log(f"quality score card initiated")
            quality_scorecard = evaluate_grouping(grouped_trialbalance)
            grouping_data["quality"] = quality_scorecard

            grouping_df.columns = [
                "accountType" if col == "account" else col
                for col in list(grouping_df.columns)
            ]
            grouping_data["grouping"] = grouping_df.to_dict("records")
            log(f"quality analysis completed. Exiting the mapping function.")
            print(grouping_data)
        else:
            log(f"grouping_response[data][nominalCodeMappings]= {grouping_response['data']['nominalCodeMappings']}")
    except Exception as e:
        log(f"Exception: {str(e)}")

# cmd 14
def main():
    # step 1:
    group_config = fetch_groupings()
    if group_config is not None:
        # step 2:
        databricks_setup()

        # step 3:
        copy_cdm_zip_to_databricks()

        # step 4:
        create_grouped_trial_balance(group_config)

        # step 5:
        export_log_to_container()

        # step 6:
        display_log()
    else:
        log(f"Group config data is None from GraphQL for group_id: {global_group_id}")
        display_log()
if __name__ == '__main__':
    main()