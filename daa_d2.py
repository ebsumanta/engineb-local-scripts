# cmd 1
import json
import os, sys, traceback
from zipfile import ZipFile
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col
import time
import requests
import csv
import pandas as pd
from datetime import datetime

# cmd 2
# %run /IntegrationEngine01/utilities/storageAccess

# cmd 3
local = True
global_analytics_id=''
global_ingestion_id=''
global_storage_account_name=''
global_container_name=''
global_log = []


if local:
    global_analytics_id='clgxw6kzz0069ec1w5dligfvl'
    global_ingestion_id='fghrw6kzz0069ec1w5dadsfgr'
    global_storage_account_name='daa'
    global_container_name='daa'
else:
    # # analytics id to get the json file
    # dbutils.widgets.text("analytics_id","","")
    # global_analytics_id = dbutils.widgets.get("analytics_id")

    # # ingestion id for accessing the path
    # dbutils.widgets.text("ingestion_id","","")
    # global_ingestion_id = dbutils.widgets.get("ingestion_id")

    # # for permission - Storage Account name
    # dbutils.widgets.text("storage_account_name", "","")
    # global_storage_account_name = dbutils.widgets.get("storage_account_name")

    # # for permission - Container Name
    # dbutils.widgets.text("containerName", "","")
    # global_container_name = dbutils.widgets.get("containerName")
    pass

# cmd 4
def log(msg):
    if local:
        print(msg)
    global_log.append(msg)

def display_log():
    for item in global_log:
        print(item,'\n\n')

def check_and_create_directory(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)


def databricks_setup_local():
    log("Creating local execution container")
    STORAGE_URL = 'daa'
    INPUT_CONTAINER_PATH=f'{STORAGE_URL}/input/'
    OUTPUT_CONTAINER_PATH=f'{STORAGE_URL}/output/'
    HDFS_PATH = f'{STORAGE_URL}/hdfs/'

    check_and_create_directory(STORAGE_URL)
    check_and_create_directory(INPUT_CONTAINER_PATH)
    check_and_create_directory(OUTPUT_CONTAINER_PATH)
    check_and_create_directory(HDFS_PATH)
    
    # copy json to hdfs
    os.system("copy "+f"./{INPUT_CONTAINER_PATH}analytic_operation.json"+" "+f"{HDFS_PATH}analytic_operation.json")

    # reading json
    with open(f'{HDFS_PATH}analytic_operation.json') as conf:
        json_data = json.load(conf)
        log("Json reading completed")

    # copy data file
    file_path = json_data['file_container_path']
    os.system("copy "+f"./{INPUT_CONTAINER_PATH}cdm.zip"+" "+f"{HDFS_PATH}cdm.zip")
    log("Unzipping..")
    # unzip the zip into dbfs
    with ZipFile(f'{HDFS_PATH}cdm.zip', 'r') as zObject:
        zObject.extractall(path=f'{HDFS_PATH}')
    log("Unzip Completed")
    UNZIPED_DIR = "cdm"

    return {
        "config": json_data,
        "OUTPUT_CONTAINER_PATH": OUTPUT_CONTAINER_PATH, 
        "INPUT_CONTAINER_PATH": INPUT_CONTAINER_PATH, 
        "DBFS_PATH": HDFS_PATH,
        "UNZIPED_DIR": UNZIPED_DIR
    }


# def databricks_setup():
#     try:
#         # data-ingestions / clg0h8yx50064h10l2x2eksgw / share / zip
#         log("forming storage url")
        
#         STORAGE_URL = f'abfss://{global_container_name}@{global_storage_account_name}.dfs.core.windows.net'
#         log("forming input container url")
#         INPUT_CONTAINER_PATH = f"{STORAGE_URL}/data-ingestions/{global_ingestion_id}/share/daa/{global_analytics_id}/in/"
#         log("forming output container url")
#         OUTPUT_CONTAINER_PATH = f"{STORAGE_URL}/data-ingestions/{global_ingestion_id}/share/daa/{global_analytics_id}/out/"
        

#         # setup DBFS local directory to access the file
#         log("Creating HDFS fo this analytics ID")
#         DBFS_PATH = f"/FileStore/DAAv15/{global_analytics_id}/"
#         dbutils.fs.mkdirs(DBFS_PATH)
#         log("HDFS directory created")
        
#         # copy json file for operation
#         log("Copying json")
#         json_path = f"{INPUT_CONTAINER_PATH}analytic_operation.json"
#         copy = dbutils.fs.cp(json_path, DBFS_PATH, recurse=True)
#         log("json copied")
        
#         # copy json data to the varibale for runtime use
#         log("Reading json for configuration")
#         with open(f'/dbfs{DBFS_PATH}analytic_operation.json') as conf:
#             json_data = json.load(conf)
#             log("Json reading completed")
        
#         # copy data to BDFS
#         log("Copying data file to HDFS")
#         file_path = json_data['file_container_path']
#         # Sample: data-ingestions/clg0h8yx50064h10l2x2eksgw/share/zip/cdm.zip
#         dbutils.fs.cp(f"{STORAGE_URL}/{file_path}", DBFS_PATH, recurse=True)
#         log("Zip file copied")
        
#         log("Unzipping..")
#         # unzip the zip into dbfs
#         with ZipFile(f'/dbfs{DBFS_PATH}{file_path.split("/")[-1]}', 'r') as zObject:
#             zObject.extractall(path=f'/dbfs{DBFS_PATH}')
#         log("Unzip Completed")

#         UNZIPED_DIR = (file_path.split("/")[-1]).split(".")[-2]
#         log(f"UNZIPED_DIR: {UNZIPED_DIR}")
        
#         return {
#             "config": json_data,
#             "OUTPUT_CONTAINER_PATH": OUTPUT_CONTAINER_PATH, 
#             "INPUT_CONTAINER_PATH": INPUT_CONTAINER_PATH, 
#             "DBFS_PATH": DBFS_PATH,
#             "UNZIPED_DIR": UNZIPED_DIR
#         }
#     except Exception as ex:
#         log(str(ex))
#         # dispatch_response_graphql("FAILED",global_analytics_id)
#         raise Exception("Error(databricks_setup): ", str(ex))



def dispatch_response_graphql(status, analytics_id, path=''):
    path=path[1:]
    log(f"graphQL Path variable data: {path}")
    url = os.getenv('STATUS_UPDATE_URL', 'http://localhost:7072/graphql')
    log(f"GraphQL URL: {url}")
    try:
        if local:
            pass
        else:
            request_body = {
                "query": "mutation UpdateOperationLog($status: String, $updateOperationLogId: String) {updateOperationLog(status: $status, id: $updateOperationLogId) {name}}",
                "variables":{
                    "status": status,
                    "updateOperationLogId": analytics_id
                }
            }
            print(request_body)
            response = requests.post(url, data = json.dumps(request_body), headers = {'Content-Type': 'application/json'})
            if response.status_code == 200:
                print(response.status_code)
                log('GraphQL: Job Status Updated')
            else:
                print(response.status_code)
                log('GraphQL: Could not update Job Status')
    except Exception as ex:
        raise Exception("Failed to send graphQL response")
    
if local:
    global_data_config = databricks_setup_local()
else:
    # global_data_config = databricks_setup()
    pass

start_time = time.time()
spark = SparkSession.builder.appName('DAAv1.5').getOrCreate()
log(f"PySpark Session created in {str(time.time() - start_time)}")

def read_content():
    try:
        # file path example: data-ingestions/{clfqy13lt066501p8b3y6ra0v}/share/zip/cdm.zip
        log("Trying to read content from the file")
        hdfs_file_path = f"{global_data_config['DBFS_PATH']}{global_data_config['UNZIPED_DIR']}/{global_data_config['config']['file_name']}"
        log(f"HDFS FILE PATH: {hdfs_file_path}")
        dataframe = spark.read.option('header','true').csv(f"{hdfs_file_path}")
        log("able to read dataframe")
        return dataframe
    except Exception as ex:
        dispatch_response_graphql("FAILED",global_analytics_id)
        log(f"exception(read-content): {str(ex)}")
        raise Exception("Unable to read content")



# all the operation function
def process_filter(column_name,condition,input_data,df):
    log(f"column_name: {column_name} | condition: {condition} | input_data: {input_data}")
    def filter_lt(column,value,dff):
        return dff.filter(f"{column} < {value}")

    # filter: greater than
    def filter_gt(column,value,dff):
        print(f"{column} > {value}")
        return dff.filter(f"{column} > {value}")
    
    # filter: equal
    def filter_eq(column,value,dff):
        if value.isnumeric():
            return dff.filter(f"{column} == {value}")
        else:
            return dff.filter(f"{column} == '{value}'")
    
    # filter: not equal
    def filter_neq(column,value,dff):
        if value.isnumeric():
            return dff.filter(f"{column} != {value}")
        else:
            return dff.filter(f"{column} != '{value}'")
    
    # filter: like
    def filter_like(column,value,dff):
        return dff.filter(col(column).like(f"%{value}%"))
    
    # filter: less than equal to
    def filter_lte(column,value,dff):
        return dff.filter(f"{column} <= {value}")
    
    # filter: less than equal to
    def filter_gte(column,value,dff):
        return dff.filter(f"{column} >= {value}")
    
    
    if condition == 'lt':
        log("less than function triggred")
        return filter_lt(column_name,float(input_data),df)
    if condition == 'gt':
        log("greater than filter triggred")
        return filter_gt(column_name,float(input_data),df)
    if condition == 'eq':
        log("equal to filter triggred")
        return filter_eq(column_name,input_data,df)
    if condition == 'neq':
        log("not equal to filter triggred")
        return filter_neq(column_name,input_data,df)
    if condition == 'like':
        log("like filter triggred")
        return filter_like(column_name,input_data,df)
    if condition == 'lte':
        log("less than equal to filter triggred")
        return filter_lte(column_name,float(input_data),df)
    if condition == 'gte':
        log("greater than equal to filter triggred")
        return filter_gte(column_name,float(input_data),df)

def process_net(dataframe):
    try:
        total_debit_amount=0
        total_credit_amount=0

        dict_rows = [row.asDict(True) for row in dataframe.collect()]
        for item in dict_rows:
            if item['amountCreditDebitIndicator'] == 'D': 
                total_debit_amount += float(item['amount'])
            if item['amountCreditDebitIndicator'] == 'C': 
                total_credit_amount += float(item['amount'])*(-1.0)

        net = total_debit_amount-(total_credit_amount)
        print(f"total_debit_amount: {total_debit_amount} | total_credit_amount: {total_credit_amount} | net: {net}")
        
        data = [(f'{total_debit_amount}',f'{total_credit_amount}',f'{net}')]
        columns = ["total_debit_amount","total_credit_amount","net"]
        return spark.createDataFrame(data=data, schema = columns)
    except Exception as ex:
        log(f"{str(ex)}")

def process_count(dataframe,column_name,isUnique):
    try:
        # pyspark SQL Like implementation
        log("Processing count. Creating sql like dataframe for sql count operation")
        dataframe.createOrReplaceTempView("eBData")
        log("SQL like datafrae created for count")
        count=0
        if not isUnique:
            log("not unique")
            return spark.sql(f"select count({column_name}) as count from eBData")
        else:
            log("using distinct as its unique")
            return spark.sql(f"select count( DISTINCT {column_name}) as count from eBData")

    except Exception as ex:
        log(f"Exception(process_count): {str(ex)}")
    finally:
        log("Exiting count")

def export_processed_df(data,dbfs_path,container_output_path,operation_name,daa_id,order):
    try:
        if local:
            log("Showing step data as it is local")
            data.show()
        log("Operation completed. Trying to convert to PandasDF")
        pdf = data.toPandas()
        log("PandasDF created. trying to read same file for verification.")

        if local:
            log(f"Execuiting locally. File: {operation_name}_{daa_id}_{order}_data.csv")
            pdf.to_csv(f"{dbfs_path}{operation_name}_{daa_id}_{order}_data.csv",index=False)
        else:
            log("Execuing export processed dataframe on container")
            pdf.to_csv(f"/dbfs{dbfs_path}{operation_name}_{daa_id}_{order}_data.csv",index=False)
        log("Able to read HDFS exported processed csv using pandas function. Good to export to container")
        log("Exporting the output file to container")
        if local:
            log("Copying file locally")
            os.system("copy "+f"./{dbfs_path}{daa_id}_{order}_data.csv"+" "+f"{container_output_path}{operation_name}_{daa_id}_{order}_data.csv")
        else:
            log("Copying files to container")
            # dbutils.fs.cp(f"dbfs:{dbfs_path}{operation_name}_{daa_id}_{order}_data.csv",f"{container_output_path}{operation_name}_{daa_id}_{order}_data.csv",recurse=True)
            pass
    except Exception as ex:
        log(f"Exception(export_processed_df): {str(ex)}")

def export_processed_df_to_json(data,dbfs_path,container_output_path,operation_name,daa_id,order):
    try:
        result_df = data.limit(10)
        dict_rows = [row.asDict(True) for row in result_df.collect()]
        json_object_tmp = json.dumps({"data": dict_rows}, indent=4)
            
        log("temp json created for export")

        if not local:
            with open(f"/dbfs{dbfs_path}tmp_{operation_name}_{daa_id}_{order}.json", "w") as outfile:
                outfile.write(json_object_tmp)
        else:
                with open(f"./{dbfs_path}tmp_{operation_name}_{daa_id}_{order}.json", "w") as outfile:
                    outfile.write(json_object_tmp)
        
        log("Json is created at HDFS. preparing for Container export")

        if local:
            os.system("copy "+f"{dbfs_path}tmp_{operation_name}_{daa_id}_{order}.json"+" "+f'{container_output_path}tmp_{operation_name}_{daa_id}_{order}.json')
        else:
            # dbutils.fs.cp(
            #     f'dbfs:{dbfs_path}tmp_{daa_id}_{order}.json',
            #     f'{container_output_path}tmp_{operation_name}_{daa_id}_{order}.json',
            #     recurse=True)
            pass
    except Exception as ex:
        log(f"Error(export_processed_df_to_json): {str(ex)}")

def export_final_result(data,dbfs_path,daa_id,output_path, export_file_name):
    try:
        # data.show()
        log("Operation completed. Trying to convert to PandasDF")
        pdf = data.toPandas()
        log("PandasDF created. trying to read same file for verification.")

        if local:
            log("Executing locaaly for exporting")
            pdf.to_csv(f"{dbfs_path}{daa_id}_data.csv",index=False)
        else:
            log("Executing on Databricks cluster")
            pdf.to_csv(f"/dbfs{dbfs_path}{daa_id}_data.csv",index=False)
        
        log("Able to read HDFS exported processed csv using pandas function. Good to export to container")
        log("Exporting the output file to container")
        if local:
            log("result export Done locally!")
            os.system("copy "+f"./{dbfs_path}{daa_id}_data.csv"+" "+f"{output_path}{export_file_name}")
        else:
            log("Result export done on databricks")
            # dbutils.fs.cp(f"dbfs:{dbfs_path}{daa_id}_data.csv",f"{output_path}{export_file_name}",recurse=True)
            pass
    except Exception as ex:
        log(f"Error(export_final_result): {str(ex)}")


def update_process_status(daa_id,order,status):
    try:
        pass
    except Exception as ex:
        log(f"Exception(update_process_status): {str(ex)}")

def main():
    try:
        data = read_content()
        for operation in global_data_config['config']['operation']:
            # for each operation , process its filters
            operation_type = operation['operation_type']
            temp_data = data
            order = operation['order']

            temp_dfs = []
            
            for condition in operation['conditions']:
                if operation_type == 'Filter':
                    try:
                        temp_dfs.append(
                            process_filter(
                            condition['filter_column'],
                            condition['filter_condition'],
                            condition['input'],temp_data)
                        )
                    except Exception as ex:
                        print(str(ex))
            
            # concat dataframes from the temparary array 
            opx_dataframe = None
            for i in range(1,len(temp_dfs)):
                if i == 1:
                    opx_dataframe = temp_dfs[0].union(temp_dfs[i])
                else:
                    opx_dataframe = opx_dataframe.union(temp_dfs[i])
            
            export_processed_df(
                opx_dataframe,
                global_data_config['DBFS_PATH'],
                global_data_config['OUTPUT_CONTAINER_PATH'],
                operation_type,
                global_analytics_id,
                order
            )
            export_processed_df_to_json(
                opx_dataframe,
                global_data_config['DBFS_PATH'],
                global_data_config['OUTPUT_CONTAINER_PATH'],
                operation_type,
                global_analytics_id,
                order
            )
            update_process_status(
                global_analytics_id,
                order,
                'SUCCESS'
            )
        # prepare data for 1st 10 rows to export as json
        try:
            log("Preparing top 10 resulted rows for json export")
            
            result_df = data.limit(10)
            dict_rows = [row.asDict(True) for row in result_df.collect()]
            json_object_tmp = json.dumps({"data": dict_rows}, indent=4)
            
            log("temp json created for export")

            if not local:
                with open(f"/dbfs{global_data_config['DBFS_PATH']}tmp_{global_analytics_id}.json", "w") as outfile:
                    outfile.write(json_object_tmp)
            else:
                 with open(f"./{global_data_config['DBFS_PATH']}tmp_{global_analytics_id}.json", "w") as outfile:
                    outfile.write(json_object_tmp)
            
            log("Json is created at HDFS. preparing for Container export")

            if local:
                os.system("copy "+f"{global_data_config['DBFS_PATH']}tmp_{global_analytics_id}.json"+" "+f'{global_data_config["OUTPUT_CONTAINER_PATH"]}tmp_{opx}_{global_analytics_id}.json')
            else:
                # dbutils.fs.cp(
                #     f'dbfs:{global_data_config["DBFS_PATH"]}tmp_{global_analytics_id}.json',
                #     f'{global_data_config["OUTPUT_CONTAINER_PATH"]}tmp_{global_analytics_id}.json',
                #     recurse=True)
                pass
            log(f"JSON file exported to container. Export File name: tmp_{global_analytics_id}.json")
        except Exception as exx:
            log(f"Exception while generating json file: {str(exx)}")

            
        log("Exported to container")
        dispatch_response_graphql("COMPLETED",global_analytics_id,global_data_config['OUTPUT_CONTAINER_PATH'])
        
        
        log("Removing processed file from HDFS storage")
        if local:
            pass
        else:
            # dbutils.fs.rm(f"/FileStore/DAAv15/{global_analytics_id}/",True)
            pass
        log("HDFS storage cleared")
    except Exception as ex:
        dispatch_response_graphql("FAILED",global_analytics_id)
        log(f"exception(main): {str(ex)}")
        raise Exception("Exception in main")
    finally:
        log("Exporting log to container")
        json_object = json.dumps({"log": global_log}, indent=4)
        with open(f"./{global_data_config['DBFS_PATH']}log.json", "w") as outfile:
            outfile.write(json_object)
        #dbutils.fs.cp(f'dbfs:{global_data_config["DBFS_PATH"]}log.json',f'{global_data_config["OUTPUT_CONTAINER_PATH"]}log_{str(datetime.now()).replace(" ","_")}.json',recurse=True)
            
            

main()
log(f"Execution time: {str(time.time() - start_time)}")
print(global_log)