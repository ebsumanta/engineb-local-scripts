# Databricks notebook source
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

# COMMAND ----------

# MAGIC %run /IntegrationEngine01/utilities/storageAccess

# COMMAND ----------

local=True
global_analytics_id=""
global_ingestion_id=""
global_storage_account_name = ""
global_container_name = ""

if local:
    global_analytics_id=""
    global_ingestion_id=""
    global_storage_account_name = ""
    global_container_name = ""
else: 
    # analytics id to get the json file
    dbutils.widgets.text("analytics_id","","")
    global_analytics_id = dbutils.widgets.get("analytics_id")

    # ingestion id for accessing the path
    dbutils.widgets.text("ingestion_id","","")
    global_ingestion_id = dbutils.widgets.get("ingestion_id")

    # for permission - Storage Account name
    dbutils.widgets.text("storage_account_name", "","")
    global_storage_account_name = dbutils.widgets.get("storage_account_name")

    # for permission - Container Name
    dbutils.widgets.text("containerName", "","")
    global_container_name = dbutils.widgets.get("containerName")


    

global_log = []
def log(msg):
    if local:
        print("***** ", msg, " *****\n")
    global_log.append(msg)

def databricks_setup_local():
    try:
        STORAGE_URL = "d2"
        INPUT_CONTAINER_PATH = f"{STORAGE_URL}/in/"
        OUTPUT_CONTAINER_PATH = f"{STORAGE_URL}/out/"
        DBFS_PATH = f"{STORAGE_URL}/dbfs/"
        
        with open(f'./{DBFS_PATH}analytic_operation.json') as conf:
            json_data = json.load(conf)
            log("Json reading completed")
        
        file_path = json_data['file_container_path']
        with ZipFile(f'./{DBFS_PATH}{file_path.split("/")[-1]}', 'r') as zObject:
            zObject.extractall(path=f'{DBFS_PATH}')
        log("Unzip Completed")
        
        UNZIPED_DIR = (file_path.split("/")[-1]).split(".")[-2]
        log(f"UNZIPED_DIR: {UNZIPED_DIR}")
        
        return {
            "config": json_data,
            "OUTPUT_CONTAINER_PATH": OUTPUT_CONTAINER_PATH, 
            "INPUT_CONTAINER_PATH": INPUT_CONTAINER_PATH, 
            "DBFS_PATH": DBFS_PATH,
            "UNZIPED_DIR": UNZIPED_DIR
        }
        
        
    except Exception as ex:
        print(str(ex))

def databricks_setup():
    try:
        # data-ingestions / clg0h8yx50064h10l2x2eksgw / share / zip
        log("forming storage url")
        
        STORAGE_URL = f'abfss://{global_container_name}@{global_storage_account_name}.dfs.core.windows.net'
        log("forming input container url")
        INPUT_CONTAINER_PATH = f"{STORAGE_URL}/data-ingestions/{global_ingestion_id}/share/daa/{global_analytics_id}/in/"
        log("forming output container url")
        OUTPUT_CONTAINER_PATH = f"{STORAGE_URL}/data-ingestions/{global_ingestion_id}/share/daa/{global_analytics_id}/out/"
        

        # setup DBFS local directory to access the file
        log("Creating HDFS fo this analytics ID")
        DBFS_PATH = f"/FileStore/DAAv15/{global_analytics_id}/"
        dbutils.fs.mkdirs(DBFS_PATH)
        log("HDFS directory created")
        
        # copy json file for operation
        log("Copying json")
        json_path = f"{INPUT_CONTAINER_PATH}analytic_operation.json"
        copy = dbutils.fs.cp(json_path, DBFS_PATH, recurse=True)
        log("json copied")
        
        # copy json data to the varibale for runtime use
        log("Reading json for configuration")
        with open(f'/dbfs{DBFS_PATH}analytic_operation.json') as conf:
            json_data = json.load(conf)
            log("Json reading completed")
        
        # copy data to BDFS
        log("Copying data file to HDFS")
        file_path = json_data['file_container_path']
        # Sample: data-ingestions/clg0h8yx50064h10l2x2eksgw/share/zip/cdm.zip
        dbutils.fs.cp(f"{STORAGE_URL}/{file_path}", DBFS_PATH, recurse=True)
        log("Zip file copied")
        
        log("Unzipping..")
        # unzip the zip into dbfs
        with ZipFile(f'/dbfs{DBFS_PATH}{file_path.split("/")[-1]}', 'r') as zObject:
            zObject.extractall(path=f'/dbfs{DBFS_PATH}')
        log("Unzip Completed")

        UNZIPED_DIR = (file_path.split("/")[-1]).split(".")[-2]
        log(f"UNZIPED_DIR: {UNZIPED_DIR}")
        
        return {
            "config": json_data,
            "OUTPUT_CONTAINER_PATH": OUTPUT_CONTAINER_PATH, 
            "INPUT_CONTAINER_PATH": INPUT_CONTAINER_PATH, 
            "DBFS_PATH": DBFS_PATH,
            "UNZIPED_DIR": UNZIPED_DIR
        }
    except Exception as ex:
        log(str(ex))
        # dispatch_response_graphql("FAILED",global_analytics_id)
        raise Exception("Error(databricks_setup): ", str(ex))
#databricks_setup()

# COMMAND ----------

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

# COMMAND ----------

# initiate databricks setup and get the global config
# dispatch_response_graphql("INITIATED",global_analytics_id)
if local:
    global_data_config = databricks_setup_local()
else:
    global_data_config = databricks_setup()
    
start_time = time.time()
spark = SparkSession.builder.appName('DAAv1.5').getOrCreate()
log(f"PySpark Session created in {str(time.time() - start_time)}")

# COMMAND ----------

def read_content(past_data_path=''):
    try:
        # past used data might not be in hdfs, we need to copy the data
        if not local and past_data_path != '':
            past_file_path = f"{global_data_config['INPUT_CONTAINER_PATH']}{past_data_path}"
            dbutils.fs.cp(past_file_path, global_data_config['DBFS_PATH'], recurse=True)
            log("past file  copied")
        # file path example: data-ingestions/{clfqy13lt066501p8b3y6ra0v}/share/zip/cdm.zip
        log("Trying to read content from the file")
        
        if past_data_path != "":
            if local:
                hdfs_file_path = f"./{past_data_path}"
            else:
                hdfs_file_path = f"{past_data_path}"
        else:
            if local:
                hdfs_file_path = f"./{global_data_config['DBFS_PATH']}{global_data_config['UNZIPED_DIR']}/{global_data_config['config']['file_name']}"
            else:
                hdfs_file_path = f"{global_data_config['DBFS_PATH']}{global_data_config['UNZIPED_DIR']}/{global_data_config['config']['file_name']}"
        
        
        
        log(f"HDFS FILE PATH: {hdfs_file_path}")
        dataframe = spark.read.option('header','true').csv(f"{hdfs_file_path}")
        log("able to read dataframe")
        return dataframe
    except Exception as ex:
        dispatch_response_graphql("FAILED",global_analytics_id)
        log(f"exception(read-content): {str(ex)}")
        raise Exception("Unable to read content")


# COMMAND ----------

# filter: less than
def process_filter(column_name,condition,input_data,df):
    log(f"column_name: {column_name} | condition: {condition} | input_data: {input_data}")
    def filter_lt(column,value,dff):
        return dff.filter(f"{column} < {value}")

    # filter: greater than
    def filter_gt(column,value,dff):
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
        log(f"total_debit_amount: {total_debit_amount} | total_credit_amount: {total_credit_amount} | net: {net}")
        
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
def process_sum(dataframe, column):
    try:
        dataframe.createOrReplaceTempView("tempTable")
        
        sum_result = spark.sql(f"select sum({str(column)}) as sum from tempTable")
        print("sum_result type: \n",type(sum_result))
        sum_result.show()
        print("process sum: ", sum_result)
        return sum_result
    except Exception as ex:
        print("process_sum Exception :",str(ex))
        log(f"Process Sum failure: {str(ex)}")
    finally:
        log(f"Exiting Sum")
def convert_dataframe_to_json_export(dataframe, file_name, dbfs, output):
    try:
        result_df = dataframe.limit(10)
        dict_rows = [row.asDict(True) for row in result_df.collect()]
        json_object_tmp = json.dumps({"data": dict_rows}, indent=4)
        if not local:
            with open(f"/dbfs{dbfs}{file_name}", "w") as outfile:
                outfile.write(json_object_tmp)
        else:
            with open(f"./{dbfs}{file_name}", "w") as outfile:
                outfile.write(json_object_tmp)
        if not local:
            dbutils.fs.cp(
                f'dbfs:{dbfs}{file_name}',
                f'{output}{file_name}',
                recurse=True)
    except Exception as ex:
        log(f"Error(export_processed_df_to_json): {str(ex)}")
def convert_dataframe_to_csv_export(dataframe, file_name, dbfs, output):
    try:
        # data.show()
        pdf = dataframe.toPandas()
        if local:
            pdf.to_csv(f"./{dbfs}{file_name}",index=False)
        else:
            pdf.to_csv(f"/dbfs{dbfs}{file_name}",index=False)
        
        if not local:
            dbutils.fs.cp(
                f"dbfs:{dbfs}{file_name}",
                f"{output}{file_name}",
                recurse=True)
            
    except Exception as ex:
        log(f"Error(export_final_result): {str(ex)}")
def update_graphql_order_status(order,daa_id,status):
    try:
        url = os.getenv('STATUS_UPDATE_URL', 'http://localhost:7072/graphql')
        if local:
            pass
        else:
            request_body = {
                "query": "mutation UpdateOperationLog($status: String, $updateOperationLogId: String) {updateOperationLog(status: $status, id: $updateOperationLogId) {name}}",
                "variables":{
                    "status": status,
                    "updateOperationLogId": daa_id,
                    "order": order
                }
            }
            response = requests.post(url, data = json.dumps(request_body), headers = {'Content-Type': 'application/json'})
            if response.status_code == 200:
                print(response.status_code)
                log('GraphQL: Job Status Updated')
            else:
                print(response.status_code)
                log('GraphQL: Could not update Job Status')
    except Exception as ex:
        log(f"Exception(update_process_status): {str(ex)}")

def main():
    try:
        # for retry, data can not be available on run time, 
        # we have to read from container, then load the data
        if global_data_config['config'].get('source') is None:
            if global_data_config['config'].get('source') != "":
                data = read_content(global_data_config['config'].get('source'))
            else:
                data = read_content()
        else:
            data = read_content()
        
        root_holder = dict()
        
        for function in global_data_config['config']['operation']:
            data_to_use = None
            temp_data_holder = []
            result_data_holder = dict()
            log(f"Executing order: {function['order']}")
            print("temp_data_holder initiation: ", temp_data_holder)
            print("result_data_holder: ", result_data_holder)
            if function["source"] == "":
                data_to_use = data
            else:
                log(f"taking source mentioned data (child source)")
                data_to_use = root_holder.get(str(function["source"]))[0]
                
            for operation in function['conditions']:
                #print("operation: ",operation)
                if function['operation_type'] == "Filter":
                    filter_output = process_filter(
                        operation['filter_column'],
                        operation['filter_condition'],
                        operation['input'],
                        data_to_use
                    )
                    data_to_use = filter_output
                if function['operation_type'] == "Count":
                    count_output = process_count(
                        data_to_use,
                        operation['filter_column'],
                        operation['is_unique']
                    )
                    temp_data_holder.append(count_output)
                if function['operation_type'] == "Net":
                    net_output = process_net(
                        data_to_use
                    )
                    temp_data_holder.append(net_output)
                if function['operation_type'] == "Sum":
                    print("Calling sum.....")
                    sum_output = process_sum(
                        data_to_use,
                        operation['filter_column']
                    )
                    #print("sum_output: ", sum_output,"\n\n")
                    temp_data_holder.append(sum_output)
                print("temp_data_holder under for loop: ",temp_data_holder, len(temp_data_holder))
            
            # it should only work for Filter type operation
            if function['operation_type'] == "Filter":
                temp_data_holder.append(data_to_use)
           
            opx_dataframe = None
            print("opx_dataframe before merger: ",opx_dataframe)
            print("temp_data_holder: ",temp_data_holder, len(temp_data_holder))
            if len(temp_data_holder) > 1:
                for i in range(1,len(temp_data_holder)):
                    if i == 1:
                        opx_dataframe = temp_data_holder[0].union(temp_data_holder[i])
                    else:
                        opx_dataframe = opx_dataframe.union(temp_data_holder[i])
            else:
                opx_dataframe = temp_data_holder
            print("opx_dataframe: ", opx_dataframe)
            result_data_holder[str(function['order'])] = opx_dataframe
            root_holder[str(function['order'])] = opx_dataframe
            # convert the dataframe to json
            print("saving json")
            convert_dataframe_to_json_export(
                opx_dataframe[0],
                f"tmp_{function['operation_type']}_{function['order']}.json",
                global_data_config['DBFS_PATH'],
                global_data_config["OUTPUT_CONTAINER_PATH"]
            )
            # export the result dataframe to container
            print("saving csv")
            convert_dataframe_to_csv_export(
                opx_dataframe[0], 
                f"tmp_{function['operation_type']}_{function['order']}.csv", 
                global_data_config['DBFS_PATH'],
                global_data_config["OUTPUT_CONTAINER_PATH"])
            # update the graphql about the order execution status
            print("updaing db")
            update_graphql_order_status(
                function['order'],
                global_analytics_id,
                'SUCCESS')
            print("result_data_holder (end): ",result_data_holder,"\n\n\n\n\n\n\n\n")
            
     
    except Exception as ex:
        dispatch_response_graphql("FAILED",global_analytics_id)
        log(f"exception(main): {str(ex)}")
        raise Exception("Exception in main")
    finally:
        log("Exporting log to container")
        json_object = json.dumps({"log": global_log}, indent=4)
        if local:
            with open(f"./{global_data_config['DBFS_PATH']}log.json", "w") as outfile:
                outfile.write(json_object)
        else:
            with open(f"/dbfs{global_data_config['DBFS_PATH']}log.json", "w") as outfile:
                outfile.write(json_object)
            dbutils.fs.cp(f'dbfs:{global_data_config["DBFS_PATH"]}log.json',f'{global_data_config["OUTPUT_CONTAINER_PATH"]}log_{str(datetime.now()).replace(" ","_")}.json',recurse=True)
            
            
main()
log(f"Execution time: {str(time.time() - start_time)}")
#print(global_log)
