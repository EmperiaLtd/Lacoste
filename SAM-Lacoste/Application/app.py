import json  
import pandas as pd
from schema import get_schema, schema_to_json
from schema1 import get_schema_1, schema_to_json_1
from schema2 import get_schema_2, schema_to_json_2
from schema3 import get_schema_3, schema_to_json_3
from db import connect_to_db                                                                                                        
import boto3
import csv
import io
import sys
import numpy as np

headers = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Origin": '*',
    "Access-Control-Allow-Methods": 'POST'
}

"""connection to database"""
db = connect_to_db()
s3 = boto3.resource('s3')

def lambda_handler(event, context):
    if event.get('body') == None:
        response = {
            "statusCode": 404,
            "headers": headers,
            "body": json.dumps({"message": "no body"}),
        }
        return response
    
    body = json.loads(event.get('body'))
    pid = body.get('pid')
    market=body.get('market')

    if 'pid' == None:
        return {
            "statusCode": 404,
            "headers": headers,
            "body": json.dumps({"message": "no pid"})
        }

    db_obj = load_from_db(pid)
    if db_obj is None:
        file_data = json.dumps(read_csv_from_s3(pid))
        db.set(pid + "_" + market, file_data) #set the data into database
    else:
        return {
            'statusCode': 200,
            "headers": headers,
            "body": json.dumps(db_obj)
        }
    return {
        'statusCode': 200,
        "headers": headers,
        "body": file_data
    }


def load_from_db(pid):
  db_obj = db.get(pid)
  if db_obj is None: return None
  json_data = json.loads(db_obj.decode('utf-8'))
  return json_data

#////////////////////////////////////////////////////////
"""main function for read csv file and return the json """
#////////////////////////////////////////////////////////


def read_csv_from_s3(pid):
    
    s3_bucket_name='sftpgw-i-06e8a0b5d0a44b1fb'
    
    
    my_bucket=s3.Bucket(s3_bucket_name)
    bucket_list = []
    for file in my_bucket.objects.filter(Prefix = 'users/Lacoste/'):
        file_name=file.key
        if file_name.find(".csv")!=-1:
            bucket_list.append(file.key)
    if sys.version_info[0] < 3:
        from io import StringIO  # Python 3.x
    
    df = []
    df_1=[]
    df_2=[]
    df_3=[]
    
    # Initializing empty list of dataframes
    converted_df = pd.DataFrame(columns=get_schema())
    converted_df_1 = pd.DataFrame(columns=get_schema_1())
    converted_df_2 = pd.DataFrame(columns=get_schema_2())
    converted_df_3 = pd.DataFrame(columns=get_schema_3()) 
  
    
    for file in bucket_list:
        obj = s3.Object(s3_bucket_name,file)
        data=obj.get()['Body'].read()
        
        #/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        """ take the firts csv and turn it into json"""
        #////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        
        if file == "users/Lacoste/virtual store catalog CA (2).csv":
            df.append(pd.read_csv(io.BytesIO(data), header=0, delimiter=",", low_memory=False,encoding = "ISO-8859-1"))
            for file in df:
                converted_df1 = pd.DataFrame(data = file)
                converted_df = pd.DataFrame(np.concatenate([converted_df.values, converted_df1.values]), columns=converted_df.columns)
            upcs = [] #initialising empty list 
            for index, row in converted_df.iterrows():
                if row.Product_ID == pid: 
                    upcs.append(row) #append the row into upcs 
            if len(upcs) > 0:
               return schema_to_json(upcs)
            
            #///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            """ take the second csv and turn it into json"""
            #///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
           
        elif file == "users/Lacoste/virtual store catalog US (1).csv":
            df_1.append(pd.read_csv(io.BytesIO(data), header=0, delimiter=",", low_memory=False,encoding = "ISO-8859-1"))
            for file in df_1:
                converted_df_11 = pd.DataFrame(data = file)
                converted_df_1 = pd.DataFrame(np.concatenate([converted_df_1.values, converted_df_11.values]), columns=converted_df_1.columns)
            upcs_1=[]
            for index, row in converted_df_1.iterrows():
                if row.Product_ID == pid:
                    upcs_1.append(row)      
            if len(upcs_1) > 0:
               return schema_to_json_1(upcs_1)
           
            #///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            """ take the third csv and turn it into json"""
            #//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        
        elif file == "users/Lacoste/virtual store catalog MX (2).csv":
            df_2.append(pd.read_csv(io.BytesIO(data), header=0, delimiter=",", low_memory=False,encoding = "ISO-8859-1"))
            for file in df_2:
                converted_df_22 = pd.DataFrame(data = file)
                converted_df_2 = pd.DataFrame(np.concatenate([converted_df_2.values, converted_df_22.values]), columns=converted_df_2.columns)
            upcs_2=[]
            for index, row in converted_df_2.iterrows():
                if row.Product_ID == pid:
                    upcs_2.append(row)      
            if len(upcs_2) > 0:
               return schema_to_json_2(upcs_2)
           
            #/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            """ take the second csv and turn it into json"""
            #/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        
        elif file == "users/Lacoste/virtual store catalog EU (2) (1).csv":
            df_3.append(pd.read_csv(io.BytesIO(data), header=0, delimiter=",", low_memory=False,encoding = "ISO-8859-1"))
            for file in df_3:
                converted_df_33 = pd.DataFrame(data = file)
                converted_df_3 = pd.DataFrame(np.concatenate([converted_df_3.values, converted_df_33.values]), columns=converted_df_3.columns)
            upcs_3=[]
            for index, row in converted_df_3.iterrows():
                if row.Product_ID == pid:
                    upcs_3.append(row)      
            if len(upcs_3) > 0:
               return schema_to_json_3(upcs_3)              

    return {
        "status": "BAD",
        "message": "Could not find specified PID",
    }
