import json  
import pandas as pd
from schema import get_schema, schema_to_json, market_to_json
from schema1 import get_schema_1, schema_to_json_1, market_to_json_1
from schema2 import get_schema_2, schema_to_json_2, market_to_json_2
from schema3 import get_schema_3, schema_to_json_3, market_to_json_3
from db import connect_to_db                                                                                                        
import boto3
import csv
import io
import sys
import numpy as np

#/////////////////////////this is test one for lacoste /////////////////////////
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

    insert_data_into_db()
    
def insert_data_into_db():
     
    s3_bucket_name='sftpgw-i-06e8a0b5d0a44b1fb'
    df = []
    df_1=[]
    df_2=[]
    df_3=[]
    converted_df = pd.DataFrame(columns=get_schema())
    converted_df_1 = pd.DataFrame(columns=get_schema_1())
    converted_df_2 = pd.DataFrame(columns=get_schema_2())
    converted_df_3 = pd.DataFrame(columns=get_schema_3())
    
    my_bucket=s3.Bucket(s3_bucket_name)
    bucket_list = []
    for file in my_bucket.objects.filter(Prefix = 'users/Lacoste/'):
        file_name=file.key
        if file_name.find(".csv")!=-1:
            bucket_list.append(file.key)
        # print(bucket_list, "all the keys")    
    if sys.version_info[0] < 3:
        from io import StringIO 
    
    for file in bucket_list:
        obj = s3.Object(s3_bucket_name,file)
        data=obj.get()['Body'].read()
        
        if file == "users/Lacoste/virtual store catalog CA (2).csv":
            market="CA"
            df.append(pd.read_csv(io.BytesIO(data), header=0, delimiter=",", low_memory=False,encoding = "ISO-8859-1"))
            for file in df:
                converted_df1 = pd.DataFrame(data = file)
                converted_df = pd.DataFrame(np.concatenate([converted_df.values, converted_df1.values]), columns=converted_df.columns)
            upcs = [] #initialising empty list
            pid2=converted_df['Product_ID'].iloc[0]
            for index, row in converted_df.iterrows():
                pid = row.Product_ID
                if pid != pid2:
                        upcs.clear()
                        pid2=pid
                if row.Product_ID == pid:
                    upcs.append(row)
                    file_data = json.dumps(schema_to_json(upcs))
                    db.set("Lacoste" + "_" + market +"_"+ row.Product_ID , file_data)
                    print("inserted",market)      
            for index, row in converted_df.iterrows():
                upcs.append(row)
                file_d = json.dumps(market_to_json(upcs))
                db.set("Lacoste"+ "_" + market, file_d)

            
        elif file == "users/Lacoste/virtual store catalog US (1).csv":
            market="US"
            df_1.append(pd.read_csv(io.BytesIO(data), header=0, delimiter=",", low_memory=False,encoding = "ISO-8859-1"))
            for file in df_1:
                converted_df11 = pd.DataFrame(data = file)
                converted_df_1 = pd.DataFrame(np.concatenate([converted_df_1.values, converted_df11.values]), columns=converted_df_1.columns)
            upcs_1 = [] #initialising empty list 
            pid2=converted_df_1['Product_ID'].iloc[0]
            for index, row in converted_df_1.iterrows():
                pid = row.Product_ID
                if pid != pid2:
                        upcs_1.clear()
                        pid2=pid
                if row.Product_ID == pid:
                    upcs_1.append(row)
                    file_data_1 = json.dumps(schema_to_json_1(upcs_1))
                    db.set("Lacoste" + "_" + market +"_"+ row.Product_ID, file_data_1)
                    print("inserted",market)      
            for index, row in converted_df_1.iterrows():
                    upcs_1.append(row)
                    file_d_1 = json.dumps(market_to_json_1(upcs_1))
                    db.set("Lacoste" + "_" + market, file_d_1)
            
        elif file == "users/Lacoste/virtual store catalog MX (2).csv":
            market="MX"
            df_2.append(pd.read_csv(io.BytesIO(data), header=0, delimiter=",", low_memory=False,encoding = "ISO-8859-1"))
            for file in df_2:
                converted_df22 = pd.DataFrame(data = file)
                converted_df_2 = pd.DataFrame(np.concatenate([converted_df_2.values, converted_df22.values]), columns=converted_df_2.columns)
            upcs_2 = [] #initialising empty list 
            pid2=converted_df_2['Product_ID'].iloc[0]
            for index, row in converted_df_2.iterrows():
                pid = row.Product_ID
                if pid != pid2:
                        upcs_2.clear()
                        pid2=pid
                if row.Product_ID == pid:
                    upcs_2.append(row)
                    file_data_2 = json.dumps(schema_to_json_2(upcs_2))
                    db.set("Lacoste" + "_" + market +"_"+ row.Product_ID, file_data_2)      
                    print("inserted",market)
            for index, row in converted_df_2.iterrows():
                    upcs_2.append(row)
                    file_d_2 = json.dumps(market_to_json_2(upcs_2))
                    db.set("Lacoste"+ "_" + market, file_d_2)
            
        elif file == "users/Lacoste/virtual store catalog EU (2) (1).csv":
            market="EU"
            df_3.append(pd.read_csv(io.BytesIO(data), header=0, delimiter=",", low_memory=False,encoding = "ISO-8859-1"))
            for file in df_3:
                converted_df33 = pd.DataFrame(data = file)
                converted_df_3 = pd.DataFrame(np.concatenate([converted_df_3.values, converted_df33.values]), columns=converted_df_3.columns)
            upcs_3 = [] #initialising empty list 
            pid2=converted_df_3['Product_ID'].iloc[0]
            for index, row in converted_df_3.iterrows():
                pid = row.Product_ID
                if pid != pid2:
                        upcs_3.clear()
                        pid2=pid
                if row.Product_ID == pid:
                    upcs_3.append(row)
                    file_data_3 = json.dumps(schema_to_json_3(upcs_3))
                    db.set("Lacoste" + "_" + market +"_"+ row.Product_ID, file_data_3)      
                    print("inserted",market)
            for index, row in converted_df_3.iterrows():
                    upcs_3.append(row)
                    file_d_3 = json.dumps(market_to_json_3(upcs_3))
                    db.set("Lacoste"+ "_" + market, file_d_3)          
    # # return {
    # #     "status": "BAD",
    # #     "message": "Could not find specified PID",
    # # }


