import json
import pandas as pd
from schema import get_schema, schema_to_json
from db import connect_to_db
headers = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Origin": '*',
    "Access-Control-Allow-Methods": 'POST'
}


# #Preloader function
# _file = pd.read_excel(f'./feed.xlsx', sheet_name="Worksheet")
# df = pd.DataFrame(_file, columns=get_schema())
# for index, row in df.iterrows():
#  db.set(row.PRODUCT_ID, json.dumps(schema_to_json(row)))
db = connect_to_db()

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

    if 'pid' == None:
        return {
            "statusCode": 404,
            "headers": headers,
            "body": json.dumps({"message": "no pid"})
        }

    db_obj = load_from_db(pid)
    if db_obj is None:
        file_data = json.dumps(load_from_xlsx_file(pid))
        db.set(pid, file_data)
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


def load_from_xlsx_file(pid):
    _file = pd.read_excel(f'./feed1.xlsx')
    df = pd.DataFrame(_file, columns=get_schema())
    upcs = []

    for index, row in df.iterrows():
        if row.Product_id == pid:
            upcs.append(row)

    if len(upcs) > 0:
        return schema_to_json(upcs)

    return {
        "status": "BAD",
        "message": "Could not find specified PID",
    }
