def get_schema_1():
    """
    Returns the schema for the csv file in-which to retrieve data from
    :return list:
    """
    return [
        'Product_ID',
        'color',
        'display_name_EN'
    ]

def schema_to_json_1(upcs_1):
    """
    Convert the file schema into a JSON response object to be received by the client

    :param upcs list: contains objects
    """
    resp = {
        'name': upcs_1[0].display_name_EN,
        'defaultColor': {},
    }
    for row in upcs_1: 
        resp["defaultColor"][row.color] = row.color
 
    return {
        "status": "OK",
        "data": resp
    }

def market_to_json_1(upcs_1):
    """
    Convert the file schema into a JSON response object to be received by the client

    :param upcs list: contains objects
    """
    resp = []
    for row in upcs_1: 
        objHolder =  {
        'pid':row.Product_ID,   
        'title': row.display_name_EN,
        # 'products':{}
        }
        resp.append(objHolder)
    return {
        "status": "OK",
        "data": resp
    }     


