def get_schema_2():
    """
    Returns the schema for the csv file in-which to retrieve data from
    :return list:
    """
    return [
        'Product_ID',
        'color',
        'display_name_EN',
        'display_name_ES'
    ]

def schema_to_json_2(upcs_2):
    """
    Convert the file schema into a JSON response object to be received by the client

    :param upcs_2 list: contains objects
    """
    resp = {
        'name_EN': upcs_2[0].display_name_EN,
        'name_ES': upcs_2[0].display_name_ES,
        'defaultColor': {},
    }
    for row in upcs_2: 
        resp["defaultColor"][row.color] = row.color

    return {
        "status": "OK",
        "data": resp
    }

def market_to_json_2(upcs_2):
    """
    Convert the file schema into a JSON response object to be received by the client

    :param upcs list: contains objects
    """
    resp = []
    for row in upcs_2: 
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
