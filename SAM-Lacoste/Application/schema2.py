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
        'defaultColor': upcs_2[0].color,
    }

    return {
        "status": "OK",
        "data": resp
    }


