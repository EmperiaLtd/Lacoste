def get_schema():
    """
    Returns the schema for the csv file in-which to retrieve data from
    :return list:
    """
    return [
        'Product_ID',
        'color',
        'display_name_EN',
        'display_name_FR'
    ]


def schema_to_json(upcs):
    """
    Convert the file schema into a JSON response object to be received by the client

    :param upcs list: contains objects
    """
    resp = {
        'name_EN': upcs[0].display_name_EN,
        'name_FR': upcs[0].display_name_FR,
        'defaultColor': upcs[0].color,
    }
    
    return {
        "status": "OK",
        "data": resp
    }


