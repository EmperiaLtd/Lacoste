def get_schema_3():
    """
    Returns the schema for the csv file in-which to retrieve data from
    :return list:
    """
    return [
        'Product_ID',
        'color',
        'display_name_EN',
        'display_name_FR',
        'display_name_DE',
        'display_name_IT',
        'display_name_ES',
        'display_name_NL',
        'display_name_PT',
        'image_3d'
    ]

def schema_to_json_3(upcs_3):
    """
    Convert the file schema into a JSON response object to be received by the client

    :param upcs_3 list: contains objects
    """
    resp = {
        'name_EN': upcs_3[0].display_name_EN,
        'name_FR': upcs_3[0].display_name_FR,
        'name_DE': upcs_3[0].display_name_DE,
        'name_IT': upcs_3[0].display_name_IT,
        'name_ES': upcs_3[0].display_name_ES,
        'name_NL': upcs_3[0].display_name_NL,
        'name_PT': upcs_3[0].display_name_PT,
        'defaultColor': upcs_3[0].color,
        'image_3d':upcs_3[0].image_3d,
    }
    
    return {
        "status": "OK",
        "data": resp
    }


