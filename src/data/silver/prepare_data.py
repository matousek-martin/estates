from functools import reduce
from typing import Dict, Any

import pandas as pd


def json_to_pandas(data: Dict) -> pd.DataFrame:
    """Loads a parsed estate.json

    Args:
        data: A single estate to be parsed

    Returns:
        pd.DataFrame: A semi-processed dataframe containing relevant estate information
    """
    df = pd.DataFrame.from_dict(data, orient='index')
    new_col_names = ['estate_id'] + df.columns.to_list()
    df = df.reset_index()
    df.columns = new_col_names
    return df


def parse(estate: Dict) -> Dict:
    """Parse Sreality API response into a flat dictionary

    Args:
        estate (Dict): a dictionary containing estate information in the Sreality API format

    Returns:
        Dict: Flattened dictionary with data for the price prediction model and data for the website
    """
    estate_flattened = {
        # Web
        # Estate
        "estate_title": extract(estate, "name.value"),
        "estate_description_short": extract(estate, "meta_description"),
        "estate_description_long": extract(estate, "text.value"),
        "estate_category_main_cb": extract(estate, "seo.category_main_cb"),
        "estate_disposition": extract(estate, "seo.category_sub_cb"),
        "estate_rental_or_sell": extract(estate, "seo.category_type_cb"),
        "estate_locality_district": extract(estate, "locality_district_id"),
        "estate_longitude": extract(estate, "map.lon"),
        "estate_latitude": extract(estate, "map.lat"),
        'estate_map_zoom': extract(estate, 'map.zoom'),
        "estate_images": [
            image["_links"]["view"]["href"]
            for image in extract(estate, "_embedded.images", default=[])
        ],
        # Seller
        "seller_ico": extract(estate, "_embedded.seller._embedded.premise.ico"),
        "seller_email": extract(estate, "_embedded.seller._embedded.premise.email"),
        "seller_numbers": [
            tel["code"] + tel["number"]
            for tel in extract(estate, "_embedded.seller._embedded.premise.phones", default=[])
        ],
        "seller_web": extract(estate, "_embedded.seller._embedded.premise.www"),
        "seller_address": extract(estate, "_embedded.seller._embedded.premise.address"),
        "seller_name": extract(estate, "_embedded.seller._embedded.premise.name")
    }

    # Model
    # Points of interest
    for poi in extract(estate, "poi", default=[]):
        estate_flattened[poi["name"]] = poi["distance"]
    # Items
    for item in extract(estate, "items", default=[]):
        if item['type'] == 'set':
            for val in item['value']:
                estate_flattened[val['name']] = val['value']
        else:
            estate_flattened[item["name"]] = item["value"]

    return estate_flattened


def extract(dictionary: Dict, keys: str, default: Any = None) -> Any:
    """Wrap around dictionary item, return None or empty list in case it does not exist
    # https://stackoverflow.com/questions/25833613/safe-method-to-get-value-of-nested-dictionary

    Args:
        dictionary: dictionary to parse
        keys: path to item, e.g. dict['level1']['level2'] => level1.level2
        default: what to return in case of None

    Returns:
        Any: Extracted

    """
    return reduce(
        lambda d, key: d.get(key, default)
        if isinstance(d, dict)
        else default, keys.split("."), dictionary
    )
